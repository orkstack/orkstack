// The CodeMirror half of the YAML editor: every extension the editor runs,
// built from the pure modules beside it. Nothing here decides anything about a
// document — lint.ts, server.ts, complete.ts and highlight.ts do, and each is
// unit-tested — this file only connects them to a view.
//
// The theme names tokens and nothing else: CodeMirror's base theme draws its
// own colours (a light-blue current line, a grey gutter, a #17c completion
// highlight, red SVG lint marks), and every one of them is overridden here
// with ui/src/tokens.css, in both themes, so the editor is the drawing
// office's like every other field. The base theme writes its colours under
// `&light`/`&dark`, which only a base theme may use; `&.cm-editor` gives an
// override the same weight, and a theme is mounted after the base theme, so
// the token wins in either mode.
import { autocompletion, completionKeymap, type CompletionContext, type CompletionResult } from '@codemirror/autocomplete';
import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
import { yaml } from '@codemirror/lang-yaml';
import { indentUnit, syntaxTree } from '@codemirror/language';
import { lintGutter, lintKeymap, setDiagnostics, type Diagnostic as CmDiagnostic } from '@codemirror/lint';
import { EditorState, RangeSetBuilder, StateEffect, StateField, type Extension, type Text } from '@codemirror/state';
import {
	Decoration,
	EditorView,
	ViewPlugin,
	highlightActiveLine,
	highlightActiveLineGutter,
	highlightSpecialChars,
	closeHoverTooltips,
	hasHoverTooltips,
	hoverTooltip,
	keymap,
	lineNumbers,
	placeholder as placeholderText,
	showTooltip,
	type DecorationSet,
	type KeyBinding,
	type Tooltip,
	type ViewUpdate
} from '@codemirror/view';
import { contextAt, hoverDoc, keyOptions, valueOptions } from './complete';
import { classify, type SyntaxRole } from './highlight';
import { SCHEMAS } from './schemas';
import type { Diagnostic, DocumentKind } from './types';

// ---------------------------------------------------------------- colour

const MARKS: Record<SyntaxRole, Decoration> = {
	key: Decoration.mark({ class: 'syntax-key' }),
	string: Decoration.mark({ class: 'syntax-string' }),
	number: Decoration.mark({ class: 'syntax-number' }),
	literal: Decoration.mark({ class: 'syntax-literal' }),
	template: Decoration.mark({ class: 'syntax-template' }),
	meta: Decoration.mark({ class: 'syntax-meta' }),
	comment: Decoration.mark({ class: 'syntax-comment' }),
	punctuation: Decoration.mark({ class: 'syntax-punctuation' })
};

function colour(view: EditorView): DecorationSet {
	const builder = new RangeSetBuilder<Decoration>();
	const text = view.state.doc.toString();
	const tree = syntaxTree(view.state);
	for (const { from, to } of view.visibleRanges) {
		for (const span of classify(tree, text, from, to)) builder.add(span.from, span.to, MARKS[span.role]);
	}
	return builder.finish();
}

/** The syntax pencils over the visible lines, from the same classifier the read-only views use. */
const syntaxColours = ViewPlugin.fromClass(
	class {
		decorations: DecorationSet;
		constructor(view: EditorView) {
			this.decorations = colour(view);
		}
		update(update: ViewUpdate) {
			if (update.docChanged || update.viewportChanged || syntaxTree(update.startState) !== syntaxTree(update.state)) {
				this.decorations = colour(update.view);
			}
		}
	},
	{ decorations: (plugin) => plugin.decorations }
);

// ---------------------------------------------------------------- findings

/** A finding's sentence as the popup draws it: its severity word, its path at weight, a `quoted` key in the mono. */
function renderSentence(severity: string, path: string, message: string): () => Node {
	return () => {
		const dom = document.createElement('span');
		const word = dom.appendChild(document.createElement('span'));
		word.className = 'cm-diagnostic-severity';
		word.textContent = severity === 'error' ? 'Error' : 'Warning';
		dom.appendChild(document.createTextNode(' '));
		if (path) {
			const p = dom.appendChild(document.createElement('span'));
			p.className = 'cm-diagnostic-path';
			p.textContent = path;
			dom.appendChild(document.createTextNode(': '));
		}
		for (const part of message.split(/(`[^`]+`)/)) {
			if (/^`[^`]+`$/.test(part)) dom.appendChild(document.createElement('code')).textContent = part.slice(1, -1);
			else if (part) dom.appendChild(document.createTextNode(part));
		}
		return dom;
	};
}

/** The diagnostics on screen, drawn by @codemirror/lint and listed under the editor. */
export function showFindings(view: EditorView, findings: readonly Diagnostic[]): void {
	const length = view.state.doc.length;
	const diagnostics: CmDiagnostic[] = findings.map((d) => ({
		from: Math.min(d.from, length),
		to: Math.min(Math.max(d.to, d.from), length),
		severity: d.severity,
		source: d.source === 'engine' ? 'engine' : d.source === 'syntax' ? 'YAML syntax' : 'schema',
		message: d.path ? `${d.path}: ${d.message}` : d.message,
		renderMessage: renderSentence(d.severity, d.path, d.message)
	}));
	view.dispatch(setDiagnostics(view.state, diagnostics));
}

/** An engine finding, with the issue as the engine wrote it. */
export type EngineFinding = Diagnostic & { field: string; raw: string };

/**
 * The engine's last findings, carried through edits. They were placed on the
 * text the engine read; as the document is typed they move with the text
 * they are about, until the next answer replaces them.
 */
export const setEngineFindings = StateEffect.define<EngineFinding[]>();
export const engineFindingsField = StateField.define<EngineFinding[]>({
	create: () => [],
	update(value, tr) {
		for (const effect of tr.effects) if (effect.is(setEngineFindings)) return effect.value;
		if (!tr.docChanged) return value;
		return value.map((d) => ({ ...d, from: tr.changes.mapPos(d.from, 1), to: tr.changes.mapPos(d.to, -1) }));
	}
});

// ---------------------------------------------------------------- completion

function completionSource(kind: DocumentKind) {
	return (context: CompletionContext): CompletionResult | null => {
		const text = context.state.doc.toString();
		const node = syntaxTree(context.state).resolveInner(context.pos, -1);
		for (let n: typeof node | null = node; n; n = n.parent) {
			if (/^(?:Comment|QuotedLiteral|BlockLiteral|BlockLiteralContent)$/.test(n.name)) return null;
		}
		const at = contextAt(text, context.pos);
		if (!at) return null;
		const root = SCHEMAS[kind];
		if (at.position === 'key') {
			if (!context.explicit && at.prefix === '') return null;
			const options = keyOptions(root, text, at.path).map((o) => ({
				label: o.label,
				detail: o.detail,
				info: o.info,
				apply: o.apply,
				type: 'property'
			}));
			return options.length ? { from: at.from, options, validFor: /^[\w.\-/]*$/ } : null;
		}
		const options = valueOptions(root, text, at.path).map((o) => ({ label: o.label, apply: o.apply, type: 'keyword' }));
		return options.length ? { from: at.from, options, validFor: /^[\w.\-/]*$/ } : null;
	};
}

// ---------------------------------------------------------------- key docs

/**
 * What the schema says about the key on a line, as a popup at the key: from
 * the pointer (hover, the pointer within the key) or the keyboard
 * (Ctrl-Shift-Space, the caret anywhere on the key's line).
 */
function keyDoc(view: EditorView, pos: number, kind: DocumentKind, anywhereOnLine: boolean): { tooltip: Tooltip; text: string } | null {
	const line = view.state.doc.lineAt(pos);
	const m = /^((?:[ \t]*-[ \t]+)*[ \t]*)([\w.\-/]+)[ \t]*:(?:[ \t]|$)/.exec(line.text);
	if (!m) return null;
	const keyFrom = line.from + m[1].length;
	const keyTo = keyFrom + m[2].length;
	if (!anywhereOnLine && (pos < keyFrom || pos > keyTo)) return null;
	const text = view.state.doc.toString();
	const at = contextAt(text, keyFrom);
	if (!at || at.position !== 'key') return null;
	const doc = hoverDoc(SCHEMAS[kind], text, [...at.path, m[2]]);
	if (!doc) return null;
	const title = doc.type ? `${doc.title} · ${doc.type}` : doc.title;
	return {
		text: [title, doc.description, doc.allowed.length ? `Takes ${doc.allowed.join(', ')}.` : ''].filter(Boolean).join('. '),
		tooltip: {
			pos: keyFrom,
			end: keyTo,
			above: true,
			create() {
				const dom = document.createElement('div');
				dom.className = 'cm-schema-doc';
				const head = dom.appendChild(document.createElement('p'));
				head.className = 'cm-schema-doc-title';
				head.textContent = title;
				if (doc.description) dom.appendChild(document.createElement('p')).textContent = doc.description;
				if (doc.allowed.length) {
					const allowed = dom.appendChild(document.createElement('p'));
					allowed.className = 'cm-schema-doc-allowed';
					allowed.textContent = doc.allowed.join(' · ');
				}
				return { dom };
			}
		}
	};
}

function hoverSource(kind: DocumentKind) {
	return hoverTooltip((view, pos) => keyDoc(view, pos, kind, false)?.tooltip ?? null);
}

/** The keyboard's key popup: shown by Ctrl-Shift-Space, gone at the next edit, move or Escape. */
const setKeyDoc = StateEffect.define<Tooltip | null>();
const keyDocField = StateField.define<Tooltip | null>({
	create: () => null,
	update(value, tr) {
		for (const effect of tr.effects) if (effect.is(setKeyDoc)) return effect.value;
		return tr.docChanged || tr.selection ? null : value;
	},
	provide: (field) => showTooltip.from(field)
});

function explainKey(kind: DocumentKind, announce: (text: string) => void) {
	return (view: EditorView): boolean => {
		const found = keyDoc(view, view.state.selection.main.head, kind, true);
		view.dispatch({ effects: setKeyDoc.of(found?.tooltip ?? null) });
		announce(found ? found.text : 'This line has no key the schema describes.');
		return true;
	};
}

/** Escape closes the key popups, then lets CodeMirror's own Escape run (closing a completion list, arming Escape-then-Tab). */
function closeKeyDocs(view: EditorView): boolean {
	if (view.state.field(keyDocField) || hasHoverTooltips(view.state)) view.dispatch({ effects: [setKeyDoc.of(null), closeHoverTooltips] });
	return false;
}

/**
 * Keeps the caret clear of the page's sticky bands — the top bar and the
 * title block (tokens.css, --spacing-bar and --spacing-titleblock) — when the
 * editor scrolls the window to it.
 */
const clearOfChrome = EditorView.scrollMargins.of(() => {
	const css = getComputedStyle(document.documentElement);
	const px = (name: string) => {
		const value = css.getPropertyValue(name).trim();
		const n = parseFloat(value);
		if (Number.isNaN(n)) return 0;
		return value.endsWith('rem') ? n * parseFloat(css.fontSize) : n;
	};
	const gap = px('--spacing-gutter');
	return { top: px('--spacing-bar') + gap, bottom: px('--spacing-titleblock') + gap };
});

// ---------------------------------------------------------------- theme

const SQUARE = 'calc(var(--spacing) * 2)';

const theme = EditorView.theme({
	'&': {
		color: 'var(--color-ink)',
		backgroundColor: 'var(--color-paper)',
		fontSize: 'var(--text-sm)',
		border: 'var(--rule-hair) solid var(--color-rule-strong)',
		borderRadius: 'var(--radius-sm)'
	},
	'&.cm-focused': { outline: 'var(--rule-frame) solid var(--color-focus)', outlineOffset: 'var(--rule-frame)' },
	'.cm-scroller': { fontFamily: 'var(--font-mono)', lineHeight: 'var(--text-sm--line-height)', fontVariantLigatures: 'none' },
	'.cm-content': { caretColor: 'var(--color-ink)', paddingBlock: 'calc(var(--spacing) * 2)' },
	'.cm-line': { paddingInline: 'calc(var(--spacing) * 3)' },
	'.cm-cursor, .cm-dropCursor': { borderLeftColor: 'var(--color-ink)' },
	'&.cm-editor .cm-selectionBackground, &.cm-editor.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground':
		{ background: 'var(--color-blue-tint)' },
	'.cm-content ::selection': { backgroundColor: 'var(--color-blue-tint)' },
	'&.cm-editor .cm-activeLine': { backgroundColor: 'transparent' },
	'&.cm-editor.cm-focused .cm-activeLine': { backgroundColor: 'var(--color-hover)' },
	'&.cm-editor .cm-gutters': {
		backgroundColor: 'var(--color-paper-sunken)',
		color: 'var(--color-ink-muted)',
		borderRight: 'var(--rule-hair) solid var(--color-rule)'
	},
	'&.cm-editor .cm-activeLineGutter': { backgroundColor: 'transparent' },
	'&.cm-editor.cm-focused .cm-activeLineGutter': {
		backgroundColor: 'var(--color-hover)',
		color: 'var(--color-ink)'
	},
	'.cm-lineNumbers .cm-gutterElement': { paddingInline: 'calc(var(--spacing) * 2)', fontVariantNumeric: 'tabular-nums' },
	'&.cm-editor .cm-specialChar': { color: 'var(--color-red-strong)' },
	'.cm-placeholder': { color: 'var(--color-ink-placeholder)' },

	// Popups: the overlay treatment (ui/DESIGN.md, "Overlays").
	'&.cm-editor .cm-tooltip': {
		backgroundColor: 'var(--color-sheet)',
		color: 'var(--color-ink)',
		border: 'var(--rule-hair) solid var(--color-ink)',
		borderRadius: 'var(--radius-md)',
		boxShadow: 'var(--shadow-overlay)',
		fontFamily: 'var(--font-sans)',
		fontSize: 'var(--text-sm)',
		lineHeight: 'var(--text-sm--line-height)',
		maxWidth: 'min(36rem, 90vw)'
	},
	'.cm-tooltip .cm-tooltip-arrow:before, .cm-tooltip .cm-tooltip-arrow:after': { display: 'none' },
	'.cm-tooltip.cm-tooltip-autocomplete > ul': { fontFamily: 'var(--font-mono)', maxHeight: 'calc(var(--spacing) * 60)' },
	'.cm-tooltip.cm-tooltip-autocomplete > ul > li': { paddingInline: 'calc(var(--spacing) * 2)', paddingBlock: 'var(--spacing)' },
	'&.cm-editor .cm-tooltip-autocomplete ul li[aria-selected]': {
		backgroundColor: 'var(--color-blue-tint)',
		color: 'var(--color-ink)',
		fontWeight: 'var(--font-weight-semibold)'
	},
	'.cm-completionDetail': { color: 'var(--color-ink-muted)', fontStyle: 'normal', marginInlineStart: 'calc(var(--spacing) * 3)' },
	'.cm-completionMatchedText': { textDecoration: 'none', color: 'var(--color-blue-strong)' },
	'.cm-completionIcon': { display: 'none' },
	'.cm-tooltip.cm-completionInfo': { padding: 'calc(var(--spacing) * 2) calc(var(--spacing) * 3)', maxWidth: 'min(28rem, 60vw)' },
	'.cm-schema-doc': { padding: 'calc(var(--spacing) * 2) calc(var(--spacing) * 3)', display: 'grid', gap: 'var(--spacing)' },
	'.cm-schema-doc-title': { fontFamily: 'var(--font-mono)', fontWeight: 'var(--font-weight-semibold)' },
	'.cm-schema-doc-allowed': { fontFamily: 'var(--font-mono)', color: 'var(--color-ink-secondary)' },

	// Findings: the severity marks of ui/DESIGN.md ("Notes and Holds") — a
	// filled square for an error, an outlined one for a warning, in red — and
	// an underline drawn as text decoration (a data-URL image cannot read a
	// token). Errors wave; warnings dot.
	'.cm-lintRange-error, .cm-lintRange-warning, .cm-lintRange-info, .cm-lintRange-hint': {
		backgroundImage: 'none',
		paddingBottom: '0',
		textDecorationLine: 'underline',
		textDecorationColor: 'var(--color-red)',
		textDecorationThickness: 'var(--rule-region)',
		textUnderlineOffset: 'var(--underline-offset)',
		textDecorationSkipInk: 'none'
	},
	'.cm-lintRange-error': { textDecorationStyle: 'wavy' },
	'.cm-lintRange-warning, .cm-lintRange-info, .cm-lintRange-hint': { textDecorationStyle: 'dotted' },
	'.cm-gutter-lint': { width: 'calc(var(--spacing) * 4)' },
	'.cm-gutter-lint .cm-gutterElement': { padding: '0', display: 'flex', alignItems: 'center', justifyContent: 'center' },
	'.cm-lint-marker': { content: 'normal', width: SQUARE, height: SQUARE, boxSizing: 'border-box' },
	'.cm-lint-marker-error': { content: 'normal', backgroundColor: 'var(--color-red)' },
	'.cm-lint-marker-warning, .cm-lint-marker-info': {
		content: 'normal',
		backgroundColor: 'transparent',
		border: 'var(--rule-region) solid var(--color-red)'
	},
	'.cm-diagnostic': { padding: 'calc(var(--spacing) * 2) calc(var(--spacing) * 3)', marginLeft: '0', whiteSpace: 'pre-wrap' },
	'.cm-diagnostic-error': { borderLeft: 'var(--rule-frame) solid var(--color-red)' },
	'.cm-diagnostic-warning, .cm-diagnostic-info, .cm-diagnostic-hint': { borderLeft: 'var(--rule-frame) dotted var(--color-red)' },
	'.cm-diagnostic-path': { fontWeight: 'var(--font-weight-semibold)' },
	'.cm-diagnostic-severity': {
		fontSize: 'var(--text-2xs)',
		fontWeight: 'var(--font-weight-semibold)',
		letterSpacing: 'var(--tracking-caps)',
		textTransform: 'uppercase',
		color: 'var(--color-red-strong)'
	},
	'.cm-diagnostic code': { fontFamily: 'var(--font-mono)' },
	'.cm-diagnosticSource': { color: 'var(--color-ink-muted)', opacity: '1', fontFamily: 'var(--font-sans)' },
	'.cm-tooltip-lint': { padding: '0' },
	'&.cm-editor .cm-panels': { backgroundColor: 'var(--color-paper-sunken)', color: 'var(--color-ink)' },
	'.cm-panel.cm-panel-lint ul [aria-selected]': { backgroundColor: 'var(--color-blue-tint)', backgroundImage: 'none' },
	'.cm-panel.cm-panel-lint ul:focus [aria-selected]': { backgroundColor: 'var(--color-blue-tint)', color: 'var(--color-ink)' },
	'.cm-panel.cm-panel-lint button[name=close]': { color: 'var(--color-ink-secondary)' }
});

// ---------------------------------------------------------------- assembly

/**
 * CodeMirror's default keys, less the one that would take Ctrl-K from the
 * command palette (on a Mac, Ctrl-K deletes to the line's end): the palette
 * opens from inside any field (ui/DESIGN.md, "Command palette").
 */
const keys: readonly KeyBinding[] = [
	{ key: 'Escape', run: closeKeyDocs },
	...defaultKeymap.filter((b) => b.mac !== 'Ctrl-k' && b.key !== 'Ctrl-k'),
	...historyKeymap,
	...completionKeymap,
	...lintKeymap,
	indentWithTab
];

export interface EditorOptions {
	kind: DocumentKind;
	/** The editor's accessible name: a visible label's id, or the words themselves. */
	labelledby?: string;
	label: string;
	describedBy: string;
	testid: string;
	placeholder: string;
	onchange: (update: ViewUpdate) => void;
	/** Says a sentence to assistive technology (the field's live region). */
	announce: (text: string) => void;
}

export function editorExtensions(o: EditorOptions): Extension[] {
	return [
		lineNumbers(),
		lintGutter(),
		highlightSpecialChars(),
		highlightActiveLine(),
		highlightActiveLineGutter(),
		history(),
		indentUnit.of('  '),
		EditorState.tabSize.of(2),
		yaml(),
		syntaxColours,
		engineFindingsField,
		autocompletion({ override: [completionSource(o.kind)], icons: false }),
		hoverSource(o.kind),
		keyDocField,
		clearOfChrome,
		placeholderText(o.placeholder),
		keymap.of([{ key: 'Ctrl-Shift-Space', run: explainKey(o.kind, o.announce) }, ...keys]),
		theme,
		EditorView.contentAttributes.of({
			...(o.labelledby ? { 'aria-labelledby': o.labelledby } : { 'aria-label': o.label }),
			'aria-describedby': o.describedBy,
			'aria-placeholder': o.placeholder,
			'data-testid': o.testid,
			// Already in the Tab order as an editable region; said outright so
			// a scrolling editor reads as a focusable region (axe,
			// scrollable-region-focusable) to tools that do not count
			// contenteditable.
			tabindex: '0',
			spellcheck: 'false',
			autocapitalize: 'off',
			autocorrect: 'off'
		}),
		EditorView.updateListener.of(o.onchange)
	];
}

/**
 * The line break a document was given in. CodeMirror splits on any break and
 * counts each as one character, so findings are placed on its own text; the
 * document handed back keeps the break it came with, so a stored source with
 * CRLF line ends that nobody edited is byte for byte what was stored.
 */
export function separatorOf(text: string): '\r\n' | '\n' {
	return text.includes('\r\n') ? '\r\n' : '\n';
}

/** The document as text, in the given line break. */
export function documentText(doc: Text, separator: '\r\n' | '\n'): string {
	return separator === '\n' ? doc.toString() : doc.toString().split('\n').join(separator);
}
