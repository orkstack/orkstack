<script lang="ts">
	// The editor itself, loaded only when a page shows one (YamlField.svelte
	// imports it on mount). It owns the CodeMirror view and nothing else: the
	// value is the page's (bound), the engine checks and the findings list are
	// the session's (session.svelte.ts), and every rule is a pure module.
	import { onMount, untrack } from 'svelte';
	import { EditorView, type ViewUpdate } from '@codemirror/view';
	import { EditorState } from '@codemirror/state';
	import { documentText, editorExtensions, engineFindingsField, separatorOf, setEngineFindings, showFindings } from './editor';
	import { lintDocument } from './lint';
	import { engineFindings } from './server';
	import { localFindings } from './status';
	import type { Finding, YamlSession } from './session.svelte';
	import type { Diagnostic, DocumentKind } from './types';

	interface Props {
		value: string;
		kind: DocumentKind;
		namespace: string;
		session: YamlSession;
		testid: string;
		label: string;
		labelledby?: string;
		describedBy: string;
		placeholder: string;
		minLines: number;
		focus: boolean;
		/** The textarea's selection when the editor took its place, so the caret stays put. */
		selection: { anchor: number; head: number } | null;
		oninput?: () => void;
	}

	let {
		value = $bindable(),
		kind,
		namespace,
		session,
		testid,
		label,
		labelledby,
		describedBy,
		placeholder,
		minLines,
		focus,
		selection,
		oninput
	}: Props = $props();

	let host: HTMLDivElement;
	let view: EditorView | null = null;
	let separator = separatorOf(untrack(() => value));
	let lintTimer: ReturnType<typeof setTimeout> | null = null;

	const LINT_DELAY_MS = 250;

	function place(view: EditorView, d: Diagnostic): Finding {
		const line = view.state.doc.lineAt(Math.min(d.from, view.state.doc.length));
		return { ...d, line: line.number, col: d.from - line.from + 1 };
	}

	/** The editor's own findings and the engine's, recomputed and drawn. */
	function refresh() {
		if (!view) return;
		const text = view.state.doc.toString();
		const local = lintDocument(text, kind);
		const parses = !local.some((d) => d.source === 'syntax');
		// The engine is asked about the document as the page holds it: the
		// bound value, byte for byte, line breaks and all.
		session.schedule(value, namespace, parses);
		const shown = localFindings(local, session.engine, value, namespace);
		const engine = view.state.field(engineFindingsField);
		session.localFindings = shown.map((d) => place(view!, d));
		session.engineFindings = engine.map((d) => place(view!, d));
		showFindings(view, [...engine, ...shown]);
	}

	function scheduleRefresh() {
		if (lintTimer !== null) clearTimeout(lintTimer);
		lintTimer = setTimeout(refresh, LINT_DELAY_MS);
	}

	function onchange(update: ViewUpdate) {
		if (!update.docChanged) return;
		const next = documentText(update.state.doc, separator);
		if (next !== value) {
			value = next;
			oninput?.();
		}
		scheduleRefresh();
	}

	onMount(() => {
		view = new EditorView({
			parent: host,
			state: EditorState.create({
				doc: value,
				selection: selection
					? { anchor: Math.min(selection.anchor, value.length), head: Math.min(selection.head, value.length) }
					: undefined,
				extensions: editorExtensions({
					kind,
					label,
					labelledby,
					describedBy,
					testid,
					placeholder,
					onchange,
					announce: (text) => (session.announcement = text)
				})
			})
		});
		session.jump = (from, to) => {
			if (!view) return;
			const length = view.state.doc.length;
			view.dispatch({ selection: { anchor: Math.min(from, length), head: Math.min(to, length) }, scrollIntoView: true });
			view.focus();
		};
		session.editor = 'ready';
		if (focus) view.focus();
		refresh();
		return () => {
			if (lintTimer !== null) clearTimeout(lintTimer);
			session.jump = null;
			view?.destroy();
			view = null;
		};
	});

	// The page set the value (Import YAML, an example, a reload): replace the
	// document, and keep the line break the new text came with. Text that is
	// already the document but for its line breaks (a source mixing CRLF and
	// LF) is left alone, so nobody's document changes until they edit it.
	$effect(() => {
		const next = value;
		untrack(() => {
			if (!view || view.state.doc.toString() === next.replace(/\r\n?/g, '\n')) return;
			separator = separatorOf(next);
			view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: next } });
		});
	});

	// Another namespace is another question for the engine.
	$effect(() => {
		void namespace;
		untrack(() => refresh());
	});

	// The engine answered: place its issues and redraw.
	// While a newer check is pending, the last answer's findings stay, moved
	// with the text they are about (engineFindingsField); the status says the
	// check is running.
	let handled: { engine: unknown; namespace: string } | null = null;
	$effect(() => {
		const engine = session.engine;
		const ns = namespace;
		untrack(() => {
			if (!view || (handled?.engine === engine && handled.namespace === ns)) return;
			handled = { engine, namespace: ns };
			const current = value;
			if (engine.kind === 'answered' && engine.text === current && engine.namespace === namespace) {
				const placed = engineFindings(view.state.doc.toString(), engine.issues).map((d, i) => ({
					...d,
					field: engine.issues[i].field,
					raw: engine.issues[i].message
				}));
				view.dispatch({ effects: setEngineFindings.of(placed) });
				refresh();
			} else if (engine.kind === 'idle' || (engine.kind === 'failed' && engine.text === current)) {
				view.dispatch({ effects: setEngineFindings.of([]) });
				refresh();
			}
		});
	});
</script>

<div
	bind:this={host}
	class="yaml-editor"
	style:--_lines={minLines}
	data-testid="{testid}-editor"
></div>

<style>
	/* The editor is at least as tall as the field it replaces, and grows
	   with its document up to most of the viewport, then scrolls inside. */
	.yaml-editor :global(.cm-editor) {
		min-height: calc(var(--_lines) * var(--text-sm--line-height) + var(--spacing) * 4);
		max-height: 75vh;
	}
	.yaml-editor :global(.cm-scroller) {
		min-height: inherit;
	}
</style>
