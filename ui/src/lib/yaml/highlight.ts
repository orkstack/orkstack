// One classifier colours YAML everywhere the UI shows it: the editor (as
// decorations over CodeMirror's own syntax tree), the automation export and
// the version diff (as spans). The roles are the syntax pencils of
// ui/src/tokens.css (--color-syntax-<role>), which the site's code themes read
// too, so a key, a string or a template looks the same in the UI and the docs.
//
// The Lezer YAML grammar tags every plain scalar alike; whether `300`, `true`
// or `nightly` is a number, a literal or a string is decided here, by YAML
// 1.2's core schema, which is how an editor reads it. That is a colour, not a
// verdict: whether the engine takes `yes` is the engine's answer (./server.ts).
// A `{{ … }}` inside any string is a template (docs/DSL.md, "Templating").
import { parser } from '@lezer/yaml';
import type { Tree } from '@lezer/common';

export const SYNTAX_ROLES = ['key', 'string', 'number', 'literal', 'template', 'meta', 'comment', 'punctuation'] as const;
export type SyntaxRole = (typeof SYNTAX_ROLES)[number];

export interface Span {
	from: number;
	to: number;
	role: SyntaxRole;
}

export interface Segment {
	text: string;
	role: SyntaxRole | null;
}

const NULL = /^(?:null|Null|NULL|~)$/;
const BOOL = /^(?:true|True|TRUE|false|False|FALSE)$/;
const NUMBER = /^(?:[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+|[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$/;
const TEMPLATE = /\{\{[\s\S]*?\}\}/g;

const PUNCTUATION = new Set([':', ',', '-', '?', '[', ']', '{', '}', 'BlockLiteralHeader']);
const META = new Set(['Anchor', 'Alias', 'Tag', 'DirectiveName', 'DirectiveContent', 'DirectiveEnd', 'DocEnd']);

function scalarRole(text: string): SyntaxRole {
	const t = text.trim();
	if (NULL.test(t) || BOOL.test(t)) return 'literal';
	if (NUMBER.test(t)) return 'number';
	return 'string';
}

/** A string's spans, with every `{{ … }}` in it split out as a template. */
function stringSpans(text: string, from: number, to: number, out: Span[]): void {
	let at = from;
	const body = text.slice(from, to);
	for (const m of body.matchAll(TEMPLATE)) {
		const start = from + (m.index ?? 0);
		if (start > at) out.push({ from: at, to: start, role: 'string' });
		out.push({ from: start, to: start + m[0].length, role: 'template' });
		at = start + m[0].length;
	}
	if (at < to) out.push({ from: at, to, role: 'string' });
}

/** The highlighted spans of `text` between `from` and `to`, in order, from its syntax tree. */
export function classify(tree: Tree, text: string, from = 0, to = text.length): Span[] {
	const out: Span[] = [];
	tree.iterate({
		from,
		to,
		enter(node) {
			const name = node.name;
			if (name === 'Key') {
				out.push({ from: node.from, to: node.to, role: 'key' });
				return false;
			}
			if (name === 'Comment') out.push({ from: node.from, to: node.to, role: 'comment' });
			else if (META.has(name)) out.push({ from: node.from, to: node.to, role: 'meta' });
			else if (PUNCTUATION.has(name)) out.push({ from: node.from, to: node.to, role: 'punctuation' });
			else if (name === 'QuotedLiteral' || name === 'BlockLiteralContent') stringSpans(text, node.from, node.to, out);
			else if (name === 'Literal') {
				const role = scalarRole(text.slice(node.from, node.to));
				if (role === 'string') stringSpans(text, node.from, node.to, out);
				else out.push({ from: node.from, to: node.to, role });
			} else return;
			return false;
		}
	});
	return out.filter((s) => s.to > s.from);
}

/** Spans as consecutive segments covering the whole text, gaps unstyled. */
function segmentsOf(text: string, spans: readonly Span[], from = 0, to = text.length): Segment[] {
	const out: Segment[] = [];
	let at = from;
	for (const s of spans) {
		const start = Math.max(s.from, at);
		const end = Math.min(s.to, to);
		if (end <= start) continue;
		if (start > at) out.push({ text: text.slice(at, start), role: null });
		out.push({ text: text.slice(start, end), role: s.role });
		at = end;
	}
	if (at < to) out.push({ text: text.slice(at, to), role: null });
	return out;
}

/** A YAML text as segments: concatenated, they are the text, byte for byte. */
export function yamlSegments(text: string): Segment[] {
	return segmentsOf(text, classify(parser.parse(text), text));
}

/** Each line of a text's segments, split at line breaks (the breaks themselves dropped). */
function lineSegments(text: string): Segment[][] {
	const lines: Segment[][] = [[]];
	for (const seg of yamlSegments(text)) {
		const parts = seg.text.split('\n');
		parts.forEach((part, i) => {
			if (i > 0) lines.push([]);
			if (part !== '') lines[lines.length - 1].push({ text: part, role: seg.role });
		});
	}
	return lines;
}

export interface DiffRow {
	line: string;
	segments: Segment[];
}

/**
 * A unified diff of two YAML documents, coloured as YAML. Each hunk is read as
 * its two sides — the old text from its context and removed lines, the new
 * from its context and added lines — so a line is coloured in the document it
 * belongs to. Removed lines stay uncoloured (DiffView sets them back); the
 * headers and the "no newline" marker are not YAML. Every row's segments are
 * its line, byte for byte.
 */
export function diffSegments(diff: string): DiffRow[] {
	const lines = diff.split('\n');
	if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
	const rows: DiffRow[] = lines.map((line) => ({ line, segments: [{ text: line, role: null }] }));
	let i = 0;
	while (i < lines.length) {
		if (!lines[i].startsWith('@@')) {
			i++;
			continue;
		}
		const start = ++i;
		while (i < lines.length && /^[ +-]/.test(lines[i]) && !lines[i].startsWith('---') && !lines[i].startsWith('+++')) i++;
		const hunk = lines.slice(start, i);
		const newSide = hunk.map((l, k) => ({ l, k })).filter(({ l }) => l[0] === ' ' || l[0] === '+');
		const coloured = lineSegments(newSide.map(({ l }) => l.slice(1)).join('\n'));
		newSide.forEach(({ l, k }, n) => {
			rows[start + k].segments = [{ text: l[0], role: null }, ...(coloured[n] ?? [])];
		});
	}
	for (const row of rows) {
		// A segment list that lost a byte would change what is copied: never.
		if (row.segments.map((s) => s.text).join('') !== row.line) row.segments = [{ text: row.line, role: null }];
	}
	return rows;
}
