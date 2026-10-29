// Parsing an authored document for the editor, and finding a node by path.
//
// The `yaml` package reads YAML 1.2, as every editor and language server
// does, and keeps each node's source range — what places a finding on the
// key or value a person would point at. It is NOT the engine's reader: the
// engine decodes with yaml.v3 and then applies the strict YAML-to-JSON
// mapping (docs/JSON.md), so `yes` here is a string that the engine refuses
// in a data position. The editor never decides that; the engine's answer
// does (./server.ts).
import { isMap, isScalar, isSeq, parseDocument, type Document, type Node } from 'yaml';
import type { Diagnostic, PathSegment } from './types';

export interface Parsed {
	text: string;
	doc: Document.Parsed;
	/** The document as JSON data, or null while it has a syntax error. */
	value: unknown;
	syntax: Diagnostic[];
}

export function parseYaml(text: string): Parsed {
	const doc = parseDocument(text, {
		version: '1.2',
		uniqueKeys: true,
		strict: true,
		prettyErrors: false
	});
	const syntax: Diagnostic[] = doc.errors.map((error) => ({
		from: error.pos[0],
		to: Math.max(error.pos[1], error.pos[0]),
		severity: 'error',
		source: 'syntax',
		message: error.message.split('\n')[0],
		path: ''
	}));
	let value: unknown = null;
	if (syntax.length === 0) {
		try {
			value = doc.toJS({ maxAliasCount: 100 });
		} catch (err) {
			syntax.push({ from: 0, to: 0, severity: 'error', source: 'syntax', message: String(err), path: '' });
		}
	}
	return { text, doc, value, syntax };
}

/** 1-based line and column of an offset, clamped to the text. */
export function lineCol(text: string, offset: number): { line: number; col: number } {
	const at = Math.max(0, Math.min(offset, text.length));
	let line = 1;
	let start = 0;
	for (let i = text.indexOf('\n'); i !== -1 && i < at; i = text.indexOf('\n', i + 1)) {
		line++;
		start = i + 1;
	}
	return { line, col: at - start + 1 };
}

/** The engine's field path (`tasks[2].with.body`, `pins.etl`) as segments. */
export function parseFieldPath(field: string): PathSegment[] {
	const out: PathSegment[] = [];
	for (const part of field.split('.')) {
		if (part === '') continue;
		const m = /^([^[\]]*)((?:\[\d+\])*)$/.exec(part);
		if (!m) {
			out.push(part);
			continue;
		}
		if (m[1] !== '') out.push(m[1]);
		for (const index of m[2].matchAll(/\[(\d+)\]/g)) out.push(Number(index[1]));
	}
	return out;
}

/** A path in the engine's own spelling: `tasks[0].with.op`. */
export function displayPath(path: readonly PathSegment[]): string {
	let out = '';
	for (const seg of path) out += typeof seg === 'number' ? `[${seg}]` : out === '' ? seg : `.${seg}`;
	return out;
}

/** A JSON pointer (`/tasks/0/with`) as segments; a numeric step stays a number. */
export function pointerPath(pointer: string): PathSegment[] {
	if (pointer === '') return [];
	return pointer
		.slice(1)
		.split('/')
		.map((p) => p.replace(/~1/g, '/').replace(/~0/g, '~'))
		.map((p) => (/^\d+$/.test(p) ? Number(p) : p));
}

export interface Range {
	from: number;
	to: number;
}

function nodeRange(node: Node | null | undefined): Range | null {
	if (!node?.range) return null;
	return { from: node.range[0], to: node.range[1] };
}

/** The node at a path, and the key node it hangs from when its parent is a mapping. */
export function nodeAt(doc: Document.Parsed, path: readonly PathSegment[]): { node: Node | null; key: Node | null } | null {
	let node = doc.contents as Node | null;
	let key: Node | null = null;
	for (const seg of path) {
		if (isMap(node)) {
			const pair = node.items.find((p) => isScalar(p.key) && String(p.key.value) === String(seg));
			if (!pair) return null;
			key = pair.key as Node;
			node = (pair.value as Node | null) ?? null;
		} else if (isSeq(node) && typeof seg === 'number') {
			if (seg >= node.items.length) return null;
			key = null;
			node = node.items[seg] as Node | null;
		} else {
			return null;
		}
	}
	return { node, key };
}

/**
 * The range a finding at `path` should cover. `key` is the mapping key the
 * node hangs from (a sequence item has none, so its own range stands in);
 * `value` is the node itself.
 */
export function rangeAt(doc: Document.Parsed, path: readonly PathSegment[], which: 'key' | 'value'): Range | null {
	const found = nodeAt(doc, path);
	if (!found) return null;
	if (which === 'key') return nodeRange(found.key) ?? nodeRange(found.node);
	return nodeRange(found.node);
}

/**
 * Where a finding about a whole mapping goes: the key it hangs from, or, for
 * a sequence item or the document itself, its first key, so the mark sits on
 * one short line rather than under a block.
 */
export function headAt(doc: Document.Parsed, path: readonly PathSegment[]): Range | null {
	const found = nodeAt(doc, path);
	if (!found) return null;
	if (found.key) return nodeRange(found.key);
	const node = found.node;
	if (isMap(node) && node.items.length > 0) return nodeRange(node.items[0].key as Node);
	if (isSeq(node) && node.items.length > 0) return nodeRange(node.items[0] as Node);
	return nodeRange(node);
}
