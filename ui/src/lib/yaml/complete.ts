// Completion and hover from the generated schema.
//
// Two halves. contextAt reads the cursor's path from the block structure of
// the text itself — indentation, `key:` lines and `- ` items — because while a
// key is half typed the document does not parse, and that is exactly when
// completion is asked for. schemasAt then walks the schema along that path the
// way a validator would: $ref, allOf, anyOf, and each if/then/else decided by
// ajv against the data written so far, so a task with `uses: http` is offered
// the http image's `with:` keys and an awaited task is never offered
// `command:`. Every word offered and every hover sentence is the schema's.
import Ajv, { type ValidateFunction } from 'ajv';
import { parseDocument } from 'yaml';
import { schemaAt } from './schemas';
import type { PathSegment } from './types';

type Schema = Record<string, unknown>;

export interface CursorContext {
	/** The mapping a key is typed into, or the key whose value is typed. */
	path: PathSegment[];
	position: 'key' | 'value';
	/** What is typed so far, and where it starts. */
	prefix: string;
	from: number;
}

export interface Option {
	label: string;
	/** A short type, shown beside the label. */
	detail?: string;
	/** The schema's description of the key. */
	info?: string;
	/** What accepting inserts. */
	apply: string;
}

export interface HoverDoc {
	title: string;
	description: string;
	type: string;
	allowed: string[];
}

// ---------------------------------------------------------------- path

interface LineInfo {
	first: number;
	dashes: number[];
	key: string | null;
	keyCol: number;
	/** The key's value is a block scalar (`|`, `>`): the lines below are text. */
	block: boolean;
	/** The key's value is written on its line: the lines below are not its children. */
	inline: boolean;
}

function lineInfo(line: string): LineInfo | null {
	const trimmed = line.trimStart();
	if (trimmed === '' || trimmed.startsWith('#')) return null;
	const first = line.length - trimmed.length;
	const dashes: number[] = [];
	let col = first;
	let rest = trimmed;
	for (let m = /^-(\s+|$)/.exec(rest); m; m = /^-(\s+|$)/.exec(rest)) {
		dashes.push(col);
		col += m[0].length;
		rest = rest.slice(m[0].length);
	}
	const key = /^([\w.\-/]+|"[^"]*"|'[^']*')[ \t]*:(?:[ \t]+|$)/.exec(rest);
	if (!key) return { first, dashes, key: null, keyCol: col, block: false, inline: false };
	const value = rest.slice(key[0].length).replace(/(^|\s)#.*$/, '').trim();
	return {
		first,
		dashes,
		key: key[1].replace(/^["']|["']$/g, ''),
		keyCol: col,
		block: /^(?:&\S+\s+)?[|>][-+0-9]*$/.test(value),
		inline: value !== '' && !/^&\S+$/.test(value)
	};
}

/** The 0-based index of the sequence item whose dash is at column `dash` on line `at`. */
function itemIndex(lines: string[], at: number, dash: number): number {
	let index = 0;
	for (let j = at - 1; j >= 0; j--) {
		const info = lineInfo(lines[j]);
		if (!info) continue;
		if (info.first < dash) break;
		if (info.first === dash && info.dashes[0] === dash) index++;
	}
	return index;
}

/** The path of the mapping whose keys start at column `col` on line `at`, or null inside text. */
function containerPath(lines: string[], at: number, col: number): PathSegment[] | null {
	const path: PathSegment[] = [];
	let t = col;
	const own = lineInfo(lines[at]);
	for (const d of (own?.dashes ?? []).filter((d) => d < col).reverse()) {
		path.unshift(itemIndex(lines, at, d));
		t = d;
	}
	for (let i = at - 1; i >= 0 && t > 0; i--) {
		const info = lineInfo(lines[i]);
		if (!info || info.first >= t) continue;
		if (info.key !== null && info.keyCol < t) {
			if (info.block || info.inline) return null;
			path.unshift(info.key);
			t = info.keyCol;
		} else if (info.dashes.length === 0) {
			return null;
		}
		for (const d of info.dashes.filter((d) => d < t).reverse()) {
			path.unshift(itemIndex(lines, i, d));
			t = d;
		}
	}
	return path;
}

export function contextAt(text: string, pos: number): CursorContext | null {
	const lineStart = text.lastIndexOf('\n', pos - 1) + 1;
	const before = text.slice(lineStart, pos);
	if (/(^|\s)#/.test(before)) return null;
	const lines = text.slice(0, lineStart).split('\n');
	lines[lines.length - 1] = text.slice(lineStart, text.indexOf('\n', pos) === -1 ? text.length : text.indexOf('\n', pos));
	const at = lines.length - 1;

	const value = /^((?:[ \t]*-[ \t]+)*[ \t]*)([\w.\-/]+)[ \t]*:[ \t]+([^\s[{|>"'&*!][^\s]*|)$/.exec(before);
	if (value) {
		const parent = containerPath(lines, at, value[1].length);
		if (!parent) return null;
		return { path: [...parent, value[2]], position: 'value', prefix: value[3], from: pos - value[3].length };
	}
	const key = /^((?:[ \t]*-[ \t]+)*[ \t]*)([\w.\-/]*)$/.exec(before);
	if (key) {
		const path = containerPath(lines, at, key[1].length);
		if (!path) return null;
		return { path, position: 'key', prefix: key[2], from: pos - key[2].length };
	}
	return null;
}

// ---------------------------------------------------------------- schema

// Conditions are evaluated by their own ajv: a bare `if` subschema has no
// type of its own, which the strict validator (./lint.ts) would refuse to
// compile out of its context.
const conditions = new Ajv({ strict: false, allErrors: false });
const compiled = new WeakMap<object, ValidateFunction>();

function holds(ifSchema: unknown, data: unknown, root: Schema): boolean {
	if (typeof ifSchema === 'boolean') return ifSchema;
	if (!ifSchema || typeof ifSchema !== 'object') return true;
	let validate = compiled.get(ifSchema);
	if (!validate) {
		const needsRoot = JSON.stringify(ifSchema).includes('"$ref"');
		validate = conditions.compile(needsRoot ? { definitions: root.definitions, ...(ifSchema as Schema) } : (ifSchema as Schema));
		compiled.set(ifSchema, validate);
	}
	return validate(data) as boolean;
}

function expand(schemas: unknown[], data: unknown, root: Schema): Schema[] {
	const out: Schema[] = [];
	const seen = new Set<unknown>();
	const visit = (s: unknown) => {
		if (!s || typeof s !== 'object' || seen.has(s)) return;
		seen.add(s);
		const schema = s as Schema;
		out.push(schema);
		if (typeof schema.$ref === 'string') visit(schemaAt(root, schema.$ref));
		for (const key of ['allOf', 'anyOf', 'oneOf']) for (const x of (schema[key] as unknown[]) ?? []) visit(x);
		if ('if' in schema && data !== undefined) visit(holds(schema.if, data, root) ? schema.then : schema.else);
	};
	schemas.forEach(visit);
	return out;
}

/** The subschemas a child takes, and whether any applicable schema forbids it. */
function children(schemas: Schema[], seg: PathSegment): { next: unknown[]; forbidden: boolean } {
	const next: unknown[] = [];
	let forbidden = false;
	for (const s of schemas) {
		if (typeof seg === 'number') {
			if (Array.isArray(s.items)) next.push(s.items[seg]);
			else if (s.items !== undefined) next.push(s.items);
			continue;
		}
		const props = (s.properties ?? {}) as Record<string, unknown>;
		if (seg in props) {
			if (props[seg] === false) forbidden = true;
			else next.push(props[seg]);
			continue;
		}
		let matched = false;
		for (const [pattern, sub] of Object.entries((s.patternProperties ?? {}) as Record<string, unknown>)) {
			if (new RegExp(pattern).test(seg)) {
				next.push(sub);
				matched = true;
			}
		}
		if (!matched && s.additionalProperties && typeof s.additionalProperties === 'object') next.push(s.additionalProperties);
	}
	return { next, forbidden };
}

function child(data: unknown, seg: PathSegment): unknown {
	if (data === null || typeof data !== 'object') return undefined;
	return (data as Record<string, unknown>)[seg as string];
}

/** The schemas in force at a path, or null when the path is forbidden or unknown. */
export function schemasAt(root: Schema, value: unknown, path: readonly PathSegment[]): Schema[] | null {
	let data = value;
	let schemas = expand([root], data, root);
	for (const seg of path) {
		const { next, forbidden } = children(schemas, seg);
		if (forbidden || next.length === 0) return null;
		data = child(data, seg);
		schemas = expand(next, data, root);
	}
	return schemas;
}

/** The document as data, as far as it parses: completion runs while it does not. */
export function draftValue(text: string): unknown {
	try {
		return parseDocument(text, { version: '1.2', uniqueKeys: false, strict: false }).toJS({ maxAliasCount: 100 }) ?? {};
	} catch {
		return {};
	}
}

function valueAt(value: unknown, path: readonly PathSegment[]): unknown {
	return path.reduce<unknown>((data, seg) => child(data, seg), value);
}

const TYPE: Record<string, string> = { object: 'mapping', array: 'list' };

function typeOf(schemas: Schema[]): string {
	const types = new Set<string>();
	for (const s of schemas) {
		if (typeof s.type === 'string') types.add(TYPE[s.type] ?? s.type);
		if (Array.isArray(s.type)) for (const t of s.type) types.add(TYPE[String(t)] ?? String(t));
		if (Array.isArray(s.enum) || 'const' in s) types.add('word');
	}
	if (types.has('word')) return 'word';
	return [...types].join(' | ');
}

function descriptionOf(schemas: Schema[]): string {
	return (schemas.find((s) => typeof s.description === 'string')?.description as string) ?? '';
}

function allowedOf(schemas: Schema[]): string[] {
	const out: string[] = [];
	const add = (v: unknown) => {
		const word = typeof v === 'string' ? v : JSON.stringify(v);
		if (!out.includes(word)) out.push(word);
	};
	for (const s of schemas) {
		if (Array.isArray(s.enum)) s.enum.forEach(add);
		if ('const' in s) add(s.const);
	}
	if (out.length === 0 && schemas.some((s) => s.type === 'boolean' || (Array.isArray(s.type) && s.type.length === 1 && s.type[0] === 'boolean'))) {
		out.push('true', 'false');
	}
	return out;
}

/** The keys a mapping at `path` may still take. */
export function keyOptions(root: Schema, text: string, path: readonly PathSegment[]): Option[] {
	const value = draftValue(text);
	const schemas = schemasAt(root, value, path);
	if (!schemas) return [];
	const written = valueAt(value, path);
	const present = new Set(written && typeof written === 'object' && !Array.isArray(written) ? Object.keys(written) : []);
	const forbidden = new Set<string>();
	const keys = new Map<string, unknown[]>();
	for (const s of schemas) {
		for (const [key, sub] of Object.entries((s.properties ?? {}) as Record<string, unknown>)) {
			if (sub === false) forbidden.add(key);
			else keys.set(key, [...(keys.get(key) ?? []), sub]);
		}
	}
	const out: Option[] = [];
	for (const [key, subs] of keys) {
		if (forbidden.has(key) || present.has(key)) continue;
		const expanded = expand(subs, undefined, root);
		const type = typeOf(expanded);
		const nested = type === 'mapping' || type === 'list';
		out.push({ label: key, detail: type || undefined, info: descriptionOf(expanded) || undefined, apply: nested ? `${key}:` : `${key}: ` });
	}
	return out;
}

/** The words a key at `path` takes, if it takes words or a boolean. */
export function valueOptions(root: Schema, text: string, path: readonly PathSegment[]): Option[] {
	const schemas = schemasAt(root, draftValue(text), path);
	if (!schemas) return [];
	return allowedOf(schemas).map((word) => ({ label: word, apply: word }));
}

/** What the schema says about the key at `path`. */
export function hoverDoc(root: Schema, text: string, path: readonly PathSegment[]): HoverDoc | null {
	if (path.length === 0) return null;
	const schemas = schemasAt(root, draftValue(text), path);
	if (!schemas) return null;
	const description = descriptionOf(schemas);
	const allowed = allowedOf(schemas);
	const type = typeOf(schemas);
	if (!description && allowed.length === 0 && !type) return null;
	const last = path[path.length - 1];
	return { title: typeof last === 'number' ? `item ${last}` : last, description, type, allowed };
}
