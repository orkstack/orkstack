// The editor's own findings: YAML syntax first, then the document's shape
// against its kind's generated schema (docs/schemas/), each placed on the key
// or value it is about.
//
// The validator is ajv, the reference draft-07 implementation and the one
// ui/test/authoredSchemas.test.ts compiles the schemas with, under the same
// options. What this module adds is only the wording and the placement: a
// raw ajv error says "must NOT have additional properties" at `/tasks/0`; a
// person needs "unknown key `need`; did you mean `needs`?" on the line where
// `need` is written. Every sentence is derived from the schema the error came
// from, never from a table of OrkStack's keys kept here, so a key the
// generator adds is checked and named the day the schema is regenerated.
//
// These findings never decide whether a document is valid: the schema is
// deliberately stricter than the compiler in a few places (docs/DSL.md,
// "Editor support"), and it cannot see what only the engine can — the strict
// YAML-to-JSON mapping, needs cycles, interfaces, pools, policy. The engine's
// answer decides (./status.ts).
import Ajv, { type ErrorObject, type ValidateFunction } from 'ajv';
import addFormats from 'ajv-formats';
import { displayPath, headAt, parseYaml, pointerPath, rangeAt, type Parsed, type Range } from './document';
import { SCHEMAS, schemaAt } from './schemas';
import type { Diagnostic, DocumentKind, PathSegment } from './types';

const validators = new Map<DocumentKind, ValidateFunction>();
let ajv: Ajv | null = null;

/** One ajv for every kind and every `if` condition the walker evaluates. */
export function sharedAjv(): Ajv {
	if (!ajv) {
		ajv = new Ajv({ allErrors: true, strict: true, strictRequired: false, allowUnionTypes: true, verbose: true });
		// Exactly the formats the generator emits (ui/test/authoredSchemas.test.ts).
		addFormats(ajv, ['date-time']);
	}
	return ajv;
}

export function validatorFor(kind: DocumentKind): ValidateFunction {
	let validate = validators.get(kind);
	if (!validate) {
		validate = sharedAjv().compile(SCHEMAS[kind]);
		validators.set(kind, validate);
	}
	return validate;
}

/** Every finding the editor can make on its own, in document order. */
export function lintDocument(text: string, kind: DocumentKind): Diagnostic[] {
	if (text.trim() === '') return [];
	const parsed = parseYaml(text);
	if (parsed.syntax.length > 0) return parsed.syntax;
	return schemaFindings(parsed, kind);
}

export function schemaFindings(parsed: Parsed, kind: DocumentKind): Diagnostic[] {
	const validate = validatorFor(kind);
	if (validate(parsed.value)) return [];
	const errors = validate.errors ?? [];
	const conditions = errors.filter((e) => e.keyword === 'if');
	const out: Diagnostic[] = [];
	const seen = new Set<string>();
	for (const error of shape(errors)) {
		const d = describe(error, SCHEMAS[kind], parsed, conditions);
		const key = `${d.from}:${d.to}:${d.message}`;
		if (seen.has(key)) continue;
		seen.add(key);
		out.push(d);
	}
	return out.sort((a, b) => a.from - b.from || a.to - b.to);
}

/**
 * The errors worth a sentence. An `if` error only says its `then` failed (the
 * `then`'s own errors are reported); a `propertyNames` error repeats the inner
 * error that names the property; and an `anyOf` is said once, from its
 * branches, instead of once per failed branch. Ajv reports a failed branch's
 * errors before the anyOf's own, at the same node or inside it; a branch
 * reached through $ref carries the definition's path, not the anyOf's, so the
 * node and the order are what tie them together.
 */
function shape(errors: ErrorObject[]): ErrorObject[] {
	const dropped = new Set<number>();
	errors.forEach((a, k) => {
		if (a.keyword !== 'anyOf') return;
		for (let i = 0; i < k; i++) {
			const e = errors[i];
			if (e.keyword === 'anyOf' || e.keyword === 'if') continue;
			if (e.instancePath === a.instancePath || e.instancePath.startsWith(`${a.instancePath}/`)) dropped.add(i);
		}
	});
	return errors.filter((e, i) => !dropped.has(i) && e.keyword !== 'if' && e.keyword !== 'propertyNames');
}

const ARTICLE: Record<string, string> = {
	integer: 'an integer',
	number: 'a number',
	string: 'a string',
	boolean: 'true or false',
	object: 'a mapping',
	array: 'a list',
	null: 'null'
};

/** What a value is, said the way a person would: `the string "soon"`, `a list`. */
function actual(data: unknown): string {
	if (data === null) return 'nothing (null)';
	if (Array.isArray(data)) return 'a list';
	switch (typeof data) {
		case 'string':
			return `the string ${JSON.stringify(data)}`;
		case 'number':
			return `the number ${data}`;
		case 'boolean':
			return `the boolean ${data}`;
		case 'object':
			return 'a mapping';
	}
	return String(data);
}

const code = (s: unknown) => `\`${typeof s === 'string' ? s : JSON.stringify(s)}\``;

function list(words: unknown[]): string {
	const items = words.map((w) => (typeof w === 'string' ? w : JSON.stringify(w)));
	if (items.length <= 1) return items.join('');
	return `${items.slice(0, -1).join(', ')} or ${items[items.length - 1]}`;
}

/** The word in `candidates` closest to `word`, if it is close enough to be a typo. */
export function suggest(word: string, candidates: readonly string[]): string | null {
	let best: string | null = null;
	let bestScore = Infinity;
	for (const c of candidates) {
		if (c === word) continue;
		const d = distance(word.toLowerCase(), c.toLowerCase());
		const limit = Math.max(1, Math.floor(Math.max(word.length, c.length) / 3));
		if (d <= limit && d < bestScore) {
			best = c;
			bestScore = d;
		}
	}
	return best;
}

function distance(a: string, b: string): number {
	const row = Array.from({ length: b.length + 1 }, (_, i) => i);
	for (let i = 1; i <= a.length; i++) {
		let prev = row[0];
		row[0] = i;
		for (let j = 1; j <= b.length; j++) {
			const cur = row[j];
			row[j] = Math.min(row[j] + 1, row[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1));
			prev = cur;
		}
	}
	return row[b.length];
}

/** Whether a schema is the generator's template form: a string holding `{{`. */
export function isTemplateSchema(schema: unknown): boolean {
	if (!schema || typeof schema !== 'object') return false;
	const s = schema as Record<string, unknown>;
	return s.type === 'string' && typeof s.pattern === 'string' && new RegExp(s.pattern).test('{{ .inputs.x }}');
}

/** A branch of an anyOf, in words. */
function branchWords(branch: unknown, root: Record<string, unknown>): string {
	if (!branch || typeof branch !== 'object') return 'a value';
	const b = branch as Record<string, unknown>;
	if (typeof b.$ref === 'string') {
		const target = schemaAt(root, b.$ref);
		return isTemplateSchema(target) ? 'a template ({{ … }})' : branchWords(target, root);
	}
	if (Array.isArray(b.enum)) return `one of ${list(b.enum)}`;
	if ('const' in b) return code(b.const);
	if (typeof b.type === 'string') return ARTICLE[b.type] ?? b.type;
	if (Array.isArray(b.type)) return list(b.type.map((t) => ARTICLE[String(t)] ?? String(t)));
	return 'a value';
}

/**
 * The condition of an `if`, for the `then` it selected or, negated, the
 * `else`: "when `uses` starts with orkflow:", "unless `await` is set".
 */
function condition(ifSchema: unknown, negated: boolean): string {
	if (!ifSchema || typeof ifSchema !== 'object') return '';
	const s = ifSchema as Record<string, unknown>;
	const parts: string[] = [];
	const props = (s.properties ?? {}) as Record<string, unknown>;
	const lead = negated ? 'unless' : 'when';
	for (const key of Array.isArray(s.required) ? (s.required as string[]) : []) {
		const p = props[key];
		if (p && typeof p === 'object' && 'const' in p) {
			parts.push(`${lead} \`${key}\` is ${code((p as Record<string, unknown>).const)}`);
			continue;
		}
		const pattern = p && typeof p === 'object' ? (p as Record<string, unknown>).pattern : undefined;
		if (typeof pattern === 'string' && /^\^[\w:./-]+$/.test(pattern)) {
			parts.push(`${lead} \`${key}\` starts with ${pattern.slice(1)}`);
			continue;
		}
		parts.push(`${lead} \`${key}\` is set`);
	}
	const not = s.not as Record<string, unknown> | undefined;
	if (not && Array.isArray(not.required)) {
		const keys = list((not.required as string[]).map((k) => `\`${k}\``));
		parts.push(negated ? `when ${keys} is set` : `when ${keys} is not set`);
	}
	return parts.join(' and ');
}

/**
 * The condition that put an error's schema in force, if an if/then/else did:
 * the `if` error ajv reports beside it, on the same node or an ancestor, whose
 * `then` or `else` the error's schema path runs through.
 */
function conditionOf(error: ErrorObject, conditions: ErrorObject[]): string {
	let best: ErrorObject | null = null;
	let negated = false;
	for (const c of conditions) {
		const base = c.schemaPath.replace(/\/if$/, '');
		if (!(error.instancePath === c.instancePath || error.instancePath.startsWith(`${c.instancePath}/`))) continue;
		const then = error.schemaPath.startsWith(`${base}/then/`);
		const otherwise = error.schemaPath.startsWith(`${base}/else/`);
		if (!then && !otherwise) continue;
		if (!best || c.instancePath.length > best.instancePath.length) {
			best = c;
			negated = otherwise;
		}
	}
	if (!best) return '';
	return condition((best.parentSchema as Record<string, unknown> | undefined)?.if, negated);
}

function multiline(parsed: Parsed, range: Range): boolean {
	return parsed.text.slice(range.from, range.to).includes('\n');
}

function finding(parsed: Parsed, range: Range | null, path: readonly PathSegment[], message: string): Diagnostic {
	const r = range ?? headAt(parsed.doc, []) ?? { from: 0, to: 0 };
	return { from: r.from, to: r.to, severity: 'error', source: 'schema', message, path: displayPath(path) };
}

function describe(error: ErrorObject, root: Record<string, unknown>, parsed: Parsed, conditions: ErrorObject[]): Diagnostic {
	const path = pointerPath(error.instancePath);
	const doc = parsed.doc;
	const params = error.params as Record<string, unknown>;
	const where = conditionOf(error, conditions);
	// propertyNames: the inner error names the offending key.
	const propertyName = (error as ErrorObject & { propertyName?: string }).propertyName;
	if (propertyName !== undefined) {
		return finding(parsed, rangeAt(doc, [...path, propertyName], 'key'), path, `the name ${code(propertyName)} ${sentence(error, root)}`);
	}
	switch (error.keyword) {
		case 'additionalProperties': {
			const key = String(params.additionalProperty);
			const known = Object.keys(((error.parentSchema as Record<string, unknown> | undefined)?.properties ?? {}) as object);
			const near = suggest(key, known);
			return finding(parsed, rangeAt(doc, [...path, key], 'key'), path, `unknown key ${code(key)}${near ? `; did you mean ${code(near)}?` : ''}`);
		}
		case 'false schema': {
			const key = String(path[path.length - 1]);
			return finding(parsed, rangeAt(doc, path, 'key'), path.slice(0, -1), `${code(key)} cannot be set ${where || 'here'}`);
		}
		case 'required': {
			const key = String(params.missingProperty);
			return finding(parsed, headAt(doc, path), path, `missing the required key ${code(key)}${where ? ` (${where})` : ''}`);
		}
	}
	// A value-level error goes on the value, unless the value is a block
	// mapping or list, whose underline would say nothing about where; then
	// its key.
	const value = rangeAt(doc, path, 'value');
	const key = rangeAt(doc, path, 'key');
	const range = value && key && multiline(parsed, value) ? key : (value ?? key);
	// The path is shown beside every finding (the list, the hover), so the
	// sentence starts at the verb; only the document itself is named.
	return finding(parsed, range, path, path.length > 0 ? sentence(error, root) : `the document ${sentence(error, root)}`);
}

/** The sentence for a value-level error, after the key it is about. */
function sentence(error: ErrorObject, root: Record<string, unknown>): string {
	const params = error.params as Record<string, unknown>;
	switch (error.keyword) {
		case 'type': {
			const types = String(params.type).split(',');
			return `must be ${list(types.map((t) => ARTICLE[t] ?? t))}, not ${actual(error.data)}`;
		}
		case 'enum': {
			const words = (params.allowedValues as unknown[]) ?? [];
			const near = typeof error.data === 'string' ? suggest(error.data, words.map(String)) : null;
			return `must be one of ${list(words)}${near ? `; did you mean ${code(near)}?` : ''}`;
		}
		case 'const':
			return `must be ${code(params.allowedValue)}`;
		case 'pattern':
			return `must match ${code(params.pattern)}`;
		case 'maxLength':
			return `must be at most ${params.limit} characters`;
		case 'minLength':
			return `must be at least ${params.limit} characters`;
		case 'minItems':
			return `needs at least ${params.limit} item${params.limit === 1 ? '' : 's'}`;
		case 'minimum':
			return `must be at least ${params.limit}`;
		case 'format':
			return params.format === 'date-time'
				? 'must be an RFC 3339 instant, such as 2026-01-02T03:04:05Z'
				: `must be a ${params.format}`;
		case 'not': {
			const not = error.schema as Record<string, unknown> | undefined;
			return not && Array.isArray(not.enum) ? `must not be ${list(not.enum)}` : 'is not allowed here';
		}
		case 'anyOf': {
			const branches = Array.isArray(error.schema) ? error.schema : [];
			return `must be ${list(branches.map((b) => branchWords(b, root)))}`;
		}
	}
	return error.message ?? 'is not valid here';
}
