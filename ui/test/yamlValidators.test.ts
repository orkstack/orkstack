import { expect, test } from 'bun:test';
import Ajv, { type ValidateFunction } from 'ajv';
import addFormats from 'ajv-formats';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { CONDITIONS } from '../src/lib/yaml/conditions.gen';
import { Automation, Orkflow } from '../src/lib/yaml/validators.gen';
import { CONDITION_OPTIONS, FILES, OUTPUTS, VALIDATOR_OPTIONS, conditionsIn, generate } from './gen-validators';

// The editor's validators are compiled ahead of time so the page can refuse
// eval (test/gen-validators.ts): the checked-in code must be what the
// schemas compile to today, it must answer exactly as a run-time compile
// would, and nothing in ui/src may compile one at run time again.

const ui = `${import.meta.dir}/..`;
const repo = `${ui}/..`;
const docsSchema = (file: string) => readFileSync(`${repo}/docs/schemas/${file}`, 'utf8');
const sources = { Orkflow: docsSchema(FILES.Orkflow), Automation: docsSchema(FILES.Automation) };
const REGENERATE = 'stale: run `cd ui && bun run gen:validators`';

test('the checked-in validators are what docs/schemas/ compiles to, byte for byte', () => {
	for (const [path, code] of Object.entries(generate(sources))) {
		const same = readFileSync(`${ui}/${path}`, 'utf8') === code;
		expect(`${path}: ${same ? 'current' : REGENERATE}`).toBe(`${path}: current`);
	}
});

test('a changed schema is seen: it compiles to other code', () => {
	// The check above can only refuse what the generator tells apart.
	const schema = JSON.parse(sources.Orkflow);
	const key = Object.keys(schema.definitions['orkflow.task'].properties)[0];
	schema.definitions['orkflow.task'].properties[`${key}_renamed`] = schema.definitions['orkflow.task'].properties[key];
	delete schema.definitions['orkflow.task'].properties[key];
	const changed = generate({ ...sources, Orkflow: JSON.stringify(schema) });
	const current = generate(sources);
	for (const path of Object.values(OUTPUTS)) expect(changed[path] === current[path]).toBe(false);
	// A byte that compiles the same still moves the hash TestSchemas_ValidatorsAreCurrent reads.
	const reformatted = generate({ ...sources, Orkflow: `${sources.Orkflow}\n` });
	expect(reformatted[OUTPUTS.validators] === current[OUTPUTS.validators]).toBe(false);
});

test('every `if` the walker can meet is compiled, and nothing else is', () => {
	const keys = new Set<string>();
	for (const source of Object.values(sources)) for (const condition of conditionsIn(JSON.parse(source))) keys.add(JSON.stringify(condition));
	expect(keys.size > 0).toBe(true);
	expect(Object.keys(CONDITIONS).sort()).toEqual([...keys].sort());
	// A task's `if`: a mapping under a key, which is a schema.
	expect(conditionsIn({ allOf: [{ if: { required: ['a'] }, then: {} }] })).toEqual([{ required: ['a'] }]);
	// A property named `if` and an example holding one are not conditions.
	expect(conditionsIn({ properties: { if: { type: 'string' } }, examples: [{ if: { required: ['a'] } }] })).toEqual([]);
});

test('the generated code evaluates nothing', () => {
	for (const path of Object.values(OUTPUTS)) {
		const code = readFileSync(`${ui}/${path}`, 'utf8');
		expect(`${path}: ${/\bnew Function\b|\beval\s*\(|\brequire\s*\(/.test(code)}`).toBe(`${path}: false`);
	}
});

test('nothing in ui/src compiles a validator at run time', () => {
	const files = readdirSync(`${ui}/src`, { recursive: true }).filter(
		(f) => /\.(?:ts|svelte|js)$/.test(f) && statSync(`${ui}/src/${f}`).isFile() && !Object.values(OUTPUTS).includes(`src/${f}` as never)
	);
	expect(files.length > 100).toBe(true);
	const importers = files.filter((f) => /^\s*import\s+(?!type\b)[^;]*from\s+['"]ajv(?:-formats)?(?:\/[^'"]*)?['"]/m.test(readFileSync(`${ui}/src/${f}`, 'utf8')));
	expect(importers).toEqual([]);
});

// The same schema and the same options, compiled here the way the editor used
// to in the browser.
function compiled(source: string): ValidateFunction {
	const ajv = new Ajv(VALIDATOR_OPTIONS);
	addFormats(ajv, ['date-time']);
	return ajv.compile(JSON.parse(source));
}

const TASK = (body: string) => `{"apiVersion":"orkstack/v1","kind":"Orkflow","name":"x","version":"1","tasks":[${body}]}`;
const DOCUMENTS: Record<'Orkflow' | 'Automation', unknown[]> = {
	Orkflow: [
		{},
		null,
		[],
		'a string',
		JSON.parse(TASK('{"id":"a","uses":"shell","with":{"op":"exec","command":["true"]}}')),
		JSON.parse(TASK('{"id":"a","need":["b"],"uses":"shell"}')),
		JSON.parse(TASK('{"id":"a","uses":"orkflow:b@1","with":{"x":1},"command":["no"]}')),
		JSON.parse(TASK('{"id":"Bad Name","uses":"http","with":{"url":5,"timeout":"soon"}}')),
		JSON.parse(TASK('{"id":"a","await":{"timeout_seconds":"never","callback":"yes"},"uses":"shell"}')),
		JSON.parse(TASK('{"id":"a","uses":"shell","join":"sometimes","retry":{"maxAttempts":-1}}')),
		JSON.parse(TASK(`{"id":"${'x'.repeat(201)}","uses":"shell"}`)),
		{ apiVersion: 'orkstack/v2', kind: 'Orkflow', name: 'x', version: 1, tasks: 'none' }
	],
	Automation: [
		{},
		null,
		{ apiVersion: 'orkstack/v1', kind: 'Automation', namespace: 'n', schedules: { s: { orkflow: 'x', overlap: 'buffer-one', start_at: 'yesterday' } } },
		{ apiVersion: 'orkstack/v1', kind: 'Automation', namespace: 'n', schedules: { s: { orkflow: 'x', start_at: '2026-01-02T03:04:05Z' } }, nope: true }
	]
};
const shipped = readdirSync(`${repo}/examples`).filter((f) => f.endsWith('.ork.yaml')).sort();

test('a compiled validator answers exactly as a run-time compile, errors and all', () => {
	const documents = { ...DOCUMENTS, Orkflow: [...DOCUMENTS.Orkflow, ...shipped.map((f) => Bun.YAML.parse(readFileSync(`${repo}/examples/${f}`, 'utf8')))] };
	const pairs = [
		['Orkflow', Orkflow as unknown as ValidateFunction, compiled(sources.Orkflow)],
		['Automation', Automation as unknown as ValidateFunction, compiled(sources.Automation)]
	] as const;
	let failing = 0;
	for (const [kind, ahead, runtime] of pairs) {
		for (const doc of documents[kind]) {
			const answer = ahead(doc);
			expect(answer).toBe(runtime(doc));
			if (!answer) failing++;
			expect(ahead.errors ?? null).toEqual(runtime.errors ?? null);
		}
	}
	// The table holds refusals, so the errors above were really compared.
	expect(failing > 5).toBe(true);
	// `date-time` reached its format: only that makes this one a refusal.
	const instant = (start_at: string) => ({ apiVersion: 'orkstack/v1', kind: 'Automation', namespace: 'n', schedules: { s: { orkflow: 'x', start_at } } });
	const validate = Automation as unknown as ValidateFunction;
	expect(validate(instant('yesterday'))).toBe(false);
	expect(validate.errors?.some((e) => e.keyword === 'format')).toBe(true);
});

test('a compiled condition answers exactly as a run-time compile', () => {
	const ajv = new Ajv(CONDITION_OPTIONS);
	const data: unknown[] = [undefined, null, {}, [], 'x', { uses: 'orkflow:b@1' }, { uses: 'http' }, { uses: 'shell' }, { op: 'exec' }, { op: 'script' }, { await: {} }, { each: {} }, { cache: { enabled: true } }];
	for (const [key, ahead] of Object.entries(CONDITIONS)) {
		const runtime = ajv.compile(JSON.parse(key));
		for (const value of data) expect(`${key} on ${JSON.stringify(value)}: ${ahead(value)}`).toBe(`${key} on ${JSON.stringify(value)}: ${runtime(value)}`);
	}
});
