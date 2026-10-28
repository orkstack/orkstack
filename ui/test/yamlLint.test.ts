import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { lintDocument } from '../src/lib/yaml/lint';
import { lineCol, parseYaml, rangeAt, parseFieldPath } from '../src/lib/yaml/document';
import type { Diagnostic } from '../src/lib/yaml/types';
import { API_VERSION, ORKFLOW_PLACEHOLDER, automationPlaceholder } from '../src/lib/placeholders';

// The editor's own findings: YAML syntax and the generated schemas
// (docs/schemas/), each placed on the line and column a person would point
// at. Every case names the position it expects, so a finding that drifts to
// the document's start, or to the wrong key, fails here first.

const repo = `${import.meta.dir}/../..`;
const read = (path: string) => readFileSync(`${repo}/${path}`, 'utf8');
const HEADER = `apiVersion: ${API_VERSION}\nkind: Orkflow\n`;
const AUTOMATION = `apiVersion: ${API_VERSION}\nkind: Automation\nnamespace: n\n`;

/** "line:col" (1-based) of where a diagnostic starts, its source and message. */
function where(text: string, d: Diagnostic): string {
	const { line, col } = lineCol(text, d.from);
	return `${line}:${col} ${d.source}`;
}
const text = (text: string, d: Diagnostic) => text.slice(d.from, d.to);

test('lineCol counts lines and columns from 1', () => {
	expect(lineCol('ab\ncd', 0)).toEqual({ line: 1, col: 1 });
	expect(lineCol('ab\ncd', 4)).toEqual({ line: 2, col: 2 });
	expect(lineCol('ab\ncd', 99)).toEqual({ line: 2, col: 3 });
});

test('parseFieldPath reads the engine’s dotted and indexed field paths', () => {
	expect(parseFieldPath('tasks[2].with.body')).toEqual(['tasks', 2, 'with', 'body']);
	expect(parseFieldPath('tasks[0].needs[1]')).toEqual(['tasks', 0, 'needs', 1]);
	expect(parseFieldPath('pins.etl')).toEqual(['pins', 'etl']);
	expect(parseFieldPath('')).toEqual([]);
	expect(parseFieldPath('values.x')).toEqual(['values', 'x']);
});

test('rangeAt finds the key of a mapping entry and the item of a sequence', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    needs: [b, c]\n';
	const { doc } = parseYaml(src);
	const uses = rangeAt(doc, ['tasks', 0, 'uses'], 'key')!;
	expect(src.slice(uses.from, uses.to)).toBe('uses');
	const value = rangeAt(doc, ['tasks', 0, 'uses'], 'value')!;
	expect(src.slice(value.from, value.to)).toBe('shell');
	const need = rangeAt(doc, ['tasks', 0, 'needs', 1], 'value')!;
	expect(src.slice(need.from, need.to)).toBe('c');
	expect(rangeAt(doc, ['tasks', 3, 'uses'], 'key')).toBeNull();
});

test('a YAML syntax error is placed where the parser stopped, and nothing else is reported', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n   needs: [b\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.length > 0).toBe(true);
	expect(ds.every((d) => d.source === 'syntax' && d.severity === 'error')).toBe(true);
	expect(lineCol(src, ds[0].from).line >= 7).toBe(true);
});

test('a key written twice is a syntax error at its second spelling', () => {
	const src = HEADER + 'name: x\nname: y\ntasks:\n  - id: a\n    uses: shell\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['4:1 syntax']);
});

test('an unknown key is flagged on its own line, with the key it was probably meant to be', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    need: [b]\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['7:5 schema']);
	expect(text(src, ds[0])).toBe('need');
	expect(ds[0].message).toContain('need');
	expect(ds[0].message).toContain('needs');
	expect(ds[0].path).toBe('tasks[0]');
});

test('a missing required key is placed on the mapping that lacks it', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    needs: []\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['5:5 schema']);
	expect(ds[0].message).toContain('uses');
});

test('a wrong type, a word the key does not take and a bad slug name their key and line', () => {
	const src = HEADER + 'name: Bad_Name\ntasks:\n  - id: a\n    uses: shell\n    timeout_seconds: soon\n    join: any_failure\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['3:7 schema', '7:22 schema', '8:11 schema']);
	expect(ds[0].message).toContain('^[a-z]');
	expect(ds[1].message).toContain('integer');
	expect(ds[2].message).toContain('any_failed');
});

test('a key a task kind forbids says which kind forbids it', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    await:\n      timeout_seconds: 5\n    command: [x]\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['8:5 schema']);
	expect(ds[0].message).toContain('command');
	expect(ds[0].message).toContain('await');
});

test('a first-party with: word reads as one finding listing the words and the template form', () => {
	const src = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: http\n    with:\n      url: https://example.com\n      op: fetch\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => where(src, d))).toEqual(['9:11 schema']);
	expect(ds[0].message).toContain('post');
	expect(ds[0].message).toContain('{{');
});

test('an automation document is held to its own schema', () => {
	const src = AUTOMATION + 'schedules:\n  nightly:\n    orkflow: etl\n    cron: "@daily"\n    overlap: sometimes\n';
	const ds = lintDocument(src, 'Automation');
	expect(ds.map((d) => where(src, d))).toEqual(['8:14 schema']);
	expect(ds[0].message).toContain('skip');
});

test('a document of the other kind is refused at its kind:', () => {
	const src = AUTOMATION;
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.some((d) => where(src, d) === '2:7 schema' && d.message.includes('Orkflow'))).toBe(true);
});

test('a document without its header is told so at the start', () => {
	const src = 'name: x\ntasks:\n  - id: a\n    uses: shell\n';
	const ds = lintDocument(src, 'Orkflow');
	expect(ds.map((d) => d.message).join(' ')).toContain('apiVersion');
	expect(ds.every((d) => d.source === 'schema')).toBe(true);
});

test('an empty document has no findings: the placeholder says what goes there', () => {
	expect(lintDocument('', 'Orkflow')).toEqual([]);
	expect(lintDocument('  \n\n', 'Automation')).toEqual([]);
});

test('every document this repository ships lints clean', () => {
	let count = 0;
	const clean = (label: string, src: string, kind: 'Orkflow' | 'Automation') => {
		const ds = lintDocument(src, kind);
		expect(ds.map((d) => `${label} ${where(src, d)} ${d.message}`)).toEqual([]);
		count++;
	};
	for (const f of readdirSync(`${repo}/examples`).filter((f) => f.endsWith('.ork.yaml')))
		clean(`examples/${f}`, read(`examples/${f}`), 'Orkflow');
	for (const f of readdirSync(`${repo}/examples/automation`).filter((f) => f.endsWith('.ork.yaml')))
		clean(`examples/automation/${f}`, read(`examples/automation/${f}`), 'Automation');
	for (const f of readdirSync(`${repo}/internal/dsl/testdata/schema-vectors`).filter((f) => f.endsWith('.ork.yaml')))
		clean(f, read(`internal/dsl/testdata/schema-vectors/${f}`), 'Orkflow');
	clean('ORKFLOW_PLACEHOLDER', ORKFLOW_PLACEHOLDER, 'Orkflow');
	clean('automationPlaceholder', automationPlaceholder('default'), 'Automation');
	expect(count > 25).toBe(true);
});

test('every refusal of the schema table is reported on the node the schema names', () => {
	// The same refusals ui/test/authoredSchemas.test.ts holds the schemas to,
	// now through the editor: the finding must sit on the node at that path.
	const cases: [string, string, 'Orkflow' | 'Automation', (string | number)[]][] = [
		['a misspelled need:', HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    need: [b]\n', 'Orkflow', ['tasks', 0, 'need']],
		['network: host', HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    network: host\n', 'Orkflow', ['tasks', 0, 'network']],
		['env on a composed task', HEADER + 'name: x\ntasks:\n  - id: a\n    uses: orkflow:child@1\n    env: {A: b}\n', 'Orkflow', ['tasks', 0, 'env']],
		['tasks: []', HEADER + 'name: x\ntasks: []\n', 'Orkflow', ['tasks']],
		['version: 2 (V)', HEADER + 'name: x\nversion: 2\ntasks:\n  - id: a\n    uses: busybox:1\n', 'Orkflow', ['version']],
		['timeout_seconds: 1.5 (I)', HEADER + 'name: x\ntasks:\n  - id: a\n    uses: busybox:1\n    timeout_seconds: 1.5\n', 'Orkflow', ['tasks', 0, 'timeout_seconds']],
		['a secret named ..', HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    secrets: [{name: "..", source: "env:X"}]\n', 'Orkflow', ['tasks', 0, 'secrets', 0, 'name']],
		['start_at: tomorrow', AUTOMATION + 'schedules:\n  s: {orkflow: x, cron: "@daily", start_at: tomorrow}\n', 'Automation', ['schedules', 's', 'start_at']],
		['a trigger description 12', AUTOMATION + 'triggers:\n  t: {orkflow: x, description: 12}\n', 'Automation', ['triggers', 't', 'description']]
	];
	for (const [label, src, kind, path] of cases) {
		const { doc } = parseYaml(src);
		const key = rangeAt(doc, path, 'key') ?? rangeAt(doc, path, 'value');
		expect(`${label}: ${key !== null}`).toBe(`${label}: true`);
		const lines = lintDocument(src, kind).map((d) => lineCol(src, d.from).line);
		expect(`${label}: ${lines.includes(lineCol(src, key!.from).line)}`).toBe(`${label}: true`);
	}
});
