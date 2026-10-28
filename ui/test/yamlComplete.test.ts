import { expect, test } from 'bun:test';
import { contextAt, keyOptions, valueOptions, hoverDoc } from '../src/lib/yaml/complete';
import { SCHEMAS } from '../src/lib/yaml/schemas';
import { API_VERSION } from '../src/lib/placeholders';

// Completion and hover read the generated schema at the cursor's path. The
// path comes from the block structure of the text itself, so it holds while
// a key is half typed and the document does not parse.

const HEADER = `apiVersion: ${API_VERSION}\nkind: Orkflow\n`;
const cursor = (doc: string) => ({ text: doc.replace('|', ''), pos: doc.indexOf('|') });

test('the path of a key being typed inside a task', () => {
	const { text, pos } = cursor(HEADER + 'name: x\ntasks:\n  - id: a\n    uses: http\n  - id: b\n    ne|\n');
	expect(contextAt(text, pos)).toEqual({ path: ['tasks', 1], position: 'key', prefix: 'ne', from: pos - 2 });
});

test('the path of a value after its key, and of a key in a nested mapping', () => {
	const v = cursor(HEADER + 'name: x\ntasks:\n  - id: a\n    join: an|\n');
	expect(contextAt(v.text, v.pos)).toEqual({ path: ['tasks', 0, 'join'], position: 'value', prefix: 'an', from: v.pos - 2 });
	const w = cursor(HEADER + 'name: x\ntasks:\n  - id: a\n    uses: http\n    with:\n      url: x\n      |\n');
	expect(contextAt(w.text, w.pos)).toEqual({ path: ['tasks', 0, 'with'], position: 'key', prefix: '', from: w.pos });
});

test('a key on the item line itself, and a top-level key', () => {
	const item = cursor(HEADER + 'name: x\ntasks:\n  - id: a\n  - i|\n');
	expect(contextAt(item.text, item.pos)?.path).toEqual(['tasks', 1]);
	const top = cursor(HEADER + 'na|\n');
	expect(contextAt(top.text, top.pos)).toEqual({ path: [], position: 'key', prefix: 'na', from: top.pos - 2 });
});

test('no completion inside a comment or after a block scalar indicator', () => {
	const c = cursor(HEADER + '# a com|\n');
	expect(contextAt(c.text, c.pos)).toBeNull();
});

test('a task offers its keys, minus those written and those its kind forbids', () => {
	const plain = keyOptions(SCHEMAS.Orkflow, HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n', ['tasks', 0]);
	const labels = plain.map((o) => o.label);
	expect(labels).toContain('needs');
	expect(labels).toContain('with');
	expect(labels).not.toContain('id');
	expect(labels).not.toContain('uses');
	const awaited = keyOptions(SCHEMAS.Orkflow, HEADER + 'name: x\ntasks:\n  - id: a\n    await:\n      timeout_seconds: 5\n', ['tasks', 0]).map((o) => o.label);
	expect(awaited).toContain('needs');
	expect(awaited).not.toContain('command');
	expect(awaited).not.toContain('uses');
	expect((plain.find((o) => o.label === 'needs')?.info ?? '').length > 0).toBe(true);
});

test('a first-party image’s with: keys come from its interface label', () => {
	const labels = keyOptions(SCHEMAS.Orkflow, HEADER + 'name: x\ntasks:\n  - id: a\n    uses: http\n    with:\n      url: x\n', ['tasks', 0, 'with']).map((o) => o.label);
	expect(labels).toContain('op');
	expect(labels).toContain('headers');
	expect(labels).not.toContain('url');
});

test('a key that takes words offers them; a boolean offers true and false', () => {
	const join = valueOptions(SCHEMAS.Orkflow, HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    join:\n', ['tasks', 0, 'join']).map((o) => o.label);
	expect(join).toEqual(['all_done', 'all_succeeded', 'any_failed', 'none_failed']);
	const flag = valueOptions(SCHEMAS.Orkflow, HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n', ['tasks', 0, 'run_as_orker_user']).map((o) => o.label);
	expect(flag).toEqual(['true', 'false']);
	const kind = valueOptions(SCHEMAS.Orkflow, HEADER, ['kind']).map((o) => o.label);
	expect(kind).toEqual(['Orkflow']);
});

test('an automation document completes from its own schema', () => {
	const doc = `apiVersion: ${API_VERSION}\nkind: Automation\nnamespace: n\nschedules:\n  s:\n    orkflow: x\n`;
	const labels = keyOptions(SCHEMAS.Automation, doc, ['schedules', 's']).map((o) => o.label);
	expect(labels).toContain('cron');
	expect(labels).toContain('overlap');
	expect(valueOptions(SCHEMAS.Automation, doc, ['schedules', 's', 'overlap']).map((o) => o.label)).toContain('buffer-one');
});

test('hover says what a key is, from the schema’s own description', () => {
	const doc = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    join: any_failed\n';
	const join = hoverDoc(SCHEMAS.Orkflow, doc, ['tasks', 0, 'join']);
	expect(join?.title).toBe('join');
	expect(join?.description).toContain('Joins and failure handlers');
	expect(join?.allowed).toEqual(['all_done', 'all_succeeded', 'any_failed', 'none_failed']);
	expect(hoverDoc(SCHEMAS.Orkflow, doc, ['tasks', 0, 'nope'])).toBeNull();
});
