import { expect, test } from 'bun:test';
import { engineFindings } from '../src/lib/yaml/server';
import { editorStatus, localFindings, type EngineState } from '../src/lib/yaml/status';
import { lineCol } from '../src/lib/yaml/document';
import { lintDocument } from '../src/lib/yaml/lint';
import { ValidationSeverity } from '../src/lib/gen/orkstack/v1/orkflow_service_pb';
import { API_VERSION } from '../src/lib/placeholders';

// The engine's answer, placed in the document, and the one sentence under the
// editor that says what is known about it. The rule these tests hold: the
// editor says a document is accepted only when the engine said so, about
// exactly the text and namespace on screen.

const HEADER = `apiVersion: ${API_VERSION}\nkind: Orkflow\n`;
const SRC = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n    needs: [b]\n    with:\n      flag: yes\n';
const issue = (field: string, message: string, severity = ValidationSeverity.ERROR) => ({ field, message, severity });
const at = (text: string, from: number) => {
	const { line, col } = lineCol(text, from);
	return `${line}:${col}`;
};

test('an engine issue lands on the node its field names', () => {
	const [need] = engineFindings(SRC, [issue('tasks[0].needs[0]', 'tasks[0] needs b, which is not a task')]);
	expect(at(SRC, need.from)).toBe('7:13');
	expect(SRC.slice(need.from, need.to)).toBe('b');
	expect(need.source).toBe('engine');
	expect(need.severity).toBe('error');
	expect(need.path).toBe('tasks[0].needs[0]');
});

test('a strict-mapping refusal keeps its sentence and lands on its line', () => {
	const message = 'line 9: yes is a boolean to YAML 1.1 readers and a string to YAML 1.2 readers; quote it';
	const [d] = engineFindings(SRC, [issue('tasks[0].with.flag', message)]);
	expect(at(SRC, d.from)).toBe('9:13');
	expect(d.message.startsWith('yes is a boolean')).toBe(true);
});

test('a decode error with no field is placed on the line yaml.v3 names inside its sentence', () => {
	const message = 'parse yaml: yaml: unmarshal errors:\n  line 7: field need not found in type dsl.task';
	const [d] = engineFindings(SRC, [issue('', message)]);
	expect(lineCol(SRC, d.from).line).toBe(7);
	expect(d.message).toBe(message);
	const [elsewhere] = engineFindings(SRC, [issue('', 'the key "a" is written twice (first at line 3)')]);
	expect(lineCol(SRC, elsewhere.from).line).toBe(1);
});

test('the line an issue names wins when its field does not resolve to that line', () => {
	const [d] = engineFindings(SRC, [issue('tasks[0].nowhere', 'line 8: something about with')]);
	expect(lineCol(SRC, d.from).line).toBe(8);
	expect(SRC.slice(d.from, d.to)).toBe('with:');
});

test('a field that does not resolve falls back to its nearest written ancestor', () => {
	const [d] = engineFindings(SRC, [issue('tasks[0].with.headers.x', 'headers: unknown')]);
	expect(SRC.slice(d.from, d.to)).toBe('with');
});

test('an issue about the document, and a warning, are placed and kept as they are', () => {
	const [doc, warn] = engineFindings(SRC, [
		issue('', 'the document has a cycle'),
		issue('tasks[0]', 'pin advice', ValidationSeverity.WARNING)
	]);
	expect(at(SRC, doc.from)).toBe('1:1');
	expect(warn.severity).toBe('warning');
	expect(at(SRC, warn.from)).toBe('5:5');
});

test('engine findings survive a document that no longer parses', () => {
	const broken = SRC + '  - [\n';
	const [d] = engineFindings(broken, [issue('tasks[0].needs[0]', 'x')]);
	expect(d.from <= broken.length).toBe(true);
});

const local = (text: string) => lintDocument(text, 'Orkflow');
const ns = 'default';

test('an empty editor is neither valid nor in error', () => {
	const s = editorStatus({ text: '', namespace: ns, local: [], engine: { kind: 'idle' } });
	expect(s.tone).toBe('empty');
});

test('a document the engine found no problem in, exactly as shown, reads as clean', () => {
	const text = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n';
	const engine: EngineState = { kind: 'answered', text, namespace: ns, issues: [] };
	const s = editorStatus({ text, namespace: ns, local: local(text), engine });
	expect(s.tone).toBe('clean');
	expect(s.errors).toBe(0);
	expect(s.sentence).toBe('The engine finds no problems in this document.');
	const noted = editorStatus({ text, namespace: ns, local: [], engine, cleanNote: 'Apply also checks stored versions.' });
	expect(noted.sentence).toBe('The engine finds no problems in this document. Apply also checks stored versions.');
});

test('an answer about other text, another namespace, a pending check or a failed one is never "accepted"', () => {
	const text = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n';
	const cases: EngineState[] = [
		{ kind: 'answered', text: text + '# edited\n', namespace: ns, issues: [] },
		{ kind: 'answered', text, namespace: 'other', issues: [] },
		{ kind: 'pending', text, namespace: ns },
		{ kind: 'failed', text, namespace: ns, message: 'unavailable: engine down' },
		{ kind: 'idle' }
	];
	for (const engine of cases) {
		const s = editorStatus({ text, namespace: ns, local: local(text), engine });
		expect(`${engine.kind}: ${s.tone}`).not.toBe(`${engine.kind}: clean`);
	}
	const failed = editorStatus({ text, namespace: ns, local: [], engine: cases[3] });
	expect(failed.tone).toBe('unavailable');
	expect(failed.sentence).toContain('engine down');
});

test('an engine error is a problem; a schema finding the engine does not share becomes a warning', () => {
	const text = HEADER + 'name: x\nversion: 2\ntasks:\n  - id: a\n    uses: shell\n';
	const accepted: EngineState = { kind: 'answered', text, namespace: ns, issues: [] };
	const before = editorStatus({ text, namespace: ns, local: local(text), engine: { kind: 'pending', text, namespace: ns } });
	expect(before.errors).toBe(1);
	const stricter = editorStatus({ text, namespace: ns, local: local(text), engine: accepted });
	expect(stricter.tone).toBe('clean');
	expect(stricter.errors).toBe(0);
	expect(stricter.warnings).toBe(1);
	expect(stricter.sentence).toContain('schema check');
	expect(localFindings(local(text), accepted, text, ns).map((d) => d.severity)).toEqual(['warning']);
	expect(localFindings(local(text), { kind: 'pending', text, namespace: ns }, text, ns).map((d) => d.severity)).toEqual(['error']);
	const refused: EngineState = { kind: 'answered', text, namespace: ns, issues: [issue('tasks[0].needs[0]', 'x')] };
	const s = editorStatus({ text, namespace: ns, local: [], engine: refused });
	expect(s.tone).toBe('problems');
	expect(s.errors).toBe(1);
});

test('warnings alone leave a document accepted, and are counted', () => {
	const text = HEADER + 'name: x\ntasks:\n  - id: a\n    uses: shell\n';
	const engine: EngineState = { kind: 'answered', text, namespace: ns, issues: [issue('pins.p', 'advice', ValidationSeverity.WARNING)] };
	const s = editorStatus({ text, namespace: ns, local: [], engine });
	expect(s.tone).toBe('clean');
	expect(s.warnings).toBe(1);
});

test('a syntax error says the engine waits for a document that parses', () => {
	const text = HEADER + 'tasks: [\n';
	const s = editorStatus({ text, namespace: ns, local: local(text), engine: { kind: 'idle' } });
	expect(s.tone).toBe('problems');
	expect(s.sentence).toContain('parses');
});
