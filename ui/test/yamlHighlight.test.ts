import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { yamlSegments, diffSegments, SYNTAX_ROLES } from '../src/lib/yaml/highlight';

// One classifier colours YAML everywhere the UI shows it: the editor, the
// automation export and the version diff. These tests hold its roles and its
// one promise to read-only views: the text it returns is the text it was given,
// byte for byte, so colouring can never change what a person copies.

const roles = (text: string) =>
	yamlSegments(text)
		.filter((s) => s.role)
		.map((s) => `${s.role}:${s.text}`);

test('keys, strings, numbers, literals, comments and punctuation take their roles', () => {
	const out = roles('name: nightly # the name\ntimeout_seconds: 300\nflag: true\nnone: ~\nq: "quoted"\n');
	expect(out).toContain('key:name');
	expect(out).toContain('string:nightly');
	expect(out).toContain('comment:# the name');
	expect(out).toContain('number:300');
	expect(out).toContain('literal:true');
	expect(out).toContain('literal:~');
	expect(out).toContain('string:"quoted"');
	expect(out).toContain('punctuation::');
});

test('a template inside a string is its own role, and the string around it keeps its own', () => {
	const out = roles('url: "https://x/{{ .inputs.day }}/y"\nplain: a{{ .tasks.f.values.n }}\n');
	expect(out).toContain('string:"https://x/');
	expect(out).toContain('template:{{ .inputs.day }}');
	expect(out).toContain('string:/y"');
	expect(out).toContain('template:{{ .tasks.f.values.n }}');
});

test('anchors, aliases, tags and block scalars', () => {
	const out = roles('base: &b\n  a: 1\nuse: *b\nt: !!str 1\nscript: |\n  echo {{ .inputs.x }}\n');
	expect(out).toContain('meta:&b');
	expect(out).toContain('meta:*b');
	expect(out).toContain('meta:!!str');
	expect(out).toContain('template:{{ .inputs.x }}');
	expect(out.some((r) => r.startsWith('string:') && r.includes('echo'))).toBe(true);
});

test('the segments are the text, byte for byte, for every shipped example', () => {
	const repo = `${import.meta.dir}/../..`;
	const files = [
		...readdirSync(`${repo}/examples`).filter((f) => f.endsWith('.ork.yaml')).map((f) => `examples/${f}`),
		...readdirSync(`${repo}/examples/automation`).filter((f) => f.endsWith('.ork.yaml')).map((f) => `examples/automation/${f}`)
	];
	expect(files.length > 20).toBe(true);
	for (const file of files) {
		const src = readFileSync(`${repo}/${file}`, 'utf8');
		expect(`${file}: ${yamlSegments(src).map((s) => s.text).join('') === src}`).toBe(`${file}: true`);
	}
	const odd = 'a: [1, {b: c}]\n? complex\n: v\n--- \n...\n%YAML 1.2\n\ttab: x\n';
	expect(yamlSegments(odd).map((s) => s.text).join('')).toBe(odd);
});

test('a diff keeps every line verbatim and colours each side as YAML', () => {
	const diff = [
		'--- deploy@1',
		'+++ deploy@2',
		'@@ -1,3 +1,3 @@',
		' tasks:',
		'   - id: build',
		'-    timeout_seconds: 60',
		'+    timeout_seconds: 120',
		'\\ No newline at end of file'
	].join('\n');
	const rows = diffSegments(diff);
	expect(rows.map((r) => r.segments.map((s) => s.text).join(''))).toEqual(diff.split('\n'));
	const added = rows[6].segments.filter((s) => s.role).map((s) => `${s.role}:${s.text}`);
	expect(added).toContain('key:timeout_seconds');
	expect(added).toContain('number:120');
	expect(rows[0].segments.every((s) => s.role === null)).toBe(true);
	expect(rows[7].segments.every((s) => s.role === null)).toBe(true);
});

test('every role has a token', () => {
	const tokens = readFileSync(`${import.meta.dir}/../src/tokens.css`, 'utf8');
	for (const role of SYNTAX_ROLES) expect(`${role}: ${tokens.includes(`--color-syntax-${role}:`)}`).toBe(`${role}: true`);
});

test('a CRLF document comes back from the editor byte for byte; an LF one stays LF', async () => {
	const { Text } = await import('@codemirror/state');
	const { separatorOf, documentText } = await import('../src/lib/yaml/editor');
	const crlf = 'apiVersion: orkstack/v1\r\nkind: Orkflow\r\nname: x\r\n';
	const lf = crlf.replace(/\r\n/g, '\n');
	// CodeMirror splits on any line break and counts each as one character.
	const doc = Text.of(crlf.split(/\r\n/));
	expect(separatorOf(crlf)).toBe('\r\n');
	expect(documentText(doc, separatorOf(crlf))).toBe(crlf);
	expect(separatorOf(lf)).toBe('\n');
	expect(documentText(Text.of(lf.split('\n')), separatorOf(lf))).toBe(lf);
});
