// The site's token tooling: the parser of ui/src/tokens.css, what it derives,
// and the drift check's detector, on planted text.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { readTokens, darkMediaCss, resolve, codeTheme } from '../scripts/tokens.mjs';
import { checkSources } from '../scripts/check-tokens.mjs';

const fixture = `/* comment */
:root {
	--font-sans: 'A Face', sans-serif;
	--color-fg: var(--color-ink);
	--color-running-fg: var(--color-blue);
}
:root,
:root[data-theme='light'] {
	color-scheme: light;
	--color-ink: #14213d;
	--color-blue: #2b59c3;
	--color-paper: #fbfbf8;
}
:root[data-theme='dark'] {
	color-scheme: dark;
	--color-ink: #e7e5dc;
	--color-blue: #86a4ec;
	--color-paper: #13161c;
}
@keyframes pulse {
	from { opacity: 1; }
	to { opacity: 0; }
}
`;

function tokensFrom(css) {
	const dir = mkdtempSync(path.join(tmpdir(), 'site-tokens-'));
	const file = path.join(dir, 'tokens.css');
	writeFileSync(file, css);
	return readTokens(file);
}

test('the parser reads the three blocks and the keyframes', () => {
	const t = tokensFrom(fixture);
	assert.equal(t.light.get('--color-ink'), '#14213d');
	assert.equal(t.dark.get('--color-ink'), '#e7e5dc');
	assert.equal(t.invariant.get('--color-fg'), 'var(--color-ink)');
	assert.ok(t.keyframes.has('pulse'));
	assert.ok(t.names.has('--font-sans') && t.names.has('--color-paper'));
});

test('an alias resolves to the theme’s own value', () => {
	const t = tokensFrom(fixture);
	assert.equal(resolve(t, '--color-running-fg', 'light'), '#2b59c3');
	assert.equal(resolve(t, '--color-running-fg', 'dark'), '#86a4ec');
	assert.throws(() => resolve(t, '--color-missing', 'light'), /has no --color-missing/);
});

test('the no-JS dark block repeats the dark values for a root with no data-theme', () => {
	const css = darkMediaCss(tokensFrom(fixture));
	assert.match(css, /@media \(prefers-color-scheme: dark\)/);
	assert.match(css, /:root:not\(\[data-theme\]\)/);
	assert.match(css, /--color-ink: #e7e5dc;/);
	assert.doesNotMatch(css, /#14213d/);
});

test('a selector tokens.css must not have is refused, not guessed at', () => {
	assert.throws(() => tokensFrom(fixture + '\n.button { color: red; }\n'), /unexpected selector/);
	assert.throws(() => readTokens('/nonexistent/tokens.css'), /not found/);
});

test('the code theme is made of the theme’s inks', () => {
	const t = tokensFrom(
		fixture
			.replace("--color-paper: #fbfbf8;", "--color-paper: #fbfbf8; --color-paper-sunken: #f3f3ee; --color-ink-secondary: #3a4661; --color-ink-muted: #5d6a85; --color-blue-tint: #e8ecf5;")
			.replace("--color-paper: #13161c;", "--color-paper: #13161c; --color-paper-sunken: #0f1217; --color-ink-secondary: #c3c2ba; --color-ink-muted: #9ba0ab; --color-blue-tint: #1c2741;"),
	);
	const light = codeTheme('light', t);
	assert.equal(light.colors['editor.foreground'], '#14213d');
	assert.equal(light.colors['editor.background'], '#f3f3ee');
	assert.equal(codeTheme('dark', t).colors['editor.foreground'], '#e7e5dc');
});

test('the drift check refuses a colour, a face or a token name written outside tokens.css', () => {
	const t = tokensFrom(fixture);
	const planted = new Map([
		['a.css', '.x { color: #ff0000; }'],
		['b.css', '.x { background: rgb(1 2 3); }'],
		['c.css', '.x { border: 1px solid red; }'],
		['d.css', ".x { font-family: 'Inter', sans-serif; }"],
		['e.css', '.x { color: var(--color-gone); }'],
	]);
	const problems = checkSources(planted, t);
	for (const file of ['a.css', 'b.css', 'c.css', 'd.css', 'e.css']) {
		assert.ok(problems.some((p) => p.startsWith(`${file}:1`)), `${file} not refused: ${problems.join('; ')}`);
	}
});

test('the drift check passes what the site legitimately writes', () => {
	const t = tokensFrom(fixture);
	const ok = new Map([
		['ok.css', [
			'/* a comment may say #fff and red */',
			'.x { color: var(--color-ink); font-family: var(--font-sans); }',
			'.y { border: 1px solid var(--color-blue); background: color-mix(in oklab, var(--color-blue) 40%, transparent); }',
			'.z { --local: 2; width: calc(var(--local) * 1rem); color: var(--sl-color-white); }',
			'a[href="#tasks"] { color: var(--color-fg); }',
		].join('\n')],
		['ok.astro', '<li style={`--line: ${n}`}>x</li>\n<style>.b { top: calc(var(--line) * 1px); }</style>'],
	]);
	assert.deepEqual(checkSources(ok, t), []);
});
