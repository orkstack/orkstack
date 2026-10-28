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
	--font-mono: 'A Mono', monospace;
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

const SYNTAX_LIGHT = '--color-syntax-string: #006b65; --color-syntax-number: #824f00; --color-syntax-literal: #5e6115; --color-syntax-template: #6d4697; --color-syntax-meta: #425f7d;';
const SYNTAX_DARK = '--color-syntax-string: #81c3bc; --color-syntax-number: #d8aa79; --color-syntax-literal: #b5ba81; --color-syntax-template: #c2a4ea; --color-syntax-meta: #9eb7d2;';
const codeFixture = fixture
	.replace('--color-running-fg: var(--color-blue);', '--color-running-fg: var(--color-blue); --color-syntax-key: var(--color-ink); --color-syntax-comment: var(--color-ink-muted); --color-syntax-punctuation: var(--color-ink-muted);')
	.replace("--color-paper: #fbfbf8;", `--color-paper: #fbfbf8; --color-paper-sunken: #f3f3ee; --color-ink-secondary: #3a4661; --color-ink-muted: #5d6a85; --color-blue-tint: #e8ecf5; ${SYNTAX_LIGHT}`)
	.replace("--color-paper: #13161c;", `--color-paper: #13161c; --color-paper-sunken: #0f1217; --color-ink-secondary: #c3c2ba; --color-ink-muted: #9ba0ab; --color-blue-tint: #1c2741; ${SYNTAX_DARK}`);

/** The colour a theme gives a TextMate scope: the most specific rule wins, as in Shiki. */
function colourOf(theme, scope) {
	let best = { length: -1, foreground: theme.tokenColors[0].settings.foreground };
	for (const rule of theme.tokenColors) {
		for (const s of rule.scope ?? []) {
			if ((scope === s || scope.startsWith(`${s}.`)) && s.length > best.length && rule.settings.foreground) {
				best = { length: s.length, foreground: rule.settings.foreground };
			}
		}
	}
	return best.foreground;
}

test('the code theme is made of the theme’s inks', () => {
	const t = tokensFrom(codeFixture);
	const light = codeTheme('light', t);
	assert.equal(light.colors['editor.foreground'], '#14213d');
	assert.equal(light.colors['editor.background'], '#f3f3ee');
	assert.equal(codeTheme('dark', t).colors['editor.foreground'], '#e7e5dc');
});

test('YAML takes the UI’s syntax colours, role for role, in both themes', () => {
	const t = tokensFrom(codeFixture);
	for (const theme of ['light', 'dark']) {
		const code = codeTheme(theme, t);
		const token = (name) => resolve(t, name, theme);
		const expect = {
			'entity.name.tag.yaml': token('--color-syntax-key'),
			'string.unquoted.plain.out.yaml': token('--color-syntax-string'),
			'string.quoted.double.yaml': token('--color-syntax-string'),
			'punctuation.definition.string.begin.yaml': token('--color-syntax-string'),
			'constant.numeric.integer.yaml': token('--color-syntax-number'),
			'constant.language.boolean.yaml': token('--color-syntax-literal'),
			'constant.language.null.yaml': token('--color-syntax-literal'),
			'meta.template.expression.orkstack': token('--color-syntax-template'),
			'entity.name.type.anchor.yaml': token('--color-syntax-meta'),
			'variable.other.alias.yaml': token('--color-syntax-meta'),
			'storage.type.tag-handle.yaml': token('--color-syntax-meta'),
			'comment.line.number-sign.yaml': token('--color-syntax-comment'),
			'punctuation.separator.key-value.mapping.yaml': token('--color-syntax-punctuation'),
		};
		for (const [scope, colour] of Object.entries(expect)) assert.equal(colourOf(code, scope), colour, `${theme} ${scope}`);
		// The three accents never colour code.
		const accents = ['--color-blue', '--color-green', '--color-red'].filter((n) => t.names.has(n)).map(token);
		for (const rule of code.tokenColors) assert.ok(!accents.includes(rule.settings.foreground), `${theme}: an accent colours ${rule.scope}`);
	}
});

test('the drift check refuses a colour, a face or a token name written outside tokens.css', () => {
	const t = tokensFrom(fixture);
	const planted = new Map([
		['a.css', '.x { color: #ff0000; }'],
		['b.css', '.x { background: rgb(1 2 3); }'],
		['c.css', '.x { border: 1px solid red; }'],
		['d.css', ".x { font-family: 'Inter', sans-serif; }"],
		['e.css', '.x { color: var(--color-gone); }'],
		['f.astro', '<rect fill="#fff" />'],
		['g.ts', "const c = ['#abc'];"],
		['h.css', '/* x */ .a { color: #fff; }'],
		['i.css', '*, *::before { background: tomato; }'],
		['j.css', '.x { color: RGB(1 2 3); }'],
		['k.css', ".x { font: 600 1rem 'Inter'; }"],
		['l.astro', '<path stroke="rebeccapurple" />'],
	]);
	const problems = checkSources(planted, t);
	for (const file of ['a.css', 'b.css', 'c.css', 'd.css', 'e.css', 'f.astro', 'g.ts', 'h.css', 'i.css', 'j.css', 'k.css', 'l.astro']) {
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
			'.f { font: inherit; } .g { font: 600 1rem var(--font-mono); }',
			'// a line comment may say #abc',
			'<a href="#add">jump</a> <a href="https://example.test/x">x</a>',
		].join('\n')],
		['ok.astro', '<li style={`--line: ${n}`}>x</li>\n<style>.b { top: calc(var(--line) * 1px); }</style>'],
	]);
	assert.deepEqual(checkSources(ok, t), []);
});
