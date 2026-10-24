// The site's one reader of ui/src/tokens.css, the UI's design tokens.
//
// The site keeps no copy of a token's value: it imports tokens.css itself,
// and everything else it needs from it is derived here, at build time, from
// the file as it is on disk:
//
//   - the no-JavaScript dark theme: tokens.css switches theme on
//     :root[data-theme], which only a script sets, so a visitor with
//     JavaScript off would always see light. darkMediaCss() repeats the dark
//     block under (prefers-color-scheme: dark) for a root with no data-theme;
//   - the code-block theme Expressive Code needs as literal colours
//     (codeTheme()), since it computes contrast and cannot read var();
//   - the token names the drift check (check-tokens.mjs) holds the site's
//     mapping to.
//
// A missing tokens.css is an error, never a fallback: the site has no style
// of its own to fall back to.
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

export const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const repoRoot = path.resolve(siteRoot, '..');
export const tokensPath = path.join(repoRoot, 'ui', 'src', 'tokens.css');

function stripComments(css) {
	return css.replace(/\/\*[\s\S]*?\*\//g, '');
}

/** The custom-property declarations of one block body, in order. */
function declarations(body) {
	const out = new Map();
	for (const part of body.split(';')) {
		const m = /^\s*(--[A-Za-z0-9-]+)\s*:\s*([\s\S]+?)\s*$/.exec(part);
		if (m) out.set(m[1], m[2].replace(/\s+/g, ' '));
	}
	return out;
}

/**
 * Parse tokens.css into its three blocks. The file's own contract
 * (ui/test/tokens.test.ts, "portability") is that its only selectors are
 * :root, ":root, :root[data-theme='light']", :root[data-theme='dark'] and
 * @keyframes, so a flat scan of top-level blocks is exact; anything else is
 * refused rather than guessed at.
 */
export function readTokens(file = tokensPath) {
	if (!existsSync(file)) {
		throw new Error(
			`ui/src/tokens.css not found at ${file}: the site takes its whole style from it and has none of its own`,
		);
	}
	const css = stripComments(readFileSync(file, 'utf8'));
	const blocks = { invariant: new Map(), light: new Map(), dark: new Map() };
	const keyframes = new Set();
	const re = /([^{}]+)\{/g;
	let m;
	while ((m = re.exec(css))) {
		const selector = m[1].trim().replace(/\s+/g, ' ');
		let depth = 1;
		let i = re.lastIndex;
		for (; i < css.length && depth > 0; i++) {
			if (css[i] === '{') depth++;
			else if (css[i] === '}') depth--;
		}
		const body = css.slice(re.lastIndex, i - 1);
		re.lastIndex = i;
		if (selector.startsWith('@keyframes')) {
			keyframes.add(selector.slice('@keyframes'.length).trim());
			continue;
		}
		const key =
			selector === ':root'
				? 'invariant'
				: selector === ":root, :root[data-theme='light']"
					? 'light'
					: selector === ":root[data-theme='dark']"
						? 'dark'
						: null;
		if (!key) throw new Error(`tokens.css: unexpected selector ${JSON.stringify(selector)}`);
		for (const [k, v] of declarations(body)) blocks[key].set(k, v);
	}
	if (blocks.light.size === 0 || blocks.dark.size === 0) {
		throw new Error('tokens.css: expected a light and a dark block');
	}
	const names = new Set([...blocks.invariant.keys(), ...blocks.light.keys(), ...blocks.dark.keys()]);
	return { ...blocks, keyframes, names };
}

/** The dark block again, for a visitor whose page carries no data-theme. */
export function darkMediaCss(tokens = readTokens()) {
	const decls = [...tokens.dark].map(([k, v]) => `\t\t${k}: ${v};`).join('\n');
	return [
		'/* Generated from ui/src/tokens.css by site/scripts/tokens.mjs. */',
		'@media (prefers-color-scheme: dark) {',
		'\t:root:not([data-theme]) {',
		'\t\tcolor-scheme: dark;',
		decls,
		'\t}',
		'}',
		'',
	].join('\n');
}

/** A token's literal value in one theme, following var() aliases. */
export function resolve(tokens, name, theme) {
	const seen = new Set();
	const alias = /^var\((--[A-Za-z0-9-]+)\)$/;
	let value = tokens[theme].get(name) ?? tokens.invariant.get(name);
	while (value && alias.test(value)) {
		const next = alias.exec(value)[1];
		if (seen.has(next)) throw new Error(`tokens.css: alias cycle at ${next}`);
		seen.add(next);
		value = tokens[theme].get(next) ?? tokens.invariant.get(next);
	}
	if (!value) throw new Error(`tokens.css has no ${name} in the ${theme} theme`);
	return value;
}

/**
 * The code theme for one site theme, as a TextMate theme object.
 *
 * Monochrome on purpose: the UI rations hue to three accents, each with one
 * job (blue: links and selection; green: running; red: failure), and its pin
 * colours reach only 3:1, below the text floor. Code is ink: keys and
 * keywords in the ink at weight, values in the secondary ink, comments and
 * punctuation in the muted ink.
 */
export function codeTheme(theme, tokens = readTokens()) {
	const c = (name) => resolve(tokens, name, theme);
	const ink = c('--color-ink');
	const secondary = c('--color-ink-secondary');
	const muted = c('--color-ink-muted');
	return {
		name: `orkstack-${theme}`,
		type: theme,
		colors: {
			'editor.background': c('--color-paper-sunken'),
			'editor.foreground': ink,
			'editor.selectionBackground': c('--color-blue-tint'),
			'editorLineNumber.foreground': muted,
			'terminal.foreground': ink,
			'terminal.background': c('--color-paper-sunken'),
		},
		tokenColors: [
			{ settings: { foreground: ink } },
			{
				scope: ['comment', 'punctuation.definition.comment'],
				settings: { foreground: muted, fontStyle: 'italic' },
			},
			{
				scope: [
					'entity.name.tag',
					'support.type.property-name',
					'keyword',
					'storage',
					'storage.type',
					'entity.name.function',
					'support.function',
				],
				settings: { foreground: ink, fontStyle: 'bold' },
			},
			{
				scope: [
					'string',
					'constant',
					'constant.numeric',
					'constant.language',
					'variable.parameter',
					'entity.other.attribute-name',
				],
				settings: { foreground: secondary },
			},
			{
				scope: ['punctuation', 'meta.brace', 'keyword.operator'],
				settings: { foreground: muted },
			},
		],
	};
}
