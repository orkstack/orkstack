import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { Scanner } from '@tailwindcss/oxide';
import { __unstable__loadDesignSystem, compile } from '@tailwindcss/node';
import { PIN_ACCENTS } from '../src/lib/runDetail';

// The token discipline: every value the UI is drawn with lives in
// ui/src/tokens.css, and these rules make a value written anywhere else a
// failing test rather than a review comment. Each rule is a function, and
// each has a fixture it must catch and one it must pass, so a pattern that
// silently matches nothing fails here first.
//
//   1. no raw colour literal;
//   2. no Tailwind palette, arbitrary colour or ring-offset utility;
//   3. no raw font, size, radius, shadow or motion literal;
//   4. every var(--name) resolves to a declared token;
//   5. no dead utility (one that Tailwind would emit but our registration
//      does not), including a variant that stopped existing;
//   6. the registration and tokens.css declare the same names, and light and
//      dark the same colours and shadows;
//   7. tokens.css is portable CSS, and app.css declares no value;
//   8. the compiled stylesheet holds no colour outside tokens.css.

const ui = `${import.meta.dir}/..`;
const src = `${ui}/src`;
const read = (path: string) => readFileSync(path, 'utf8');
const tokensCss = read(`${src}/tokens.css`);
const registrationCss = read(`${src}/tailwind-tokens.css`);
const appCss = read(`${src}/app.css`);

interface Source {
	path: string;
	ext: string;
	text: string;
}

const sources: Source[] = readdirSync(src, { recursive: true })
	.map((rel) => `${src}/${rel}`)
	.filter((p) => /\.(svelte|ts|css|html)$/.test(p) && !p.includes('/lib/gen/') && !p.endsWith('/src/tokens.css'))
	.map((path) => ({ path, ext: path.slice(path.lastIndexOf('.') + 1), text: read(path) }));

const rel = (path: string) => path.slice(src.length + 1);

/** Comments carry prose about colours ("the red cloud"); they are not values. */
function stripComments(text: string, ext: string): string {
	let out = text.replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/<!--[\s\S]*?-->/g, ' ');
	if (ext === 'ts' || ext === 'svelte') out = out.replace(/(^|[^:'"`\\])\/\/[^\n]*/g, '$1');
	return out;
}

function hits(text: string, pattern: RegExp): string[] {
	return [...text.matchAll(pattern)].map((m) => m[0]);
}

// ---------------------------------------------------------------- rule 1

const NAMED_COLOURS =
	'white|black|red|green|blue|gray|grey|silver|maroon|purple|fuchsia|lime|olive|yellow|navy|teal|aqua|orange|pink|brown|gold|indigo|violet|crimson|coral|salmon|tomato|khaki|beige|ivory|lavender|magenta|cyan|turquoise|tan|plum|orchid|chocolate|firebrick|darkred|darkblue|darkgreen|lightgray|lightgrey|darkgray|darkgrey|whitesmoke|gainsboro|snow|linen|wheat|azure';

export function rawColours(text: string, ext: string): string[] {
	const t = stripComments(text, ext).replace(/href="#[^"]*"/g, 'href=""');
	return [
		...hits(t, /(?<![&\w])#(?:[0-9a-fA-F]{8}|[0-9a-fA-F]{6}|[0-9a-fA-F]{3,4})(?![\w-])/g),
		...hits(t, /(?<![\w.$-])(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch)\(/g),
		...hits(t, /(?<![\w.$-])color\(\s*(?:srgb|display-p3|a98-rgb|prophoto-rgb|rec2020|xyz)/g),
		...hits(
			t,
			new RegExp(
				`(?<![\\w-])(?:fill|stroke|stop-color|flood-color|lighting-color|color|background|background-color|border-color|outline-color)\\s*[=:]\\s*["']?(?:${NAMED_COLOURS})(?![\\w-])`,
				'gi'
			)
		)
	];
}

test('rule 1 fixtures: raw colours are caught, tokens pass', () => {
	expect(rawColours('<rect fill="#fff" />', 'svelte')).toHaveLength(1);
	expect(rawColours('color: rgb(1 2 3);', 'css')).toHaveLength(1);
	expect(rawColours('a { color: oklch(0.5 0.1 20) }', 'css')).toHaveLength(1);
	expect(rawColours('<circle fill="white" />', 'svelte')).toHaveLength(1);
	expect(rawColours("style=\"background: black\"", 'svelte')).toHaveLength(1);
	expect(rawColours('<a href="#task-row-add">x</a> <rect fill="currentColor" stroke="var(--color-ink)" />', 'svelte')).toEqual([]);
	expect(rawColours('/* the red cloud, #fff */ a { color: var(--color-red) }', 'css')).toEqual([]);
	expect(rawColours('const q = `#${id}`; white-space: nowrap;', 'ts')).toEqual([]);
});

test('rule 1: no raw colour outside tokens.css', () => {
	const found = sources.flatMap((s) => rawColours(s.text, s.ext).map((h) => `${rel(s.path)}: ${h}`));
	expect(found).toEqual([]);
});

// ---------------------------------------------------------------- rule 2

const PALETTE =
	'slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose';
const COLOUR_UTILITY =
	'bg|text|border|border-[xytrblse]|ring|outline|fill|stroke|divide|decoration|shadow|accent|caret|placeholder|from|via|to|inset-ring|inset-shadow|drop-shadow|text-shadow';

export function paletteUtilities(text: string): string[] {
	return [
		...hits(text, new RegExp(`(?<![\\w-])(?:${COLOUR_UTILITY})-(?:${PALETTE})-(?:50|[1-9]00|950)(?:/\\d+)?(?![\\w-])`, 'g')),
		...hits(text, new RegExp(`(?<![\\w-])(?:${COLOUR_UTILITY})-(?:white|black)(?:/\\d+)?(?![\\w-])`, 'g')),
		...hits(text, new RegExp(`(?<![\\w-])(?:${COLOUR_UTILITY})-\\[(?:#|rgb|hsl|hwb|lab|lch|oklab|oklch|color)`, 'g')),
		...hits(text, /(?<![\w-])ring-offset-[\w[\]/.-]+/g)
	];
}

test('rule 2 fixtures: palette shades, white/black, arbitrary colours and ring offsets are caught', () => {
	expect(paletteUtilities('class="bg-gray-950/40 text-white hover:bg-red-500 border-[#123456]"')).toHaveLength(4);
	expect(paletteUtilities('class="focus-visible:ring-offset-2 ring-offset-canvas"')).toHaveLength(2);
	expect(paletteUtilities('class="bg-red-tint text-red border-blue-strong bg-paper whitespace-nowrap text-danger-700"')).toEqual([]);
});

test('rule 2: no palette, arbitrary colour or ring-offset utility', () => {
	const found = sources
		.filter((s) => s.ext !== 'css')
		.flatMap((s) => paletteUtilities(stripComments(s.text, s.ext)).map((h) => `${rel(s.path)}: ${h}`));
	expect(found).toEqual([]);
});

test('rule 2: no registered colour token takes a palette shade', () => {
	const names = [...registrationCss.matchAll(/^\s*(--color-[a-z0-9-]+):/gm)].map((m) => m[1]);
	expect(names.length > 40).toBe(true);
	const shaded = names.filter((n) => new RegExp(`^--color-(?:${PALETTE}|white|black)-\\d+$`).test(n));
	expect(shaded).toEqual([]);
});

// ---------------------------------------------------------------- rule 3

export function rawLiterals(text: string, ext: string): string[] {
	const t = stripComments(text, ext);
	return [
		...hits(t, /font-family\s*:\s*(?!\s*var\()[^;"}]+/g),
		...hits(t, /(?<![\w-])font-\[[^\]]*\]/g),
		...hits(t, /(?<![\w-])text-\[(?:length:)?[\d.]+(?:px|rem|em)\]/g),
		...hits(t, /(?<![\w-])(?:rounded(?:-[a-z]+)?|shadow|tracking|leading|ease)-\[[^\]]*\]/g),
		...hits(t, /cubic-bezier\(/g),
		...hits(t, /(?<![\w-])(?:duration|delay)-\d+(?![\w-])/g),
		...hits(t, /(?<![\w-])-?(?:p|px|py|pt|pr|pb|pl|ps|pe|m|mx|my|mt|mr|mb|ml|ms|me|gap|gap-x|gap-y|space-x|space-y)-\[[^\]]*\]/g),
		...hits(t, /(?<![\w-])(?:font-size|font-family|font-weight)=/g)
	];
}

test('rule 3 fixtures: raw type, size, radius, shadow and motion literals are caught', () => {
	expect(
		rawLiterals(
			'<p class="text-[11px] rounded-[5px] shadow-[0_1px_2px] tracking-[0.2em] duration-150 p-[3px] font-[500]"></p>',
			'svelte'
		)
	).toHaveLength(7);
	expect(rawLiterals('<text font-size="11" font-weight="600">a</text>', 'svelte')).toHaveLength(2);
	expect(rawLiterals('a { font-family: Inter, sans-serif; transition: all 1s cubic-bezier(0,0,1,1) }', 'css')).toHaveLength(2);
	expect(
		rawLiterals(
			'<p class="text-2xs rounded-md shadow-overlay tracking-caps transition-colors p-4 max-h-[85vh] w-[12rem]" style="font-family: var(--font-mono)"><rect rx="2" stroke-width="1.5" stroke-dasharray="5 4" /></p>',
			'svelte'
		)
	).toEqual([]);
});

test('rule 3: no raw font, size, radius, shadow or motion literal outside tokens.css', () => {
	const found = sources.flatMap((s) => rawLiterals(s.text, s.ext).map((h) => `${rel(s.path)}: ${h}`));
	expect(found).toEqual([]);
});

// ---------------------------------------------------------------- rule 4

const declared = new Set([...tokensCss.matchAll(/^\s*(--[a-z0-9-]+)\s*:/gm)].map((m) => m[1]));

/** A templated reference is allowed only through this list, whose values come from source. */
const TEMPLATED_TOKENS = [{ prefix: '--color-', suffixes: ['-line', '-fill'], values: [...PIN_ACCENTS] }];

/** The custom-property name starting at `at`, with any {…} / ${…} kept whole. */
function varName(text: string, at: number): string {
	let depth = 0;
	let i = at;
	for (; i < text.length; i++) {
		const c = text[i];
		if (c === '{') depth++;
		else if (c === '}') depth--;
		else if (depth === 0 && (c === ',' || c === ')' || /\s/.test(c))) break;
	}
	return text.slice(at, i);
}

export function unresolvedVars(text: string, ext: string, names: Set<string>): string[] {
	const t = stripComments(text, ext);
	const local = new Set([...t.matchAll(/(--_[\w-]+)\s*:/g)].map((m) => m[1]));
	for (const m of t.matchAll(/style:(--_[\w-]+)/g)) local.add(m[1]);
	const bad: string[] = [];
	for (const m of t.matchAll(/var\(\s*(?=--)/g)) {
		const name = varName(t, (m.index ?? 0) + m[0].length);
		if (name.includes('{')) {
			const whole = name;
			const entry = TEMPLATED_TOKENS.find(
				(e) =>
					whole.startsWith(e.prefix) &&
					e.suffixes.some((suffix) => whole.endsWith(suffix)) &&
					e.values.every((v) => e.suffixes.every((suffix) => names.has(`${e.prefix}${v}${suffix}`)))
			);
			if (!entry) bad.push(whole);
			continue;
		}
		if (names.has(name) || name.startsWith('--tw-') || local.has(name)) continue;
		bad.push(name);
	}
	return bad;
}

test('rule 4 fixtures: typos and unlisted templates are caught, tokens and listed templates pass', () => {
	const names = new Set(['--color-ink', '--color-pin-a-line', '--color-pin-a-fill', ...PIN_ACCENTS.flatMap((p) => [`--color-${p}-line`, `--color-${p}-fill`])]);
	expect(unresolvedVars('a { color: var(--color-inc) }', 'css', names)).toEqual(['--color-inc']);
	expect(unresolvedVars('style="color: var(--color-${x}-tone)"', 'svelte', names)).toHaveLength(1);
	expect(
		unresolvedVars(
			'a { color: var(--color-ink); --_w: 2px; width: var(--_w); } <b style="border-color: var(--color-{pinAccent(t)}-line)"></b> `var(--color-${accent}-fill)` var(--tw-shadow)',
			'svelte',
			names
		)
	).toEqual([]);
});

test('rule 4: every var() resolves to a token', () => {
	expect(PIN_ACCENTS.length).toBe(6);
	const all = [...sources, { path: `${src}/tokens.css`, ext: 'css', text: tokensCss }];
	const found = all.flatMap((s) => unresolvedVars(s.text, s.ext, declared).map((h) => `${rel(s.path)}: ${h}`));
	expect(found).toEqual([]);
	for (const accent of PIN_ACCENTS) {
		for (const suffix of ['line', 'fill']) {
			const name = `--color-${accent}-${suffix}`;
			expect(block(LIGHT).includes(`${name}:`)).toBe(true);
			expect(block(DARK).includes(`${name}:`)).toBe(true);
		}
	}
});

// ---------------------------------------------------------------- rule 5

const VARIANTS = [...appCss.matchAll(/@custom-variant[^;]+;/g)].map((m) => m[0]).join('\n');

/** Tailwind's default theme with our registration added (resets removed). */
function referenceCss(variants: string): string {
	const names = registrationCss.replace(/^\s*--[\w-]+-\*: initial;\s*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
	return `@import 'tailwindcss';\n${names}\n${variants}\n`;
}

type DesignSystem = Awaited<ReturnType<typeof __unstable__loadDesignSystem>>;

const COLOUR_PREFIXES = COLOUR_UTILITY.replace('border-[xytrblse]', 'border-x|border-y|border-t|border-r|border-b|border-l|border-s|border-e').split('|').sort((a, b) => b.length - a.length);
const colourFamilies = [...new Set([...registrationCss.matchAll(/^\s*--color-([a-z]+)/gm)].map((m) => m[1]))];

const THEMED = /^-?(?:animate|shadow|inset-shadow|drop-shadow|text-shadow|rounded(?:-[trblse]{1,2})?|tracking|leading|ease|font)-[a-z][\w-]*$/;

function splitVariants(candidate: string): { variants: string[]; utility: string } {
	const parts: string[] = [];
	let depth = 0;
	let start = 0;
	for (let i = 0; i < candidate.length; i++) {
		const c = candidate[i];
		if (c === '[' || c === '(') depth++;
		else if (c === ']' || c === ')') depth--;
		else if (c === ':' && depth === 0) {
			parts.push(candidate.slice(start, i));
			start = i + 1;
		}
	}
	return { variants: parts, utility: candidate.slice(start) };
}

function namesRegisteredColour(utility: string): boolean {
	const bare = utility.replace(/^!|!$/g, '').replace(/^-/, '');
	for (const prefix of COLOUR_PREFIXES) {
		if (!bare.startsWith(`${prefix}-`)) continue;
		const value = bare.slice(prefix.length + 1).replace(/\/\d+$/, '');
		return colourFamilies.some((family) => value === family || value.startsWith(`${family}-`));
	}
	return false;
}

export function deadCandidates(candidates: string[], reference: DesignSystem, ours: DesignSystem): string[] {
	const unique = [...new Set(candidates)];
	const ref = reference.candidatesToCss(unique);
	const our = ours.candidatesToCss(unique);
	const dead: string[] = [];
	unique.forEach((candidate, i) => {
		if (our[i]) return;
		if (ref[i]) {
			dead.push(candidate);
			return;
		}
		const { variants, utility } = splitVariants(candidate);
		if (namesRegisteredColour(utility)) {
			dead.push(candidate);
			return;
		}
		if (variants.length > 0 && ours.candidatesToCss([utility])[0]) {
			dead.push(candidate);
			return;
		}
		// A themed utility whose name only an older stylesheet defined
		// (animate-pulse-dot, shadow-card): neither system emits it.
		if (THEMED.test(utility)) dead.push(candidate);
	});
	return dead;
}

// A component's <style> block is CSS, not markup: its property names
// (font-size, font-family) are declarations, never class candidates. Rules 1
// and 3 still read every value in it.
function candidatesOf(text: string, ext: string): string[] {
	const scanner = new Scanner({});
	const content = ext === 'svelte' ? stripComments(text, ext).replace(/<style[\s\S]*?<\/style>/g, ' ') : stripComments(text, ext);
	return scanner.getCandidatesWithPositions({ content, extension: ext }).map((c) => c.candidate);
}

async function systems(appCssText: string) {
	const variants = [...appCssText.matchAll(/@custom-variant[^;]+;/g)].map((m) => m[0]).join('\n');
	const reference = await __unstable__loadDesignSystem(referenceCss(variants), { base: src });
	const ours = await __unstable__loadDesignSystem(appCssText, { base: src });
	return { reference, ours };
}

test('rule 5 fixtures: dead utilities and lost variants are caught; prose and live classes pass', async () => {
	const { reference, ours } = await systems(appCss);
	const fixture = candidatesOf(
		'<p class="rounded-lg shadow-xs text-white hover:bg-warning-100 animate-pulse-dot bg-paper text-ink p-4 rounded-md current:text-fg animate-live-dot font-semibold">to row list content-type</p>',
		'svelte'
	);
	expect(deadCandidates(fixture, reference, ours).sort()).toEqual(
		['animate-pulse-dot', 'hover:bg-warning-100', 'rounded-lg', 'shadow-xs', 'text-white'].sort()
	);
	// A <style> block's declarations are not candidates; a class beside it still is.
	const styled = candidatesOf('<p class="rounded-lg">x</p><style>.a { font-size: var(--text-sm); font-family: var(--font-sans); }</style>', 'svelte');
	expect(deadCandidates(styled, reference, ours)).toEqual(['rounded-lg']);
	// The variant clause: drop @custom-variant current, and every current:
	// candidate is dead — including one that is not a colour utility.
	const dropped = await systems(appCss.replace(/@custom-variant current[^;]+;/, ''));
	const lost = candidatesOf('<a class="current:text-fg current:font-semibold flex">x</a>', 'svelte');
	expect(deadCandidates(lost, dropped.reference, dropped.ours).sort()).toEqual(['current:font-semibold', 'current:text-fg']);
});

test('rule 5: no dead utility in the sources', async () => {
	const { reference, ours } = await systems(appCss);
	const found: string[] = [];
	for (const s of sources.filter((x) => x.ext !== 'css')) {
		for (const dead of deadCandidates(candidatesOf(s.text, s.ext), reference, ours)) found.push(`${rel(s.path)}: ${dead}`);
	}
	expect(found).toEqual([]);
});

// ---------------------------------------------------------------- rule 6

const LIGHT = ":root,\n:root[data-theme='light'] {";
const DARK = ":root[data-theme='dark'] {";
const INVARIANT = ':root {';

function block(opening: string): string {
	const start = tokensCss.indexOf(opening);
	if (start < 0) throw new Error(`tokens.css has no block ${opening}`);
	const end = tokensCss.indexOf('\n}', start);
	return tokensCss.slice(start, end);
}

const namesIn = (body: string) => new Set([...body.matchAll(/^\s*(--[a-z0-9-]+)\s*:/gm)].map((m) => m[1]));

/** Read with var() from CSS or an inline style; never a utility. */
const RUNTIME_ONLY = /^--(?:color-pin-|rule-|duration-|grid-pitch-|underline-offset$|opacity-)/;

test('rule 6: tokens.css and the registration declare the same names', () => {
	const registered = new Set(
		[...registrationCss.matchAll(/^\s*(--[a-z0-9-]+)\s*:\s*var\(\1\)/gm)].map((m) => m[1])
	);
	const missing = [...declared].filter((n) => !registered.has(n) && !RUNTIME_ONLY.test(n));
	const undeclared = [...registered].filter((n) => !declared.has(n));
	expect(missing).toEqual([]);
	expect(undeclared).toEqual([]);
	// Every registered line maps a name to itself, so no value hides here.
	const lines = [...registrationCss.matchAll(/^\s*(--[a-z0-9-]+)\s*:\s*([^;]+);/gm)];
	const other = lines.filter(([, name, value]) => value !== `var(${name})` && value !== 'initial' && !name.startsWith('--default-transition-'));
	expect(other.map((m) => m[0].trim())).toEqual([]);
});

test('rule 6: light and dark declare exactly the same colours and shadows', () => {
	const light = [...namesIn(block(LIGHT))].sort();
	const dark = [...namesIn(block(DARK))].sort();
	expect(light.length > 30).toBe(true);
	expect(dark).toEqual(light);
	expect(light.every((n) => n.startsWith('--color-') || n.startsWith('--shadow-'))).toBe(true);
	// The invariant block holds no literal colour: its colours are aliases.
	const literal = [...block(INVARIANT).matchAll(/^\s*(--color-[a-z0-9-]+)\s*:\s*(?!\s|var\()([^;]+);/gm)].map((m) => m[1]);
	expect(literal).toEqual([]);
});

// ---------------------------------------------------------------- rule 7

export function portabilityProblems(css: string): string[] {
	const body = css.replace(/\/\*[\s\S]*?\*\//g, '');
	const problems = hits(body, /@(?:import|theme|apply|tailwind|utility|custom-variant|layer|plugin|source)\b/g);
	if (/url\(/.test(body)) problems.push('url(');
	// Every block opener at the top level is an allowed selector or @keyframes.
	let depth = 0;
	let start = 0;
	for (let i = 0; i < body.length; i++) {
		if (body[i] === '{') {
			if (depth === 0) {
				const selector = body.slice(start, i).trim().replace(/\s+/g, ' ');
				const ok =
					/^@keyframes [\w-]+$/.test(selector) ||
					selector === ':root' ||
					selector === ":root, :root[data-theme='light']" ||
					selector === ":root[data-theme='dark']";
				if (!ok) problems.push(selector);
			}
			depth++;
		} else if (body[i] === '}') {
			depth--;
			if (depth === 0) start = i + 1;
		}
	}
	return problems;
}

export function appCssValues(css: string): string[] {
	const body = css.replace(/\/\*[\s\S]*?\*\//g, '').replace(/@import[^;]+;/g, '');
	return [
		...rawColours(body, 'css'),
		...hits(body, /(?<![\w-])\d*\.?\d+(?:px|rem|em|ms|s|vh|vw)(?![\w-])/g),
		...hits(body, /cubic-bezier\(/g)
	];
}

test('rule 7 fixtures', () => {
	expect(portabilityProblems("@import 'x.css';\n.btn { color: red }\n:root { --a: url(x) }")).toHaveLength(3);
	expect(portabilityProblems(":root { --a: 1px }\n:root[data-theme='dark'] { --b: 2px }\n@keyframes x { from { opacity: 0 } }")).toEqual([]);
	expect(appCssValues('.a { width: 3px; color: #fff; transition: 1s cubic-bezier(0,0,1,1) }')).toHaveLength(4);
	expect(appCssValues("@import './tokens.css';\n.a { width: calc(var(--spacing) * 9); opacity: var(--opacity-disabled); background: linear-gradient(45deg, transparent 38%, var(--color-paper) 38% 62%) }")).toEqual([]);
});

test('rule 7: tokens.css is portable; app.css imports it, declares no value, and keeps its variant', () => {
	expect(portabilityProblems(tokensCss)).toEqual([]);
	expect(appCss.includes("@import './tokens.css';")).toBe(true);
	expect(appCss.includes("@import './tailwind-tokens.css';")).toBe(true);
	expect(appCssValues(appCss)).toEqual([]);
	expect(/@custom-variant current \(&\[aria-current='page'\]\);/.test(appCss)).toBe(true);
	expect(VARIANTS.length > 0).toBe(true);
});

// ---------------------------------------------------------------- rule 8

/** The two inert values Tailwind's own preflight and @property defaults carry (4.3.3). */
const COMPILED_ALLOWED = new Map<string, string>([
	['--tw-shadow', '0 0 #0000'],
	['--tw-inset-shadow', '0 0 #0000'],
	['--tw-ring-shadow', '0 0 #0000'],
	['--tw-inset-ring-shadow', '0 0 #0000'],
	['--tw-ring-offset-shadow', '0 0 #0000'],
	['--tw-ring-offset-color', '#fff']
]);

const COLOUR_VALUE = new RegExp(
	`(?<![\\w-])(?:#[0-9a-fA-F]{3,8}|(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\\(|(?:${NAMED_COLOURS})(?![\\w-]))`,
	'i'
);

export function compiledColours(css: string): string[] {
	const found: string[] = [];
	// @property blocks: their initial-value belongs to the property they name.
	for (const m of css.matchAll(/@property\s+(--[\w-]+)\s*\{([^}]*)\}/g)) {
		const initial = m[2].match(/initial-value\s*:\s*([^;]+);?/)?.[1]?.trim();
		if (initial && COLOUR_VALUE.test(initial) && COMPILED_ALLOWED.get(m[1]) !== initial) found.push(`${m[1]}: ${initial}`);
	}
	const body = css.replace(/@property\s+--[\w-]+\s*\{[^}]*\}/g, '');
	for (const m of body.matchAll(/([-\w]+)\s*:\s*([^;{}]+);/g)) {
		const [, prop, raw] = m;
		const value = raw.trim();
		if (prop === 'font-family' || prop === 'white-space' || prop === 'text-decoration-line') continue;
		if (!COLOUR_VALUE.test(value)) continue;
		if (COMPILED_ALLOWED.get(prop) === value) continue;
		found.push(`${prop}: ${value}`);
	}
	return found;
}

test('rule 8 fixtures', () => {
	expect(compiledColours('.a { color: #fff; } .b { background-color: rgb(0 0 0); } .c { fill: white; }')).toHaveLength(3);
	expect(compiledColours('@property --x { syntax: "*"; initial-value: #fff; }')).toHaveLength(1);
	expect(
		compiledColours(
			'.a { color: var(--color-ink); white-space: nowrap; } @supports (color: color-mix(in lab, red, red)) { .b { color: color-mix(in oklab, var(--color-ink) 50%, transparent); } } @property --tw-ring-offset-color { syntax: "*"; inherits: false; initial-value: #fff; } *, ::before { --tw-shadow: 0 0 #0000; --tw-ring-offset-color: #fff; }'
		)
	).toEqual([]);
});

test('rule 8: the compiled stylesheet holds no colour outside tokens.css', async () => {
	const empty = `${import.meta.dir}/fixtures/empty.css`;
	const compiler = await compile(appCss, {
		base: src,
		onDependency: () => {},
		customCssResolver: async (id: string) => (id === './tokens.css' ? empty : undefined)
	});
	const candidates = sources.filter((s) => s.ext !== 'css').flatMap((s) => candidatesOf(s.text, s.ext));
	const css = compiler.build([...new Set([...candidates, 'bg-paper'])]);
	expect(css.includes('.bg-paper')).toBe(true);
	expect(css.includes('--color-paper: #')).toBe(false);
	expect(compiledColours(css)).toEqual([]);
	// The ring-offset white is inert only while no utility sets a width.
	const widths = [...css.matchAll(/--tw-ring-offset-width\s*:\s*([^;]+);/g)].map((m) => m[1].trim());
	expect(widths.every((w) => w === '0px')).toBe(true);
});
