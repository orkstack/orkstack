import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { PIN_ACCENTS } from '../src/lib/runDetail';

// The WCAG 2.2 AA floors of Harbour's colours, read from the ACTUAL tokens
// (ui/src/tokens.css), in both themes. Each pair is a pair the UI draws: a
// word on the ground it sits on, a mark or a rule beside its ground. The
// pairs are the design's own contrast table (121 pairs at the freeze), held
// here against the shipped values rather than the prototype's.
//
//   text      4.5:1 (1.4.3)
//   non-text  3:1   (1.4.11): focus, input edges, marks, meters, scrollbars
//
// A pair is excused only by a NEVER_ADJACENT entry that says why it cannot
// occur. There are none.
const css = readFileSync(`${import.meta.dir}/../src/tokens.css`, 'utf8');

const NEVER_ADJACENT: { fg: string; bg: string; why: string }[] = [];

function slice(opening: string, closing: string): string {
	const start = css.indexOf(opening);
	if (start < 0) throw new Error(`tokens.css has no block ${opening}`);
	return css.slice(start + opening.length, css.indexOf(closing, start));
}

function decls(body: string): Map<string, string> {
	return new Map([...body.matchAll(/^\s*(--[a-z0-9-]+)\s*:\s*([^;]+);/gm)].map((m) => [m[1], m[2].trim()]));
}

const invariant = decls(slice(':root {', '\n}'));
const themes = {
	light: new Map([...invariant, ...decls(slice(":root,\n:root[data-theme='light'] {", '\n}'))]),
	dark: new Map([...invariant, ...decls(slice(":root[data-theme='dark'] {", '\n}'))])
};

type RGBA = [number, number, number, number];

function parse(value: string, theme: Map<string, string>, depth = 0): RGBA {
	if (depth > 10) throw new Error(`var() cycle at ${value}`);
	const v = value.match(/^var\((--[a-z0-9-]+)\)$/);
	if (v) {
		const next = theme.get(v[1]);
		if (!next) throw new Error(`undeclared ${v[1]}`);
		return parse(next, theme, depth + 1);
	}
	const hex = value.match(/^#([0-9a-f]{6})$/i);
	if (hex) return [0, 2, 4].map((i) => parseInt(hex[1].slice(i, i + 2), 16)).concat(1) as RGBA;
	const rgb = value.match(/^rgb\((\d+) (\d+) (\d+)(?: \/ ([\d.]+))?\)$/);
	if (rgb) return [Number(rgb[1]), Number(rgb[2]), Number(rgb[3]), rgb[4] === undefined ? 1 : Number(rgb[4])];
	throw new Error(`cannot read colour ${value}`);
}

function over(top: RGBA, ground: RGBA): RGBA {
	const a = top[3];
	return [0, 1, 2].map((i) => top[i] * a + ground[i] * (1 - a)).concat(1) as RGBA;
}

function luminance([r, g, b]: RGBA): number {
	const ch = (c: number) => {
		const s = c / 255;
		return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
	};
	return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b);
}

function ratio(a: RGBA, b: RGBA): number {
	const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
	return (hi + 0.05) / (lo + 0.05);
}

/** fg on bg, by Harbour role name (without the leading dashes). */
type Pair = [fg: string, bg: string];

const cross = (fgs: string[], bgs: string[]): Pair[] => fgs.flatMap((fg) => bgs.map((bg): Pair => [fg, bg]));

// Words, 4.5:1.
const TEXT: Pair[] = [
	// Ink and links on every page ground: the page, a card, a sunken row, a hovered row.
	...cross(['ink', 'ink-2', 'ink-3', 'link', 'navy-text'], ['bg', 'surface', 'surface-2', 'hover']),
	['ink-3', 'surface-3'],
	['navy-text', 'navy-tint'],
	// A status pill: its word on its own tint; a reason line on a card.
	['run', 'run-tint'],
	['ok', 'ok-tint'],
	['fail', 'fail-tint'],
	['wait', 'wait-tint'],
	['await', 'await-tint'],
	['cancel', 'cancel-tint'],
	...cross(['run', 'ok', 'fail', 'wait', 'await', 'cancel'], ['surface', 'bg']),
	['ink-2', 'wait-tint'],
	['ink', 'fail-tint'],
	// The sidebar: its words, a hovered row, the active page's navy pill.
	...cross(['side-ink', 'side-ink-2', 'side-strong'], ['side', 'side-hover', 'side-field']),
	['side-active-ink', 'side-active'],
	['side-active-ink-2', 'side-active'],
	['run', 'run-tint'],
	// The one orange button, at rest and hovered; a confirm in a reason dialog.
	['on-accent', 'accent'],
	['on-accent', 'accent-hover'],
	['destroy-ink', 'destroy-bg'],
	['destroy-ink', 'destroy-hover'],
	// The tooltip: surface words on an ink ground.
	['surface', 'ink'],
	// A selected graph node and a selected item: act words on the act fill.
	['act-ink', 'act'],
	['act-ink-2', 'act'],
	// The log pane.
	['log-ink', 'log-bg'],
	['log-ink-2', 'log-bg'],
	['log-err', 'log-bg'],
	// The landing hero.
	...cross(['hero-ink', 'hero-ink-2'], ['hero', 'hero-2']),
	// Code: the editor and every code view, on the grounds code is drawn on.
	...cross(['syn-key', 'syn-str', 'syn-num', 'syn-lit', 'syn-tpl', 'syn-com'], ['surface', 'surface-2', 'bg', 'hover', 'navy-tint'])
];

// Marks, edges, meters and focus, 3:1.
const NON_TEXT: Pair[] = [
	// A control's edge and the focus ring on every ground they meet.
	...cross(['line-strong', 'focus'], ['bg', 'surface', 'surface-2']),
	['focus', 'side'],
	['side-field-line', 'side-field'],
	['select', 'surface'],
	// Status marks on a card and on their own pill.
	...cross(['run', 'ok-mark', 'fail-mark', 'wait-mark', 'await', 'cancel', 'pend-mark'], ['surface', 'bg']),
	['pend-mark', 'surface-2'],
	['wait-mark', 'wait-tint'],
	['ok-mark', 'ok-tint'],
	['fail-mark', 'fail-tint'],
	// Meters and the progress ring on their track.
	['meter', 'surface-3'],
	['wait-mark', 'surface-3'],
	['fail-mark', 'surface-3'],
	['ink-3', 'surface-3'],
	// The orange button's edge, and a confirm's fill, against the page.
	['accent-edge', 'bg'],
	['accent-edge', 'surface'],
	['destroy-bg', 'surface'],
	// Scrollbars: the thumb on its track, at rest and hovered.
	['sb-thumb', 'sb-track'],
	['sb-thumb-hover', 'sb-track'],
	['sb-log-thumb', 'log-bg'],
	['sb-log-thumb-hover', 'log-bg'],
	// The mark's orange on the navy hero.
	['mark-s', 'hero']
];

function check(theme: keyof typeof themes, fg: string, bg: string, floor: number, failures: string[]): void {
	if (NEVER_ADJACENT.some((n) => n.fg === fg && n.bg === bg)) return;
	const t = themes[theme];
	const ground = over(parse(`var(--${bg})`, t), parse('var(--bg)', t));
	const top = over(parse(`var(--${fg})`, t), ground);
	const r = ratio(top, ground);
	if (r < floor) failures.push(`${theme}: ${fg} on ${bg} is ${r.toFixed(2)}:1 (< ${floor})`);
}

test('the ratio is the WCAG one, and a translucent colour is composited first', () => {
	expect(ratio([0, 0, 0, 1], [255, 255, 255, 1]).toFixed(1)).toBe('21.0');
	expect(over([0, 0, 0, 0.5], [255, 255, 255, 1])[0]).toBe(127.5);
	expect(parse('var(--color-ink)', themes.light)).toEqual([14, 28, 40, 1]);
	expect(parse('var(--color-ink)', themes.dark)).toEqual([226, 233, 239, 1]);
});

test('the table covers at least the design’s 121 pairs', () => {
	expect(TEXT.length + NON_TEXT.length >= 121).toBe(true);
});

for (const theme of ['light', 'dark'] as const) {
	test(`${theme}: every word reads at 4.5:1 on the ground it sits on`, () => {
		const failures: string[] = [];
		for (const [fg, bg] of TEXT) check(theme, fg, bg, 4.5, failures);
		expect(failures).toEqual([]);
	});

	test(`${theme}: every mark, edge, meter and focus ring reaches 3:1`, () => {
		const failures: string[] = [];
		for (const [fg, bg] of NON_TEXT) check(theme, fg, bg, 3, failures);
		expect(failures).toEqual([]);
	});

	test(`${theme}: pin lines reach 3:1 on the page, a card and their own fill`, () => {
		const failures: string[] = [];
		for (const p of PIN_ACCENTS) {
			for (const bg of ['bg', 'surface', `color-${p}-fill`]) check(theme, `color-${p}-line`, bg, 3, failures);
		}
		expect(failures).toEqual([]);
	});
}
