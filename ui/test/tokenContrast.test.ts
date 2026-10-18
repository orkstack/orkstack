import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { PIN_ACCENTS } from '../src/lib/runDetail';

// The WCAG 2.2 AA floors of the drawing office's colours, read from the
// ACTUAL tokens (ui/src/tokens.css), in both themes, as a full cross product
// rather than chosen pairs: a held row, a NOTE, a stamp and a pinned card each
// put the inks on a ground other than paper.
//
//   text      every text token on every ground            ≥ 4.5:1 (1.4.3)
//   on fills  on-ink on every fill a label sits on        ≥ 4.5:1
//   non-text  focus and rule-strong on every ground       ≥ 3:1   (1.4.11)
//             status solids/borders on paper, sheet, own ground
//             pin lines on paper, sheet and their own fill
//
// A pair is excused only by a NEVER_ADJACENT entry that says why it cannot
// occur. There are none.
const css = readFileSync(`${import.meta.dir}/../src/tokens.css`, 'utf8');

const NEVER_ADJACENT: { fg: string; bg: string; why: string }[] = [];

function block(opening: string): string {
	const start = css.indexOf(opening);
	if (start < 0) throw new Error(`tokens.css has no block ${opening}`);
	return css.slice(start, css.indexOf('\n}', start));
}

function decls(body: string): Map<string, string> {
	return new Map([...body.matchAll(/^\s*(--[a-z0-9-]+)\s*:\s*([^;]+);/gm)].map((m) => [m[1], m[2].trim()]));
}

const invariant = decls(block(':root {'));
const themes = {
	light: new Map([...invariant, ...decls(block(":root,\n:root[data-theme='light'] {"))]),
	dark: new Map([...invariant, ...decls(block(":root[data-theme='dark'] {"))])
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
		return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
	};
	return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b);
}

function ratio(a: RGBA, b: RGBA): number {
	const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
	return (hi + 0.05) / (lo + 0.05);
}

const STATUSES = ['pending', 'running', 'awaiting', 'succeeded', 'failed', 'canceled', 'skipped', 'unspecified'];
const PIN_FILLS = PIN_ACCENTS.map((p) => `${p}-fill`);
const GROUNDS = ['paper', 'paper-sunken', 'sheet', 'hover', 'blue-tint', 'red-tint', ...STATUSES.map((s) => `${s}-bg`), ...PIN_FILLS];
const TEXT = ['ink', 'ink-secondary', 'ink-muted', 'ink-placeholder', 'blue', 'blue-strong', 'red', 'red-strong', ...STATUSES.map((s) => `${s}-fg`)];
const FILLS = ['ink', 'ink-secondary', 'red', 'red-strong', 'blue', 'blue-strong'];

function check(theme: keyof typeof themes, fg: string, bg: string, floor: number, failures: string[]): void {
	if (NEVER_ADJACENT.some((n) => n.fg === fg && n.bg === bg)) return;
	const t = themes[theme];
	const ground = over(parse(`var(--color-${bg})`, t), parse('var(--color-paper)', t));
	const top = over(parse(`var(--color-${fg})`, t), ground);
	const r = ratio(top, ground);
	if (r < floor) failures.push(`${theme}: ${fg} on ${bg} is ${r.toFixed(2)}:1 (< ${floor})`);
}

test('the ratio is the WCAG one, and a translucent rule is composited first', () => {
	expect(ratio([0, 0, 0, 1], [255, 255, 255, 1]).toFixed(1)).toBe('21.0');
	expect(over([0, 0, 0, 0.5], [255, 255, 255, 1])[0]).toBe(127.5);
	expect(parse('var(--color-ink)', themes.light)).toEqual([20, 33, 61, 1]);
});

for (const theme of ['light', 'dark'] as const) {
	test(`${theme}: every status ground is one of the grounds`, () => {
		for (const s of STATUSES) expect(GROUNDS.includes(`${s}-bg`)).toBe(true);
	});

	test(`${theme}: every text token reads at 4.5:1 on every ground`, () => {
		const failures: string[] = [];
		for (const fg of TEXT) for (const bg of GROUNDS) check(theme, fg, bg, 4.5, failures);
		expect(failures).toEqual([]);
	});

	test(`${theme}: on-ink reads at 4.5:1 on every fill a label sits on`, () => {
		const failures: string[] = [];
		for (const bg of FILLS) check(theme, 'on-ink', bg, 4.5, failures);
		expect(failures).toEqual([]);
	});

	test(`${theme}: focus and strong rules reach 3:1 on every ground`, () => {
		const failures: string[] = [];
		for (const bg of GROUNDS) {
			check(theme, 'focus', bg, 3, failures);
			check(theme, 'rule-strong', bg, 3, failures);
		}
		expect(failures).toEqual([]);
	});

	test(`${theme}: status marks and pin lines reach 3:1 where they are drawn`, () => {
		const failures: string[] = [];
		for (const s of STATUSES) {
			for (const part of ['solid', 'border']) {
				for (const bg of ['paper', 'sheet', `${s}-bg`]) check(theme, `${s}-${part}`, bg, 3, failures);
			}
		}
		for (const p of PIN_ACCENTS) {
			for (const bg of ['paper', 'sheet', `${p}-fill`]) check(theme, `${p}-line`, bg, 3, failures);
		}
		expect(failures).toEqual([]);
	});
}
