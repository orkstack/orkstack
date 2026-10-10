import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

// The colour tokens are design decisions with a measurable floor: WCAG 2.x AA
// asks 4.5:1 of text, placeholder text included. These read the ACTUAL
// stylesheet, so an edit that drops a token below the floor fails here rather
// than in an accessibility audit.
const css = readFileSync(`${import.meta.dir}/../src/app.css`, 'utf8');

function block(selector: string): string {
	const start = css.indexOf(`${selector} {`);
	if (start < 0) throw new Error(`app.css has no ${selector} block`);
	const end = css.indexOf('\n}', start);
	return css.slice(start, end);
}

function token(body: string, name: string): string {
	const match = body.match(new RegExp(`--${name}:\\s*(#[0-9a-fA-F]{6})\\s*;`));
	if (!match) throw new Error(`no --${name} in block`);
	return match[1];
}

function luminance(hex: string): number {
	const channel = (i: number) => {
		const c = parseInt(hex.slice(1 + 2 * i, 3 + 2 * i), 16) / 255;
		return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
	};
	return 0.2126 * channel(0) + 0.7152 * channel(1) + 0.0722 * channel(2);
}

function contrast(a: string, b: string): number {
	const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
	return (hi + 0.05) / (lo + 0.05);
}

test('the ratio is the WCAG one', () => {
	expect(contrast('#000000', '#ffffff').toFixed(1)).toBe('21.0');
	expect(contrast('#ffffff', '#ffffff').toFixed(1)).toBe('1.0');
});

test('dark placeholder text reads at 4.5:1 or better on the input surface', () => {
	const dark = block(":root[data-theme='dark']");
	// Every placeholder site renders on bg-surface (ListFilter, the submit
	// panel, the cache filters, the apply editor).
	const ratio = contrast(token(dark, 'color-fg-placeholder'), token(dark, 'color-surface'));
	expect(ratio >= 4.5).toBe(true);
});

test('light placeholder text reads at 4.5:1 or better on the input surface', () => {
	const light = block('@theme');
	const ratio = contrast(token(light, 'color-fg-placeholder'), token(light, 'color-surface'));
	expect(ratio >= 4.5).toBe(true);
});
