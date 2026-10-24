import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';

// The colour names pages used before the drawing office (surface, fg,
// brand-600, danger-700, console-bg, …) are gone: every page is drawn in the
// world's own tokens, and tokens.css no longer carries the alias block that
// pointed those names at them. This test keeps it so — no source may spell a
// legacy colour name, and the alias block may not come back.
//
// `--color-fg` is the one permanent alias (run-page-pins.spec.ts reads it by
// name); it stays in tokens.css, but the utilities spelled with it
// (`text-fg`) are legacy and are refused like the rest.

const src = `${import.meta.dir}/../src`;

const LEGACY =
	'canvas|surface-raised|surface-sunken|surface-hover|surface|border-subtle|border-strong|border|fg-secondary|fg-muted|fg-placeholder|fg-inverse|fg|(?:brand|danger|success|warning|info)-\\d+|console-[a-z]+';
const PREFIX = 'bg|text|border(?:-[xytrblse])?|ring|outline|fill|stroke|divide|decoration|accent|caret|placeholder|from|via|to|shadow';

export function legacyUses(text: string): string[] {
	const utility = new RegExp(`(?<![\\w-])(?:[a-z0-9-]+:)*(?:${PREFIX})-(?:${LEGACY})(?:/\\d+)?(?![\\w-])`, 'g');
	const variable = new RegExp(`var\\(--color-(?:${LEGACY})\\)`, 'g');
	return [...text.matchAll(utility), ...text.matchAll(variable)].map((m) => m[0]);
}

test('fixtures: legacy names are counted, the world’s own are not', () => {
	expect(legacyUses('class="bg-surface text-fg-muted hover:border-border-strong bg-brand-600/40 text-console-fg" style="color: var(--color-fg)"')).toHaveLength(6);
	expect(legacyUses('class="border bg-paper text-ink border-rule border-t text-red-strong bg-failed-bg text-2xs"')).toEqual([]);
});

test('no source spells a legacy colour name', () => {
	const files = readdirSync(src, { recursive: true })
		.map((rel) => `${src}/${rel}`)
		.filter((p) => /\.(svelte|ts|css|html)$/.test(p) && !p.includes('/lib/gen/') && !/\/(tokens|tailwind-tokens)\.css$/.test(p));
	let total = 0;
	const byFile: Record<string, number> = {};
	for (const path of files) {
		const n = legacyUses(readFileSync(path, 'utf8')).length;
		if (n) byFile[path.slice(src.length + 1)] = n;
		total += n;
	}
	expect(byFile).toEqual({});
	expect(total).toBe(0);
});

test('the alias block is gone, and --color-fg is the one alias left', () => {
	for (const file of ['tokens.css', 'tailwind-tokens.css']) {
		const text = readFileSync(`${src}/${file}`, 'utf8');
		expect(text.includes('LEGACY ALIASES')).toBe(false);
		expect(text.match(new RegExp(`--color-(?:${LEGACY})\\s*:`, 'g')) ?? []).toEqual(['--color-fg:']);
	}
});
