import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';

// The UI revamp's only staged token debt: the colour names pages used before
// the drawing office (surface, fg, brand-600, danger-700, console-bg, …).
// tokens.css points each at a token of the world (its LEGACY ALIASES block)
// so a page not yet redrawn still renders in it; this test holds their USE to
// a ceiling that each stage lowers as it redraws its routes, and that reaches
// zero — with the alias block deleted — when the last stage lands.
//
// `--color-fg` is the one permanent alias (a spec reads it by name); the
// utilities spelled with it (`text-fg`) are still legacy and still counted.

/** Stage 1 (foundation) left 2,080; Stage 2 (runs) redrew the run pages and their components. */
const CEILING = 1569;

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

test('legacy colour names stay within this stage’s ceiling', () => {
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
	if (total > CEILING) console.log(JSON.stringify(byFile, null, 1));
	expect(total <= CEILING).toBe(true);
});

test('the alias block exists while the ceiling is above zero, and goes with it', () => {
	const tokens = readFileSync(`${src}/tokens.css`, 'utf8');
	expect(tokens.includes('LEGACY ALIASES')).toBe(CEILING > 0);
});
