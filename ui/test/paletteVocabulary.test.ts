import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { BUILD_PAGES, OPERATE_PAGES, operateScope } from '../src/lib/navigation';
import { namespaceCommands, openRunCommand, pageCommands, themeCommands } from '../src/lib/palette/commands';

// The words the new shell puts in front of a person (AGENTS.md §7, and the
// product's own vocabulary in ui/PRODUCT.md): no Temporal mechanism, and none
// of the drawing office's DEVICE names — sheet, title block, drawing,
// revision — which are this redesign's names for components, never the
// product's words for anything. Strings are checked, identifiers are not:
// `TitleBlock.svelte`, `run-drawing-unavailable` and `SheetHeader` are fine.

const BANNED = [
	'queue',
	'queues',
	'session',
	'sessions',
	'generation',
	'generations',
	'checkpoint',
	'checkpoints',
	'task queue',
	'workflow id',
	'signal',
	'signals',
	'timer',
	'timers',
	'child workflow',
	'marker',
	'search attribute',
	'rev',
	'revision',
	'revisions',
	'started by',
	'sheet',
	'sheets',
	'title block',
	'drawing',
	'superseded'
];

export function bannedWords(text: string): string[] {
	const t = text.toLowerCase();
	return BANNED.filter((w) => new RegExp(`(?<![\\w-])${w.replace(/ /g, '\\s+')}(?![\\w-])`).test(t));
}

/** The strings a component renders: its text nodes and its accessible-name attributes. */
export function renderedStrings(svelte: string): string[] {
	const markup = svelte
		.replace(/<script[\s\S]*?<\/script>/g, ' ')
		.replace(/<style[\s\S]*?<\/style>/g, ' ')
		.replace(/<!--[\s\S]*?-->/g, ' ');
	const out: string[] = [];
	for (const m of markup.matchAll(/(?:aria-label|title|placeholder|alt)="([^"{]*)"/g)) out.push(m[1]);
	// Text between tags, with {expressions} removed.
	for (const m of markup.matchAll(/>([^<>]+)</g)) {
		const text = m[1].replace(/\{[^}]*\}/g, ' ').trim();
		if (text) out.push(text);
	}
	// String literals inside {expressions} in the markup: `{busy ? 'Working…' : label}`.
	for (const m of markup.matchAll(/\{[^}]*\}/g)) for (const s of m[0].matchAll(/'([^']*)'|"([^"]*)"|`([^`]*)`/g)) out.push(s[1] ?? s[2] ?? s[3]);
	return out.filter((s) => /[a-z]/i.test(s));
}

test('the checker catches each banned word and passes plain English and identifiers', () => {
	for (const s of ['Skip to the sheet', 'Sheet details', 'SHEET', 'Drawing', 'REV 0.4.2', 'Revision cloud', 'Started by', 'the task queue', 'SUPERSEDED', 'Title block']) {
		expect(bannedWords(s).length > 0).toBe(true);
	}
	for (const s of ['Draw all', 'drawn by hand', 'Skip to content', 'Page summary', 'Page details', 'Laying out the task map…', 'Version 0.4.2', 'Origin']) {
		expect(bannedWords(s)).toEqual([]);
	}
	expect(renderedStrings('<script>const x = "sheet";</script><p data-testid="run-drawing-unavailable" class="sheet">Hello {a ? \'Draw all\' : "x"}</p>')).toEqual(['Hello', 'Draw all', 'x']);
});

test('no static palette command says a banned word', () => {
	const labels = [
		...pageCommands('default'),
		...namespaceCommands(['default', 'payments'], 'default'),
		...themeCommands('system'),
		openRunCommand('ork-1', 'default')!
	].flatMap((c) => [c.label, c.context ?? '', c.keywords ?? '']);
	const found = labels.flatMap((l) => bannedWords(l).map((w) => `${l}: ${w}`));
	expect(found).toEqual([]);
});

test('no page name, mode or title-block label says a banned word', () => {
	const words = [
		...BUILD_PAGES.flatMap((p) => [p.label, p.title]),
		...OPERATE_PAGES.flatMap((p) => [p.label, p.title, operateScope(p.id)]),
		'PAGE',
		'NAMESPACE',
		'SCOPE',
		'Build',
		'Operate'
	];
	expect(words.flatMap((w) => bannedWords(w).map((b) => `${w}: ${b}`))).toEqual([]);
});

test('the shell and its new components render no banned word', () => {
	const files = [
		'routes/+layout.svelte',
		'routes/admin/+layout.svelte',
		'lib/components/TopBar.svelte',
		'lib/components/TitleBlock.svelte',
		'lib/components/SheetHeader.svelte',
		'lib/components/ThemeSwitch.svelte',
		'lib/components/CommandPalette.svelte',
		'lib/components/MenuDrawer.svelte',
		'lib/components/StatusPill.svelte'
	];
	const found: string[] = [];
	for (const f of files) {
		const strings = renderedStrings(readFileSync(`${import.meta.dir}/../src/${f}`, 'utf8'));
		expect(strings.length > 0 || f.endsWith('StatusPill.svelte')).toBe(true);
		for (const s of strings) for (const w of bannedWords(s)) found.push(`${f}: "${s}" (${w})`);
	}
	expect(found).toEqual([]);
	// The landmark and the skip link carry the product's words.
	const layout = readFileSync(`${import.meta.dir}/../src/routes/+layout.svelte`, 'utf8');
	expect(layout.includes('Skip to content')).toBe(true);
	const block = readFileSync(`${import.meta.dir}/../src/lib/components/TitleBlock.svelte`, 'utf8');
	expect(block.includes('aria-label="Page summary"')).toBe(true);
	expect(block.includes('Page details')).toBe(true);
});
