import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { BUILD_PAGES, OPERATE_PAGES, buildPageOf, modeOf, operatePageOf, pageOf } from '../src/lib/navigation';
import { pageCommands } from '../src/lib/palette/commands';

// The app's map (#lib/navigation.ts) is the one list the top bar, the Operate
// strip, the drawer, the palette and the title block read. These hold it to
// the routes that exist.
const routes = `${import.meta.dir}/../src/routes`;

test('every admin page has exactly one Operate entry and a palette command', () => {
	const pages = readdirSync(`${routes}/admin`, { recursive: true })
		.filter((rel) => rel.endsWith('+page.svelte'))
		// One namespace's page is reached from its row, on the page that lists it.
		.filter((rel) => !rel.includes('['))
		.map((rel) => (rel === '+page.svelte' ? '/admin' : `/admin/${rel.replace(/\/\+page\.svelte$/, '')}`));
	expect(pages.length).toBe(12);
	const commands = pageCommands('default');
	for (const href of pages) {
		const entries = OPERATE_PAGES.filter((p) => p.href === href);
		expect(entries.length).toBe(1);
		expect(commands.some((c) => c.action.kind === 'navigate' && c.action.href.startsWith(`${href}?`))).toBe(true);
	}
	expect(OPERATE_PAGES.length).toBe(pages.length);
	expect(new Set(OPERATE_PAGES.map((p) => p.id)).size).toBe(OPERATE_PAGES.length);
});

test('every Build entry is a route, and Build is the dashboard, Runs, Orkflows and Create Orkflow', () => {
	expect(BUILD_PAGES.map((p) => p.href)).toEqual(['/', '/runs', '/orkflows', '/orkflows/+new']);
	for (const p of BUILD_PAGES) {
		const dir = p.href === '/' ? routes : `${routes}${p.href}`;
		expect(readdirSync(dir).includes('+page.svelte')).toBe(true);
	}
});

test('a path belongs to exactly one mode and one page', () => {
	const cases: [string, 'build' | 'operate', string][] = [
		['/', 'build', 'overview'],
		['/runs', 'build', 'runs'],
		['/runs/ork-1-2', 'build', 'runs'],
		['/runs/ork-1-2/compare', 'build', 'runs'],
		['/orkflows', 'build', 'orkflows'],
		['/orkflows/deploy', 'build', 'orkflows'],
		// Create Orkflow is a page of Orkflows: the sidebar marks Orkflows on it.
		['/orkflows/+new', 'build', 'orkflows'],
		['/admin', 'operate', 'overview'],
		['/admin/namespaces', 'operate', 'namespaces'],
		['/admin/image-policy', 'operate', 'image-policy'],
		['/admin/audit', 'operate', 'audit']
	];
	for (const [path, mode, id] of cases) {
		expect(modeOf(path)).toBe(mode);
		expect(pageOf(path)?.id).toBe(id);
		expect(mode === 'build' ? operatePageOf(path) : buildPageOf(path)).toBeUndefined();
	}
	// A prefix that is not a page boundary is not that page.
	expect(pageOf('/runsx')).toBeUndefined();
	expect(modeOf('/administrator')).toBe('build');
});

test('the root layout imports no run or Orkflow client statically (the palette loads them on first open)', () => {
	const layout = readFileSync(`${routes}/+layout.svelte`, 'utf8');
	const statics = [...layout.matchAll(/^\s*import\s[^;]*?from\s+['"]([^'"]+)['"]/gm)].map((m) => m[1]);
	expect(statics.length > 3).toBe(true);
	for (const from of statics) {
		expect(/clients\/(runs|orkflows)|palette\/(sources|CommandPalette)|CommandPalette/.test(from)).toBe(false);
	}
	expect(/import\(\s*['"]#lib\/components\/CommandPalette\.svelte['"]\s*\)/.test(layout)).toBe(true);
});

test('Create Orkflow sits on a segment no Orkflow name can take, so /orkflows/new is an Orkflow', () => {
	// An Orkflow name is ^[a-z][a-z0-9-]*$ (docs/DSL.md, validation rule 2);
	// the create page's segment must fail it, or an Orkflow of that name
	// could never be opened.
	const slug = /^[a-z][a-z0-9-]*$/;
	const create = BUILD_PAGES.find((p) => p.id === 'create-orkflow');
	expect(create?.href).toBe('/orkflows/+new');
	expect(slug.test(create!.href.split('/').pop()!)).toBe(false);
	// Every static directory beside [name] is one no Orkflow name matches.
	for (const dir of readdirSync(`${routes}/orkflows`)) {
		if (dir.startsWith('[') || dir.startsWith('+page') || dir.includes('.')) continue;
		expect(`${dir}: ${slug.test(dir) ? 'shadows an Orkflow name' : 'ok'}`).toBe(`${dir}: ok`);
	}
	expect(pageOf('/orkflows/new')?.id).toBe('orkflows');
});
