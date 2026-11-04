// After the build: what a visitor must find on the built pages, read from the
// HTML they are served. Run by `bun run build`, so the site gate holds it.
//
//   - the landing page says what it is, offers the way in, and shows the three
//     recordings: planned, or a player that never plays by itself;
//   - no video anywhere autoplays, and each has controls, preload none, and,
//     for a recording of the UI, captions;
//   - the docs show the ten sections in their order, the current one open;
//   - every configuration setting has its entry on All settings;
//   - every old route redirects to where its page is now;
//   - the landing page uses no native title and one orange button.
import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import path from 'node:path';
import { siteRoot, repoRoot } from './tokens.mjs';
import { parseSettings, settingId } from './settings.mjs';
import { BASE_PATH } from '../site.config.mjs';

const dist = path.join(siteRoot, 'dist');
if (!existsSync(dist)) {
	console.error('check-site: no dist/; run the build first');
	process.exit(1);
}
const problems = [];
const page = (route) => {
	const file = path.join(dist, route, 'index.html');
	if (!existsSync(file)) {
		problems.push(`${route || '/'} was not built`);
		return '';
	}
	return readFileSync(file, 'utf8');
};
const manifest = JSON.parse(readFileSync(path.join(siteRoot, 'docs.manifest.json'), 'utf8'));

// ---------------------------------------------------------- the landing page
const home = page('');
for (const words of ['Durable workflows without workflow code.', 'Watch it work', 'Files in, files out, exit.', 'Run your first Orkflow on your machine']) {
	if (!home.includes(words)) problems.push(`the landing page no longer says "${words}"`);
}
if (!home.includes(`href="${BASE_PATH}/docs/get-started/run-locally/"`)) problems.push('the landing page has no way to get started');
if (!/id="reel-title"/.test(home)) problems.push('the landing page lost the anchor README.md links to (#reel-title)');
if (/<[a-z][^>]*\stitle=/i.test(home.replace(/<svg[\s\S]*?<\/svg>/g, ''))) problems.push('the landing page uses a native title: use data-tip');
const primary = home.match(/class="btn primary"/g) ?? [];
if (primary.length !== 1) problems.push(`the landing page has ${primary.length} orange buttons, the design one: Get started`);
const slots = home.match(/<figure class="video"/g) ?? [];
if (slots.length !== 3) problems.push(`the landing page shows ${slots.length} recordings, the design three`);
const planned = (home.match(/Recording planned/g) ?? []).length;
const players = (home.match(/<video/g) ?? []).length;
if (planned + players / 2 !== 3) problems.push(`of three recordings, ${planned} say they are planned and ${players / 2} play (each has two themes)`);
for (const m of home.matchAll(/<video[\s\S]*?<\/video>/g)) {
	if (!/<track[^>]+kind="captions"/.test(m[0])) problems.push('a recording on the landing page has no captions track');
}

// ---------------------------------------------------- every video of the site
const htmlFiles = (dir, out = []) => {
	for (const name of readdirSync(dir)) {
		const abs = path.join(dir, name);
		if (statSync(abs).isDirectory()) {
			if (name !== 'pagefind' && name !== '_astro') htmlFiles(abs, out);
		} else if (name.endsWith('.html')) out.push(abs);
	}
	return out;
};
for (const file of htmlFiles(dist)) {
	const html = readFileSync(file, 'utf8');
	for (const m of html.matchAll(/<video\b[^>]*>[\s\S]*?<\/video>/g)) {
		const where = path.relative(dist, file);
		const open = m[0].slice(0, m[0].indexOf('>') + 1);
		if (/\bautoplay\b/.test(open)) problems.push(`${where}: a video autoplays`);
		if (!/\bcontrols\b/.test(open)) problems.push(`${where}: a video has no controls`);
		if (!/preload="none"/.test(open)) problems.push(`${where}: a video loads before it is asked to`);
		if (!/<track[^>]+kind="captions"/.test(m[0])) problems.push(`${where}: a video has no captions`);
	}
}

// ---------------------------------------------------------------- the docs
const sections = manifest.groups.filter((g) => !g.hidden).map((g) => g.label);
const want = ['Get started', 'Build Orkflows', 'Activities', 'Run and observe', 'Operate', 'Deploy', 'Configuration', 'Examples', 'Reference', 'Concepts'];
if (sections.join() !== want.join()) problems.push(`the docs' sections are ${sections.join(', ')}; the design has ${want.join(', ')}`);
const docsHome = page('docs');
const summaries = [...docsHome.matchAll(/<summary[^>]*>[\s\S]*?class="large[^"]*"[^>]*>([^<]+)</g)].map((m) => m[1].trim());
if (summaries.join() !== want.join()) problems.push(`the sidebar shows ${summaries.join(', ')}`);
const cardCount = (docsHome.match(/class="card ia-card"/g) ?? []).length;
if (cardCount !== 10) problems.push(`the docs home has ${cardCount} Find your way cards, not ten`);
const inGroup = page('docs/build/fan-out');
const openGroup = /<details open[^>]*>\s*<summary[^>]*>[\s\S]*?class="large[^"]*"[^>]*>Build Orkflows</.test(inGroup);
if (!openGroup) problems.push('a page in Build Orkflows does not open its own sidebar group');
if (!inGroup.includes('aria-label="Breadcrumb"')) problems.push('a docs page lost its breadcrumb');
if (!docsHome.includes('Search the docs')) problems.push('the docs header no longer offers "Search the docs"');

// ------------------------------------------------------------- the settings
const allSettings = page('docs/configuration');
const rows = parseSettings(readFileSync(path.join(repoRoot, 'docs/DEPLOY.md'), 'utf8'));
for (const row of rows) {
	if (!allSettings.includes(`id="${settingId(row)}"`)) problems.push(`All settings has no entry for ${row.keys[0] ?? row.envs[0]}`);
}
if (allSettings.includes('## ')) problems.push('All settings prints markdown as text: a block lost its blank line');
for (const route of ['docs/configuration', 'docs/reference/cli', 'docs/reference/api', 'docs/reference/reasons', 'docs/reference/changelog']) {
	if (!page(route).includes('Generated from the source, always current')) problems.push(`${route} does not say it is generated from the source`);
}

// ------------------------------------------------------------ the old routes
for (const group of manifest.groups) {
	for (const p of group.pages) {
		for (const alias of p.aliases ?? []) {
			const html = page(alias);
			if (html && !html.includes(`url=${BASE_PATH}/${p.route}/`)) problems.push(`${alias} does not redirect to ${p.route}`);
		}
	}
}

if (problems.length) {
	console.error(`check-site: ${problems.length} problem(s)\n  ${problems.join('\n  ')}`);
	process.exit(1);
}
console.log(`check-site: the landing page, the ${sections.length} sections, ${rows.length} settings and every old route hold`);
