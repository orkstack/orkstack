// After the build: every internal link of every page the docs' link
// validator does not see (the landing page, the 404 page) must resolve to a
// built file, and its #anchor to an id on that page. starlight-links-validator
// checks the docs themselves during the build.
import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import path from 'node:path';
import { siteRoot } from './tokens.mjs';
import { BASE } from '../site.config.mjs';

const dist = path.join(siteRoot, 'dist');
if (!existsSync(dist)) {
	console.error('check-links: no dist/; run the build first');
	process.exit(1);
}

function pages(dir, out = []) {
	for (const name of readdirSync(dir)) {
		const abs = path.join(dir, name);
		if (statSync(abs).isDirectory()) {
			if (name === 'docs' || name === 'pagefind' || name === '_astro') continue;
			pages(abs, out);
		} else if (name.endsWith('.html')) out.push(abs);
	}
	return out;
}

const idsCache = new Map();
function ids(file) {
	if (!idsCache.has(file)) {
		const html = readFileSync(file, 'utf8');
		idsCache.set(file, new Set([...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1])));
	}
	return idsCache.get(file);
}

const problems = [];
const checked = pages(dist);
for (const file of checked) {
	const html = readFileSync(file, 'utf8');
	for (const m of html.matchAll(/\s(?:href|src|poster)="([^"]+)"/g)) {
		const raw = m[1].replace(/&amp;/g, '&');
		if (!raw.startsWith(BASE + '/') && raw !== BASE && !raw.startsWith('#')) continue;
		const [pathname, hash] = raw.split('#');
		const target = pathname
			? path.join(dist, decodeURIComponent(pathname.slice(BASE.length)).split('?')[0])
			: file;
		const resolved = existsSync(target) && statSync(target).isDirectory() ? path.join(target, 'index.html') : target;
		const where = path.relative(dist, file);
		if (!existsSync(resolved)) {
			problems.push(`${where}: ${raw} does not resolve to a built file`);
			continue;
		}
		if (hash && resolved.endsWith('.html') && !ids(resolved).has(decodeURIComponent(hash))) {
			problems.push(`${where}: ${raw} names #${hash}, which ${path.relative(dist, resolved)} does not have`);
		}
	}
}

if (problems.length) {
	console.error(`check-links: ${problems.length} broken link(s)\n  ${problems.join('\n  ')}`);
	process.exit(1);
}
console.log(`check-links: every internal link of ${checked.length} page(s) resolves`);
