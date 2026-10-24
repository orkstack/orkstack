// After the build: every internal link of every page the docs' link
// validator does not see (the landing page, the 404 page) must resolve to a
// built file, and its #anchor to an id on that page. A root-relative or
// relative link that does not carry the site's base is refused too: on
// GitHub Pages it would point outside the site. starlight-links-validator
// checks the docs themselves during the build.
import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import path from 'node:path';
import { siteRoot } from './tokens.mjs';
import { BASE_PATH } from '../site.config.mjs';

const dist = path.join(siteRoot, 'dist');
if (!existsSync(dist)) {
	console.error('check-links: no dist/; run the build first');
	process.exit(1);
}

function pages(dir, out = []) {
	for (const name of readdirSync(dir)) {
		const abs = path.join(dir, name);
		if (statSync(abs).isDirectory()) {
			if (dir === dist && (name === 'docs' || name === 'pagefind' || name === '_astro')) continue;
			pages(abs, out);
		} else if (name.endsWith('.html')) out.push(abs);
	}
	return out;
}

const idsCache = new Map();
function ids(file) {
	if (!idsCache.has(file)) {
		const html = readFileSync(file, 'utf8');
		idsCache.set(file, new Set([...html.matchAll(/\sid=(["'])(.*?)\1/g)].map((m) => m[2])));
	}
	return idsCache.get(file);
}

const decode = (s) => s.replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'");
const problems = [];
const checked = pages(dist);
if (!checked.includes(path.join(dist, 'index.html'))) problems.push('the landing page (index.html) was not built');
for (const file of checked) {
	const html = readFileSync(file, 'utf8');
	const where = path.relative(dist, file);
	for (const m of html.matchAll(/\s(?:href|src|poster)=(["'])(.*?)\1/g)) {
		const raw = decode(m[2]);
		if (/^[a-z][a-z0-9+.-]*:/i.test(raw) || raw.startsWith('//') || raw.startsWith('data:')) continue;
		const [pathname, hash] = raw.split('#');
		let target;
		if (!pathname) target = file;
		else if (pathname === BASE_PATH || pathname.startsWith(`${BASE_PATH}/`)) {
			target = path.join(dist, decodeURIComponent(pathname.slice(BASE_PATH.length)).split('?')[0]);
		} else {
			problems.push(`${where}: ${raw} does not carry the site's base (${BASE_PATH || '/'})`);
			continue;
		}
		const resolved = existsSync(target) && statSync(target).isDirectory() ? path.join(target, 'index.html') : target;
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
	console.error(`check-links: ${problems.length} problem(s)\n  ${problems.join('\n  ')}`);
	process.exit(1);
}
console.log(`check-links: every internal link of ${checked.length} page(s) resolves`);
