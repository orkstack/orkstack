// Builds the docs collection: the pages written for the site (site/content/)
// and the generated references ported from the repository as they are.
//
// docs.manifest.json says which page comes from which file (or which
// headings of it), and this script writes src/content/docs/docs/ from it on
// every build. That directory is generated and ignored by git. A page
// written for the site summarises repository docs for a reader; the
// repository's docs stay the full reference, and each site page cites the
// ones it summarises. Along the way it:
//
//   - gives each page the title and description the manifest states, and an
//     edit link to the file it came from;
//   - rewrites every link between repository files: a link to a ported file
//     becomes that page's route, its #anchor following the heading to
//     whichever page holds it; a link to anything else in the repository
//     becomes its GitHub URL; a link to a file that does not exist fails;
//   - replaces a <!-- site-media: NAME --> marker (invisible on GitHub) with
//     NAME's light and dark screenshots, and a <!-- site-diagram: NAME -->
//     marker with docs/images/NAME-{light,dark}.svg (scripts/readme-diagrams.mjs);
//   - writes one page per example Orkflow, with its YAML read from the file;
//   - fails when a page written for the site cites no repository doc, or
//     cites a file or heading that does not exist (its See also links them on
//     GitHub), and when a doc under docs/ is cited by no page, so a new doc
//     cannot be forgotten;
//   - writes src/styles/generated/nojs-theme.css from ui/src/tokens.css.
//
// starlight-links-validator then fails the build on any link that still does
// not resolve.
import { readFileSync, writeFileSync, mkdirSync, rmSync, existsSync, statSync, readdirSync } from 'node:fs';
import path from 'node:path';
import GithubSlugger from 'github-slugger';
import YAML from 'yaml';
import { BASE_PATH, REPO, BRANCH, repoUrl } from '../site.config.mjs';
import { siteRoot, repoRoot, readTokens, darkMediaCss, resolve, codeTheme } from './tokens.mjs';
import { checkAnchor, checkCitations, leftoverMarkers, uncitedDocs } from './citations.mjs';

const manifest = JSON.parse(readFileSync(path.join(siteRoot, 'docs.manifest.json'), 'utf8'));
const outRoot = path.join(siteRoot, 'src', 'content', 'docs');
const mediaDir = path.join(siteRoot, 'src', 'assets', 'media');

const problems = [];
const fail = (msg) => problems.push(msg);

// ---------------------------------------------------------------- reading

const read = (rel) => {
	const abs = path.join(repoRoot, rel);
	if (!existsSync(abs)) throw new Error(`docs.manifest.json names ${rel}, which does not exist`);
	return readFileSync(abs, 'utf8');
};

/** Lines of a markdown file, each marked as inside a code fence or not. */
function scan(text) {
	const lines = text.split('\n');
	const marks = [];
	let fence = null;
	for (const line of lines) {
		const m = /^\s{0,3}(`{3,}|~{3,})/.exec(line);
		if (fence) {
			marks.push(true);
			// A closing fence: the same character, at least as long, alone.
			const close = /^\s{0,3}(`{3,}|~{3,})\s*$/.exec(line);
			if (close && close[1][0] === fence[0] && close[1].length >= fence.length) fence = null;
		} else if (m) {
			fence = m[1];
			marks.push(true);
		} else marks.push(false);
	}
	return { lines, inCode: marks };
}

/** The plain text GitHub slugs a heading from. */
function headingText(raw) {
	return raw
		.replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
		.replace(/<[^>]+>/g, '')
		.replace(/\*\*|__/g, '')
		.trim();
}

function headings(text) {
	const { lines, inCode } = scan(text);
	const out = [];
	lines.forEach((line, i) => {
		if (inCode[i]) return;
		const m = /^(#{1,6})\s+(.*?)\s*#*\s*$/.exec(line);
		if (m) out.push({ line: i, level: m[1].length, raw: line.trim(), text: headingText(m[2]) });
	});
	return out;
}

/** The [start, end) line range of the section a heading opens. */
function sectionRange(hs, lines, h) {
	const next = hs.find((o) => o.line > h.line && o.level <= h.level);
	return [h.line, next ? next.line : lines.length];
}

/** Select a part's lines from its source, per include / omit / from-to. */
function selectLines(part, text) {
	const { lines } = scan(text);
	const hs = headings(text);
	let keep = lines.map(() => true);
	const find = (raw) => {
		const h = hs.find((o) => o.raw === raw);
		if (!h) throw new Error(`${part.source}: no heading ${JSON.stringify(raw)}`);
		return h;
	};
	if (part.include) {
		keep = lines.map(() => false);
		for (const raw of part.include) {
			const [a, b] = sectionRange(hs, lines, find(raw));
			for (let i = a; i < b; i++) keep[i] = true;
		}
	}
	if (part.omit) {
		for (const raw of part.omit) {
			const [a, b] = sectionRange(hs, lines, find(raw));
			for (let i = a; i < b; i++) keep[i] = false;
		}
	}
	if (part.from) {
		const a = lines.findIndex((l) => l.startsWith(part.from));
		const b = part.to ? lines.findIndex((l, i) => i > a && l.startsWith(part.to)) : lines.length;
		if (a < 0 || b < 0) throw new Error(`${part.source}: cannot find the range ${part.from} … ${part.to}`);
		keep = lines.map((_, i) => i >= a && i < b);
	}
	return keep;
}

// ------------------------------------------------------------ the pages

function flatten(groups, out = []) {
	for (const g of groups) {
		for (const p of g.pages) {
			if (p.pages) flatten([p], out);
			else out.push(p);
		}
	}
	return out;
}
const pages = flatten(manifest.groups);

// Example Orkflows: one generated page each.
const exampleFiles = [];
for (const page of pages.filter((p) => p.examples)) {
	const dirs = ['examples', 'examples/automation'];
	for (const dir of dirs) {
		for (const name of readdirSync(path.join(repoRoot, dir)).sort()) {
			if (!name.endsWith('.ork.yaml')) continue;
			const rel = `${dir}/${name}`;
			const id = rel.replace(/^examples\//, '').replace(/\.ork\.yaml$/, '').replace(/\//g, '-');
			const doc = YAML.parseDocument(readFileSync(path.join(repoRoot, rel), 'utf8'));
			const kind = doc.get('kind');
			const description =
				doc.get('description') ||
				(kind === 'Automation' ? `The ${doc.get('namespace') ?? 'example'} namespace's automation document.` : '');
			if (!description) fail(`${rel} has no description: for its page`);
			exampleFiles.push({ rel, id, route: `${page.examples}/${id}`, title: rel.replace(/^examples\//, ''), description, kind });
		}
	}
}

// Every source file, and the page(s) holding each of its headings.
// sourceIndex: rel -> { whole: route|null, slugs: Map(sourceSlug -> {route, pageSlug}) }
const sourceIndex = new Map();
const pageBodies = new Map();

function indexFor(rel) {
	if (!sourceIndex.has(rel)) sourceIndex.set(rel, { whole: null, first: null, slugs: new Map(), all: null });
	return sourceIndex.get(rel);
}

for (const page of pages) {
	const pageSlugger = new GithubSlugger();
	const chunks = [];
	page.parts.forEach((part, index) => {
		if (part.append) {
			const body = read(part.append).replace(/\n+$/, '');
			const ticks = '`'.repeat(Math.max(3, ...[...body.matchAll(/`+/g)].map((m) => m[0].length + 1)));
			const heading = part.heading ?? path.basename(part.append);
			chunks.push({ text: `## ${heading}\n\n${ticks}${part.lang ?? ''} title="${part.append}"\n${body}\n${ticks}\n`, source: null });
			pageSlugger.slug(heading);
			return;
		}
		const text = read(part.source);
		const keep = selectLines(part, text);
		const whole = !part.include && !part.omit && !part.from;
		const { lines } = scan(text);
		const hs = headings(text);
		const srcSlugger = new GithubSlugger();
		const srcSlugs = new Map(hs.map((h) => [h.line, srcSlugger.slug(h.text)]));
		const idx = indexFor(part.source);
		if (!idx.all) idx.all = new Set(srcSlugs.values());
		if (whole && !idx.whole) idx.whole = page.route;
		if (!idx.first) idx.first = page.route;
		// Drop the file's own title: the page title is the h1. A later
		// whole-file part keeps its title one level down, as a section.
		const firstH1 = hs.find((h) => h.level === 1);
		const demote = index > 0 && whole ? 1 : 0;
		const out = [];
		lines.forEach((line, i) => {
			if (!keep[i]) return;
			const h = hs.find((o) => o.line === i);
			if (h && h === firstH1 && index === 0) return;
			if (h) {
				const level = Math.min(6, h.level + demote);
				const pageSlug = pageSlugger.slug(h.text);
				const src = srcSlugs.get(i);
				if (!idx.slugs.has(src) || whole) idx.slugs.set(src, { route: page.route, pageSlug });
				out.push('#'.repeat(level) + line.trim().replace(/^#+/, ''));
				return;
			}
			out.push(line);
		});
		chunks.push({ text: out.join('\n').replace(/^\n+/, ''), source: part.source });
	});
	pageBodies.set(page.route, chunks);
}

for (const ex of exampleFiles) {
	const idx = indexFor(ex.rel);
	idx.whole = ex.route;
	idx.first = ex.route;
	idx.all = new Set();
}

// ------------------------------------------------------------ links

const url = (r, anchor) => `${BASE_PATH}/${r}/${anchor ? `#${anchor}` : ''}`;

function rewriteTarget(target, fromRel) {
	if (/^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith('//')) return target;
	// The site's own pages (site/content/) link by route, which the base
	// prefixes; every repository doc links by repository path.
	if (fromRel.startsWith('site/content/') && target.startsWith('/')) return BASE_PATH + target;
	const hashAt = target.indexOf('#');
	const pathPart = hashAt >= 0 ? target.slice(0, hashAt) : target;
	let anchor = '';
	try {
		anchor = hashAt >= 0 ? decodeURIComponent(target.slice(hashAt + 1)) : '';
	} catch {
		fail(`${fromRel}: link ${target} has a malformed anchor`);
		return target;
	}
	let rel = fromRel;
	if (pathPart) {
		if (pathPart.startsWith('/')) rel = pathPart.slice(1);
		else rel = path.posix.normalize(path.posix.join(path.posix.dirname(fromRel), pathPart));
		rel = rel.replace(/\/$/, '');
	}
	if (rel === '..' || rel.startsWith('../') || path.posix.isAbsolute(rel)) {
		fail(`${fromRel}: link to ${target} leaves the repository`);
		return target;
	}
	const idx = sourceIndex.get(rel);
	if (idx) {
		if (!anchor) return url(idx.whole ?? idx.first);
		const hit = idx.slugs.get(anchor);
		if (!hit) {
			// A heading of a file the site ports only in part: the full
			// file on GitHub holds it.
			if (idx.all?.has(anchor)) return `${repoUrl(rel)}#${anchor}`;
			fail(`${fromRel}: link to ${rel}#${anchor}, which has no such heading`);
			return target;
		}
		return url(hit.route, hit.pageSlug);
	}
	if (rel === fromRel && !pathPart) return target;
	const abs = path.join(repoRoot, rel);
	if (!existsSync(abs)) {
		fail(`${fromRel}: link to ${rel}, which does not exist`);
		return target;
	}
	const dir = statSync(abs).isDirectory();
	// A heading of a repository doc the site does not port: the doc on
	// GitHub must still have it.
	const anchorProblem = dir ? null : checkAnchor(repoRoot, rel, anchor);
	if (anchorProblem) fail(`${fromRel}: link to ${rel}#${anchor}, ${anchorProblem}`);
	return repoUrl(rel, { dir }) + (anchor ? `#${anchor}` : '');
}

/** The [start, end) character ranges of a chunk that are code. */
function codeRanges(text) {
	const { lines, inCode } = scan(text);
	const ranges = [];
	let at = 0;
	let prose = '';
	let proseStart = 0;
	const flushProse = () => {
		const re = /(`+)([\s\S]*?[^`])\1(?!`)/g;
		let m;
		while ((m = re.exec(prose))) ranges.push([proseStart + m.index, proseStart + m.index + m[0].length]);
		prose = '';
	};
	lines.forEach((line, i) => {
		if (inCode[i]) {
			flushProse();
			ranges.push([at, at + line.length]);
			proseStart = at + line.length + 1;
		} else {
			if (prose === '') proseStart = at;
			prose += line + '\n';
		}
		at += line.length + 1;
	});
	flushProse();
	return ranges;
}

/**
 * Rewrite every link of a chunk that is not code, in one pass, so the code
 * ranges computed on the text stay true for every match: an inline link or
 * image (whose text may be a code span, or wrap onto the next line), a
 * reference definition, and an HTML href or src in either quote. A
 * link-shaped string inside a fence or a code span is an example, and is
 * kept; a footnote definition ([^1]:) is not a link.
 */
const linkPattern = new RegExp(
	[
		// 1-3: [text](target "title")
		/(!?\[(?:[^\]`]|`[^`]*`)*\]\()(<[^>]*>|[^)\s]+)((?:\s+"[^"]*")?\))/.source,
		// 4-5: [label]: target, at the start of a line
		/(^[ \t]*\[(?!\^)[^\]\n]+\]:[ \t]*)(\S+)/.source,
		// 6-8: href="…" or src='…'
		/((?:href|src)=(["']))(.*?)\7/.source,
	].join('|'),
	'gm',
);

function rewriteLinks(text, fromRel) {
	const ranges = codeRanges(text);
	const inCode = (at) => ranges.some(([a, b]) => at >= a && at < b);
	return text.replace(linkPattern, (all, a1, t1, b1, a2, t2, a3, q3, t3, at) => {
		if (a1 !== undefined) {
			if (inCode(at) || inCode(at + a1.length)) return all;
			const bare = t1.startsWith('<') ? t1.slice(1, -1) : t1;
			return a1 + rewriteTarget(bare, fromRel) + b1;
		}
		if (inCode(at)) return all;
		if (a2 !== undefined) return a2 + rewriteTarget(t2, fromRel);
		return a3 + rewriteTarget(t3, fromRel) + q3;
	});
}

// ------------------------------------------------------------ media

const escapeHtml = (t) => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const decodeXml = (t) =>
	String(t)
		.replace(/&lt;/g, '<')
		.replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"')
		.replace(/&apos;/g, "'")
		.replace(/&amp;/g, '&');

const mediaManifestPath = path.join(mediaDir, 'manifest.json');
const media = existsSync(mediaManifestPath) ? JSON.parse(readFileSync(mediaManifestPath, 'utf8')).items ?? {} : {};

function mediaFigure(name) {
	const item = media[name];
	if (!item) {
		// Only while the media are being made for the first time; the build
		// in CI and in a proof never sets it.
		if (process.env.SITE_ALLOW_MISSING_MEDIA === '1') return `<!-- media ${name} not generated yet -->`;
		fail(`site-media marker ${name} has no entry in src/assets/media/manifest.json (run make site-media)`);
		return '';
	}
	if (item.webm || item.mp4) {
		// A recording: plain <video> elements, one per theme, served from
		// public/media as they are. No autoplay; nothing loads until played.
		const src = (f) => `${BASE_PATH}/media/${f}`;
		const video = (webm, mp4, poster) =>
			[
				`<video controls muted playsinline preload="none"${poster ? ` poster="${src(poster)}"` : ''} aria-label="${escapeHtml(item.alt)}">`,
				webm ? `<source src="${src(webm)}" type="video/webm" />` : '',
				mp4 ? `<source src="${src(mp4)}" type="video/mp4" />` : '',
				`</video>`,
			].join('');
		for (const f of [item.webm, item.mp4, item.poster, item.webmDark, item.mp4Dark, item.posterDark].filter(Boolean)) {
			if (!existsSync(path.join(siteRoot, 'public', 'media', f))) fail(`media ${name}: public/media/${f} is missing (run make site-media)`);
		}
		return [
			`<figure class="theme-figure theme-clip">`,
			`<div class="theme-pair">${video(item.webm, item.mp4, item.poster)}${video(item.webmDark, item.mp4Dark, item.posterDark)}</div>`,
			`<figcaption>${escapeHtml(item.caption)}</figcaption>`,
			`</figure>`,
		].join('\n');
	}
	// A screenshot: two images, one shown per theme by the site's stylesheet,
	// each served from public/media as make site-media saved it, losslessly,
	// at its own size and at twice it. One HTML block, so markdown neither
	// wraps it in a paragraph (whose measure would shrink it) nor hands it to
	// Astro's image pipeline (which would re-encode it): it is drawn at its own
	// size, or narrower on a narrow screen, never enlarged.
	if (!item.width || !item.height) fail(`media ${name} has no width and height in src/assets/media/manifest.json (run make site-media)`);
	const src = (f) => `${BASE_PATH}/media/${f}`;
	const img = (one, two) =>
		`<img src="${src(one)}" srcset="${src(one)} ${item.width}w, ${src(two)} ${item.width * 2}w" sizes="(min-width: 1600px) ${item.width}px, 100vw" width="${item.width}" height="${item.height}" alt="${escapeHtml(item.alt)}" loading="lazy" decoding="async" />`;
	for (const f of [item.light, item.light2x, item.dark, item.dark2x]) {
		if (!f || !existsSync(path.join(siteRoot, 'public', 'media', f))) fail(`media ${name}: public/media/${f} is missing (run make site-media)`);
	}
	return [
		`<figure class="theme-figure theme-shot">`,
		`<div class="theme-pair">${img(item.light, item.light2x)}${img(item.dark, item.dark2x)}</div>`,
		`<figcaption>${escapeHtml(item.caption)}</figcaption>`,
		`</figure>`,
	].join('\n');
}

// The README's diagrams (docs/images/, drawn by scripts/readme-diagrams.mjs
// from the tokens), served from public/diagrams, one shown per theme.
const diagramsDir = path.join(repoRoot, 'docs', 'images');
const diagramsOut = path.join(siteRoot, 'public', 'diagrams');
rmSync(diagramsOut, { recursive: true, force: true });
function diagramFigure(name) {
	const files = [`${name}-light.svg`, `${name}-dark.svg`];
	for (const f of files) {
		if (!existsSync(path.join(diagramsDir, f))) {
			fail(`site-diagram marker ${name}: docs/images/${f} does not exist (cd site && bun run diagrams)`);
			return '';
		}
	}
	mkdirSync(diagramsOut, { recursive: true });
	for (const f of files) writeFileSync(path.join(diagramsOut, f), readFileSync(path.join(diagramsDir, f)));
	// The title is XML text: decode its entities, then escape it for the
	// attribute, so a quote in a title can never end the alt early.
	const title = /<title[^>]*>([^<]*)<\/title>/.exec(readFileSync(path.join(diagramsDir, files[0]), 'utf8'))?.[1];
	const alt = escapeHtml(title === undefined ? name : decodeXml(title));
	const img = (f) => `<img src="${BASE_PATH}/diagrams/${f}" alt="${alt}" loading="lazy" decoding="async" />`;
	return [`<figure class="theme-figure theme-diagram">`, img(files[0]), img(files[1]), `</figure>`].join('\n');
}

// ------------------------------------------- generated blocks

const MODULE = 'github.com/orkstack/orkstack/';

/** The GitHub URL of a test's declaration, found in its package's sources. */
function testUrl(importPath, test) {
	const dir = importPath.startsWith(MODULE) ? importPath.slice(MODULE.length) : null;
	if (!dir) return null;
	const abs = path.join(repoRoot, dir);
	if (!existsSync(abs)) return null;
	for (const name of readdirSync(abs).sort()) {
		if (!name.endsWith('_test.go')) continue;
		const lines = readFileSync(path.join(abs, name), 'utf8').split('\n');
		const i = lines.findIndex((l) => l.startsWith(`func ${test}(`));
		if (i >= 0) return `${repoUrl(`${dir}/${name}`)}#L${i + 1}`;
	}
	return null;
}

/**
 * The §11 coverage map as a table: every claim of AGENTS.md §11's list and
 * the tests that prove it, linked to their source, read from
 * scripts/proof-section11-coverage.tsv (which TestSection11Coverage holds
 * true) on every build.
 */
function section11Coverage() {
	const rel = 'scripts/proof-section11-coverage.tsv';
	const rows = readFileSync(path.join(repoRoot, rel), 'utf8')
		.split('\n')
		.filter((l) => l && !l.startsWith('#'))
		.map((l) => l.split('\t'));
	const out = ['| § | Claim | Proved by |', '|---|---|---|'];
	for (const [bullet, claim, evidence = '', gap = ''] of rows) {
		const key = claim.split(' ')[0];
		const text = claim.slice(key.length + 1).replace(/\|/g, '\\|');
		const proofs = evidence
			.split(',')
			.filter(Boolean)
			.map((e) => {
				const f = /^fixture:([^:]+):(.*)$/.exec(e);
				if (f) return `[${path.basename(f[1])}](${repoUrl(f[1])}) (fixture)`;
				const m = /^(unit|integration|chaos):(.+)\.(Test[A-Za-z0-9_]*)(\/.*)?$/.exec(e);
				if (!m) {
					fail(`${rel}: cannot read the evidence ${e}`);
					return e;
				}
				const [, kind, pkg, test, sub = ''] = m;
				const href = testUrl(pkg, test);
				if (!href) {
					fail(`${rel}: ${test} is not declared in ${pkg}`);
					return `\`${test}\``;
				}
				return `[\`${test}${sub}\`](${href}) (${kind})`;
			});
		if (gap) proofs.push(`**${gap.replace(/^known-gap:\s*/, 'Known gap: ')}**`);
		out.push(`| ${bullet} (${key}) | ${text} | ${proofs.join('<br>')} |`);
	}
	return [`The coverage map, generated from [\`${rel}\`](${repoUrl(rel)}):`, '', ...out].join('\n');
}

const generatedBlocks = { 'section11-coverage': section11Coverage };

// ------------------------------------------------------------ writing

rmSync(path.join(outRoot, 'docs'), { recursive: true, force: true });

function frontmatter(fields) {
	return `---\n${YAML.stringify(fields, { lineWidth: 0 }).trimEnd()}\n---\n\n`;
}

function outFileFor(route) {
	return route === 'docs' ? path.join(outRoot, 'docs', 'index.md') : path.join(outRoot, `${route}.md`);
}

for (const page of pages) {
	const outFile = outFileFor(page.route);
	const firstSource = page.parts.find((p) => p.source)?.source;
	let body = pageBodies
		.get(page.route)
		.map((c) => (c.source ? rewriteLinks(c.text, c.source) : c.text))
		.join('\n\n');
	body = body.replace(/<!--\s*site-media:\s*([a-z0-9-]+)\s*-->/g, (_, name) => mediaFigure(name));
	body = body.replace(/<!--\s*site-diagram:\s*([a-z0-9-]+)\s*-->/g, (_, name) => diagramFigure(name));
	body = body.replace(/<!--\s*site-generated:\s*([a-z0-9-]+)\s*-->/g, (_, name) => {
		if (!generatedBlocks[name]) {
			fail(`${page.route}: no generated block named ${name}`);
			return '';
		}
		return generatedBlocks[name]();
	});
	for (const marker of leftoverMarkers(body)) fail(`${page.route}: the marker <!-- ${marker} --> was not replaced (no such media or diagram name)`);
	const fields = { title: page.title, description: page.description };
	fields.editUrl = firstSource ? `${REPO}/edit/${BRANCH}/${firstSource}` : false;
	mkdirSync(path.dirname(outFile), { recursive: true });
	writeFileSync(outFile, frontmatter(fields) + body.trimEnd() + '\n');
}

for (const ex of exampleFiles) {
	const outFile = outFileFor(ex.route);
	const yaml = readFileSync(path.join(repoRoot, ex.rel), 'utf8').replace(/\n+$/, '');
	const body = [
		`[Examples](${BASE_PATH}/${path.posix.dirname(ex.route)}/) lists what this ${ex.kind === 'Automation' ? 'automation document' : 'Orkflow'} shows and what it needs to run; its comment header says the same. The file, as it is in the repository: [${ex.rel}](${repoUrl(ex.rel)}).`,
		'',
		'```yaml title="' + ex.rel + '"',
		yaml,
		'```',
	].join('\n');
	mkdirSync(path.dirname(outFile), { recursive: true });
	writeFileSync(outFile, frontmatter({ title: ex.title, description: ex.description, editUrl: `${REPO}/edit/${BRANCH}/${ex.rel}` }) + body + '\n');
}

// ------------------------------------------------------------ citations
//
// A page written for the site cites the repository docs it summarises in
// its See also (scripts/citations.mjs); every doc under docs/ must be cited
// by some page (or be a generated reference ported whole), and every page
// under site/content/ must be in the manifest, so neither a new doc nor a
// new page is left out.

const cited = new Set();
for (const page of pages) {
	for (const part of page.parts) {
		if (part.source && !part.source.startsWith('site/content/')) cited.add(part.source);
	}
	for (const rel of page.parts.map((x) => x.source).filter((x) => x && x.startsWith('site/content/'))) {
		const { problems: found, cited: files } = checkCitations({ rel, text: read(rel), repoRoot, repo: REPO, branch: BRANCH });
		found.forEach(fail);
		for (const f of files) cited.add(f);
	}
}
for (const doc of uncitedDocs(cited, readdirSync(path.join(repoRoot, 'docs')).sort().map((n) => `docs/${n}`))) {
	fail(`${doc} is cited by no page of the site: link it from the page that covers it`);
}
const listed = new Set(pages.flatMap((p) => p.parts.map((x) => x.source).filter(Boolean)));
(function walk(dir) {
	for (const entry of readdirSync(path.join(repoRoot, dir), { withFileTypes: true })) {
		const rel = `${dir}/${entry.name}`;
		if (entry.isDirectory()) walk(rel);
		else if (rel.endsWith('.md') && !listed.has(rel)) fail(`${rel} is not a page of docs.manifest.json`);
	}
})('site/content');

// ------------------------------------------------------------ styles

const generated = path.join(siteRoot, 'src', 'styles', 'generated');
mkdirSync(generated, { recursive: true });
const tokens = readTokens();
writeFileSync(path.join(generated, 'nojs-theme.css'), darkMediaCss(tokens));

// The UI's icons (ui/src/lib/icons.ts) as data, so the site reads the UI's
// one icon set without bundling the UI's TypeScript (whose tsconfig needs
// the UI's generated .svelte-kit directory). Each entry is one line,
// `name: ['d', …],`; anything else inside the object is refused.
{
	const rel = 'ui/src/lib/icons.ts';
	const text = readFileSync(path.join(repoRoot, rel), 'utf8');
	const body = /export const ICONS = \{\n([\s\S]*?)\n\} as const;/.exec(text);
	if (!body) fail(`${rel}: no \`export const ICONS = { … } as const;\` object`);
	const icons = {};
	for (const line of body ? body[1].split('\n') : []) {
		const m = /^\t([a-zA-Z]+): \[((?:'[^']*'(?:, )?)+)\],?$/.exec(line);
		if (!m) {
			fail(`${rel}: cannot read the icon line ${JSON.stringify(line)}`);
			continue;
		}
		icons[m[1]] = [...m[2].matchAll(/'([^']*)'/g)].map((x) => x[1]);
	}
	if (body && Object.keys(icons).length === 0) fail(`${rel}: no icons read`);
	writeFileSync(path.join(generated, 'icons.json'), JSON.stringify(icons, null, '\t') + '\n');
}

// The code themes the landing page's listing renders with (the docs get the
// same two through Expressive Code, in astro.config.mjs).
writeFileSync(
	path.join(generated, 'code-themes.json'),
	JSON.stringify({ light: codeTheme('light', tokens), dark: codeTheme('dark', tokens) }, null, '\t') + '\n',
);

// The favicon: the wordmark's title-block corner, in each theme's ink.
mkdirSync(path.join(siteRoot, 'public'), { recursive: true });
writeFileSync(
	path.join(siteRoot, 'public', 'favicon.svg'),
	[
		'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" fill="none" stroke-width="1.5">',
		`<style>path{stroke:${resolve(tokens, '--color-ink', 'light')}}@media (prefers-color-scheme:dark){path{stroke:${resolve(tokens, '--color-ink', 'dark')}}}</style>`,
		'<path d="M1.75 1.75h12.5v12.5H1.75zM1.75 10.25h12.5M8.75 10.25v4"/>',
		'</svg>',
		'',
	].join('\n'),
);

// Exported for astro.config.mjs's sidebar.
writeFileSync(
	path.join(generated, 'examples.json'),
	JSON.stringify(exampleFiles.map(({ route, title }) => ({ route, title })), null, '\t') + '\n',
);

if (problems.length) {
	console.error(`sync-docs: ${problems.length} problem(s)\n  ${problems.join('\n  ')}`);
	process.exit(1);
}
console.log(`sync-docs: ${pages.length} pages, ${exampleFiles.length} example pages`);
