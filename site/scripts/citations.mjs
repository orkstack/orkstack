// A docs page written for the site cites the repository docs it summarises:
// its "## See also" links them on GitHub
// (https://github.com/orkstack/orkstack/blob/main/<path>[#heading]). This
// module finds those citations and checks that each names a file, and a
// heading of it, that exist. scripts/sync-docs.mjs runs it on every page;
// test/citations.test.mjs plants pages it must refuse.
import { existsSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import GithubSlugger from 'github-slugger';

const FENCE_OPEN = /^\s{0,3}(`{3,}|~{3,})/;
const FENCE_CLOSE = /^\s{0,3}(`{3,}|~{3,})\s*$/;

/** Each line of a markdown text, with whether it is inside (or opens or closes) a code fence. */
function fenced(text) {
	let fence = null;
	return text.split('\n').map((line) => {
		if (fence) {
			const close = FENCE_CLOSE.exec(line);
			if (close && close[1][0] === fence[0] && close[1].length >= fence.length) fence = null;
			return { line, code: true };
		}
		const open = FENCE_OPEN.exec(line);
		if (open) {
			fence = open[1];
			return { line, code: true };
		}
		return { line, code: false };
	});
}

/** The text with every code block and code span blanked, so a URL in code is no citation. */
export function stripCode(text) {
	return fenced(text)
		.map(({ line, code }) => (code ? '' : line.replace(/(`+)[\s\S]*?\1/g, (s) => ' '.repeat(s.length))))
		.join('\n');
}

/** GitHub's slugs of a markdown text's headings, outside code fences. */
export function headingSlugs(text) {
	const slugger = new GithubSlugger();
	const out = new Set();
	for (const { line, code } of fenced(text)) {
		const m = !code && /^#{1,6}\s+(.*)$/.exec(line);
		if (!m) continue;
		const heading = m[1]
			.replace(/\s+#+\s*$/, '')
			.replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
			.trim();
		out.add(slugger.slug(heading));
	}
	return out;
}

/** The lines under a page's "## See also" heading (outside code), or null. */
export function seeAlso(text) {
	const lines = fenced(text);
	const at = lines.findIndex(({ line, code }) => !code && /^## See also\s*$/.test(line));
	if (at < 0) return null;
	const out = [];
	for (const { line, code } of lines.slice(at + 1)) {
		if (!code && /^#{1,2}\s/.test(line)) break;
		out.push(line);
	}
	return out.join('\n');
}

const escapeRegExp = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** The repository citations in a text, code excluded: [{ path, anchor }], raw (undecoded). */
export function citations(text, repo, branch) {
	const re = new RegExp(`${escapeRegExp(repo)}/blob/${escapeRegExp(branch)}/([^\\s)>#?]+)(?:\\?[^\\s)>#]*)?(?:#([^\\s)>]+))?`, 'g');
	return [...stripCode(text).matchAll(re)].map((m) => {
		let p = m[1];
		let anchor = m[2] ?? '';
		// Prose punctuation after a bare URL is not part of it.
		if (anchor) anchor = anchor.replace(/[.,;:]+$/, '');
		else p = p.replace(/[.,;:]+$/, '');
		return { path: p, anchor };
	});
}

const slugCache = new Map();

/**
 * Whether an anchor names a heading of a repository file: null when it does
 * (or when it cannot be checked: a non-markdown file, a #L line anchor),
 * else the problem, phrased to follow "<file>#<anchor>, ".
 */
export function checkAnchor(repoRoot, file, anchor) {
	if (!anchor) return null;
	let decoded;
	try {
		decoded = decodeURIComponent(anchor);
	} catch {
		return 'a malformed anchor';
	}
	if (/^L\d+(-L\d+)?$/.test(decoded)) return null;
	const abs = path.join(repoRoot, file);
	if (!file.endsWith('.md') || !existsSync(abs) || !statSync(abs).isFile()) return null;
	if (!slugCache.has(abs)) slugCache.set(abs, headingSlugs(readFileSync(abs, 'utf8')));
	return slugCache.get(abs).has(decoded) ? null : `a heading ${file} does not have`;
}

/**
 * Checks one page's citations: it has a "## See also" that cites at least one
 * repository doc, and every citation anywhere on it names a file (and
 * heading) that exist inside the repository. Returns the problems and the
 * repository files it cites.
 */
export function checkCitations({ rel, text, repoRoot, repo, branch }) {
	const problems = [];
	const cited = new Set();
	const section = seeAlso(text);
	if (section === null) problems.push(`${rel} has no "## See also" section`);
	else if (citations(section, repo, branch).length === 0) {
		problems.push(`${rel} cites no repository doc in its See also: link the docs it summarises (${repo}/blob/${branch}/<path>#<heading>)`);
	}
	for (const c of citations(text, repo, branch)) {
		let file;
		try {
			file = decodeURIComponent(c.path).replace(/\/$/, '');
		} catch {
			problems.push(`${rel} cites ${c.path}, a malformed path`);
			continue;
		}
		const normalized = path.posix.normalize(file);
		if (normalized === '..' || normalized.startsWith('../') || path.posix.isAbsolute(normalized)) {
			problems.push(`${rel} cites ${file}, which leaves the repository`);
			continue;
		}
		if (!existsSync(path.join(repoRoot, normalized))) {
			problems.push(`${rel} cites ${normalized}, which does not exist`);
			continue;
		}
		cited.add(normalized);
		const anchorProblem = checkAnchor(repoRoot, normalized, c.anchor);
		if (anchorProblem) problems.push(`${rel} cites ${normalized}#${c.anchor}, ${anchorProblem}`);
	}
	return { problems, cited };
}

/** The markdown docs no page cites. */
export function uncitedDocs(cited, docs) {
	return docs.filter((d) => d.endsWith('.md') && !cited.has(d));
}

/** Markers a page still holds after replacement: a typo, or a name nothing knows. */
export function leftoverMarkers(text) {
	return [...text.matchAll(/<!--\s*(site-(?:media|diagram|generated):[^>]*?)\s*-->/g)].map((m) => m[1]);
}
