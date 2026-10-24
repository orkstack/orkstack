// The drift check: the site may draw nothing the UI's tokens do not define.
//
// It fails when, anywhere under site/src (generated files excepted):
//   - a colour is written as a value: a hex colour, a colour function
//     (rgb, hsl, hwb, lab, lch, oklab, oklch, color) or a named colour in a
//     colour property. color-mix() of tokens is allowed: it composes tokens;
//   - a font family is set to anything but var(--font-sans), var(--font-mono)
//     or inherit;
//   - a var(--name) names a custom property that neither ui/src/tokens.css
//     nor the site itself (or Starlight, Expressive Code and Shiki, whose own
//     properties the site maps) declares, so a token the UI renamed or
//     removed fails here rather than silently falling back;
//   - the self-hosted faces the site installs are not the UI's exact versions;
//   - a generated media file is over its budget.
// tokens.css itself being missing fails in readTokens(), as does the build.
import { readFileSync, readdirSync, statSync, existsSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { siteRoot, repoRoot, readTokens } from './tokens.mjs';

// Colour literals. Hex colours only where a value can stand: after a colon,
// a space, a comma or an opening parenthesis, so an anchor (#tasks) or a
// numbered item in prose does not match.
const hex = /(^|[\s:(,])#(?:[0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})\b(?![-\w])/;
const colourFn = /\b(?:rgba?|hsla?|hwb|lab|lch|oklab|oklch|color)\(/;
const named =
	/\b(?:color|background(?:-color)?|border(?:-[a-z]+)*-color|border(?:-(?:top|right|bottom|left|block|inline))?|fill|stroke|outline(?:-color)?|caret-color|accent-color|text-decoration-color|column-rule-color|box-shadow|text-shadow)\s*:\s*[^;{}]*\b(?:black|white|red|green|blue|gray|grey|silver|navy|maroon|purple|teal|olive|orange|yellow|pink|brown|cyan|magenta|lime|aqua|fuchsia)\b/;
const fontFamily = /font-family\s*:\s*([^;}"]+)/g;
const varRef = /var\(\s*(--[A-Za-z0-9_-]+)/g;
const declared = /(^|[\s;{(])(--[A-Za-z0-9_-]+)\s*:/g;

// Custom properties the site may read without declaring them: Starlight's
// and Expressive Code's own (the site maps onto them), Shiki's dual-theme
// variables, and the inline --line, --first-line style hooks set in markup.
const foreign = /^--(?:sl-|__sl-|ec-|shiki-|astro-)/;

/**
 * The colour, face and token-name problems of a set of sources (path → text):
 * what the drift check refuses, separated from the file walk so it can be
 * tested on planted text.
 */
export function checkSources(sources, tokens, where = (f) => f) {
	const problems = [];
	const siteDeclared = new Set();
	for (const text of sources.values()) {
		for (const m of text.matchAll(declared)) siteDeclared.add(m[2]);
		for (const m of text.matchAll(/style=\{?[`"']([^`"']*)/g)) {
			for (const d of m[1].matchAll(/(--[A-Za-z0-9_-]+)\s*:/g)) siteDeclared.add(d[1]);
		}
	}
	for (const [file, text] of sources) {
		let inBlockComment = false;
		text.split('\n').forEach((line, i) => {
			// Comment-only lines (CSS, JS) may talk about colours freely.
			const trimmed = line.trim();
			if (inBlockComment) {
				if (trimmed.includes('*/')) inBlockComment = false;
				return;
			}
			if (trimmed.startsWith('/*')) {
				if (!trimmed.includes('*/')) inBlockComment = true;
				return;
			}
			if (trimmed.startsWith('//') || trimmed.startsWith('*')) return;
			const at = `${where(file)}:${i + 1}`;
			if (hex.test(line)) problems.push(`${at}: a hex colour; use a token from ui/src/tokens.css`);
			if (colourFn.test(line.replace(/color-mix\(/g, ''))) problems.push(`${at}: a colour function; use a token from ui/src/tokens.css`);
			// A token's name (var(--color-blue)) is not a colour written out.
			const values = line.replace(/var\([^)]*\)/g, 'var()').replace(/--[A-Za-z0-9_-]+/g, '--x');
			if (named.test(values)) problems.push(`${at}: a named colour; use a token from ui/src/tokens.css`);
			for (const m of line.matchAll(fontFamily)) {
				const v = m[1].trim();
				if (!/^(?:var\(--font-(?:sans|mono)\)|inherit)$/.test(v)) {
					problems.push(`${at}: font-family ${v}; use var(--font-sans) or var(--font-mono)`);
				}
			}
			for (const m of line.matchAll(varRef)) {
				const name = m[1];
				if (tokens.names.has(name) || siteDeclared.has(name) || foreign.test(name)) continue;
				problems.push(`${at}: var(${name}) is not a token of ui/src/tokens.css`);
			}
		});
	}
	return problems;
}

function main() {
	const tokens = readTokens();
	const problems = [];
	const srcRoot = path.join(siteRoot, 'src');
	const skip = [
		path.join(srcRoot, 'styles', 'generated'),
		path.join(srcRoot, 'content', 'docs', 'docs'),
		path.join(srcRoot, 'assets'),
	];
	const walk = (dir, out = []) => {
		for (const name of readdirSync(dir)) {
			const abs = path.join(dir, name);
			if (skip.some((s) => abs === s || abs.startsWith(s + path.sep))) continue;
			if (statSync(abs).isDirectory()) walk(abs, out);
			else if (/\.(css|astro|ts|mjs|js|md|mdx)$/.test(name)) out.push(abs);
		}
		return out;
	};
	const files = walk(srcRoot);
	const rel = (f) => path.relative(repoRoot, f);

	const sources = new Map(files.map((f) => [f, readFileSync(f, 'utf8')]));
	problems.push(...checkSources(sources, tokens, rel));

	// The mapping files must name only tokens that exist (the check above), and
	// must still map: a mapping file that lost its var() references would pass
	// vacuously.
	for (const f of ['styles/starlight.css', 'styles/base.css']) {
		const text = sources.get(path.join(srcRoot, f)) ?? '';
		if (!/var\(--color-/.test(text)) problems.push(`site/src/${f} maps no colour token`);
	}

	// The faces: the UI's own packages, at the UI's own versions.
	const uiPkg = JSON.parse(readFileSync(path.join(repoRoot, 'ui', 'package.json'), 'utf8'));
	const sitePkg = JSON.parse(readFileSync(path.join(siteRoot, 'package.json'), 'utf8'));
	for (const face of ['@fontsource-variable/archivo-narrow', '@fontsource-variable/spline-sans-mono']) {
		const ui = (uiPkg.dependencies?.[face] ?? uiPkg.devDependencies?.[face] ?? '').replace(/^[\^~]/, '');
		const site = sitePkg.dependencies?.[face] ?? '';
		if (!ui) problems.push(`ui/package.json no longer installs ${face}; the site's face must follow the UI's`);
		else if (site !== ui) problems.push(`site/package.json pins ${face}@${site}, the UI ${ui}`);
	}
	for (const [name, value] of tokens.invariant) {
		if (name === '--font-sans' && !/Archivo Narrow Variable/.test(value)) problems.push('tokens.css --font-sans is no longer Archivo Narrow: update the face the site installs');
		if (name === '--font-mono' && !/Spline Sans Mono Variable/.test(value)) problems.push('tokens.css --font-mono is no longer Spline Sans Mono: update the face the site installs');
	}

	// Media budget: what one page may make a visitor download.
	const mediaDir = path.join(srcRoot, 'assets', 'media');
	const budget = { image: 900 * 1024, video: 2.5 * 1024 * 1024, total: 16 * 1024 * 1024 };
	if (existsSync(mediaDir)) {
		let total = 0;
		for (const name of readdirSync(mediaDir)) {
			const size = statSync(path.join(mediaDir, name)).size;
			total += size;
			const kind = /\.(webm|mp4)$/.test(name) ? 'video' : /\.(png|webp|avif|jpg)$/.test(name) ? 'image' : null;
			if (kind && size > budget[kind]) {
				problems.push(`site/src/assets/media/${name} is ${(size / 1024).toFixed(0)} KiB, over the ${kind} budget of ${(budget[kind] / 1024).toFixed(0)} KiB`);
			}
		}
		if (total > budget.total) problems.push(`site/src/assets/media totals ${(total / 1048576).toFixed(1)} MiB, over ${budget.total / 1048576} MiB`);
	}

	if (problems.length) {
		console.error(`check-tokens: ${problems.length} problem(s)\n  ${problems.join('\n  ')}`);
		process.exit(1);
	}
	console.log(`check-tokens: ${files.length} files hold to ui/src/tokens.css (${tokens.names.size} tokens)`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) main();
