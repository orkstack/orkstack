// The example Orkflow the landing page shows, read from the repository at
// build time so the page can never show YAML the file does not contain. The
// screenshots and the recording are of a run of this same file
// (media/capture.ts).
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';

export const examplePath = 'examples/each-fan-out.ork.yaml';
// Astro builds from site/ (bun run build, the site gate, site.yml); bundled
// code cannot use its own location, so the repository is site/'s parent.
const repoRoot = path.resolve(process.cwd(), '..');
if (!existsSync(path.join(repoRoot, 'ui', 'src', 'tokens.css'))) {
	throw new Error(`build the site from site/ (cwd is ${process.cwd()})`);
}
const text = readFileSync(path.join(repoRoot, examplePath), 'utf8');
const doc = YAML.parse(text);

/** The document from its header on, without the comment block above it. */
const lines = text.replace(/\n+$/, '').split('\n');
const start = lines.findIndex((l) => l.startsWith('apiVersion:'));
if (start < 0) throw new Error(`${examplePath}: no apiVersion line`);

export const example = {
	path: examplePath,
	name: doc.name as string,
	version: doc.version as string,
	tasks: (doc.tasks as unknown[]).length,
	/** 1-based line number in the file of the first line shown. */
	firstLine: start + 1,
	lines: lines.slice(start),
};

/** The 1-based line, within the shown excerpt, of the first line matching. */
export function lineOf(pattern: RegExp, after = 0): number {
	const i = example.lines.findIndex((l, n) => n >= after && pattern.test(l));
	if (i < 0) throw new Error(`${examplePath}: no line matches ${pattern}; the landing page's annotations need it`);
	return i + 1;
}
