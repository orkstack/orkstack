// The example the landing page's hero shows, read from the repository at build
// time so the page can never show YAML the file does not contain: the render
// task of examples/config-from-git.ork.yaml, from its id down to its template
// source.
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';

export const examplePath = 'examples/config-from-git.ork.yaml';
// Astro builds from site/; bundled code cannot use its own location.
const repoRoot = path.resolve(process.cwd(), '..');
if (!existsSync(path.join(repoRoot, 'ui', 'src', 'tokens.css'))) {
	throw new Error(`build the site from site/ (cwd is ${process.cwd()})`);
}
const text = readFileSync(path.join(repoRoot, examplePath), 'utf8');
const doc = YAML.parse(text);
const lines = text.split('\n');
const first = lines.findIndex((l) => /^\s*- id: render$/.test(l));
if (first < 0) throw new Error(`${examplePath}: no render task`);
const last = lines.findIndex((l, i) => i > first && /^\s+src:/.test(l));
if (last < 0) throw new Error(`${examplePath}: the render task has no src`);
const indent = /^\s*/.exec(lines[first])?.[0].length ?? 0;

export const example = {
	path: examplePath,
	name: doc.name as string,
	version: doc.version as string,
	/** The render task's lines, as they are in the file, without their indent. */
	excerpt: lines
		.slice(first, last + 1)
		.map((l) => l.slice(indent))
		.join('\n'),
};
