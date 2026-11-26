import { expect } from 'bun:test';
import path from 'node:path';
import { listTree, readSource, scanTest } from './sourceTree';

// The app runs in a browser, where Node's globals and modules do not exist.
// Its type check cannot say so: `@types/node` is a dependency (the specs under
// ui/e2e run on Node and are checked against it), and SvelteKit's own types
// reference it through Vite's, so `process` and `Buffer` type-check in
// ui/src as they would in a Node program. This rule refuses them instead.

const uiDir = path.resolve(import.meta.dir, '..');

const NODE_ONLY: { name: string; pattern: RegExp }[] = [
	{
		name: 'a Node module',
		pattern:
			/(\bfrom\s+|\bimport\s+|\bimport\(\s*)['"](node:[\w/]+|fs|path|os|child_process|crypto|http|https|net|url|util|zlib|stream|events|worker_threads)(\/[\w/]+)?['"]/
	},
	{ name: 'require', pattern: /\brequire\(/ },
	{
		name: 'process',
		pattern:
			/\bglobalThis\.process\b|(^|[^.\w])process\.(env|argv|cwd|exit|on|platform|versions|nextTick|stdout|stderr|hrtime)\b/
	},
	{
		name: 'Buffer',
		pattern: /\bnew Buffer\(|(^|[^.\w])Buffer\.(from|alloc|allocUnsafe|concat|isBuffer|byteLength)\b/
	},
	{ name: 'the NodeJS namespace', pattern: /\bNodeJS\.\w/ },
	{ name: "a CommonJS module's path", pattern: /\b__(dirname|filename)\b/ }
];

/** nodeOnlyUses names each Node-only use in text, with its 1-based line. */
function nodeOnlyUses(text: string): string[] {
	const found: string[] = [];
	text.split('\n').forEach((line, i) => {
		for (const { name, pattern } of NODE_ONLY) {
			if (pattern.test(line)) found.push(`${i + 1}: ${name}`);
		}
	});
	return found;
}

scanTest('the rule names every Node-only use it is for', () => {
	expect(nodeOnlyUses("import { readFileSync } from 'node:fs';")).toEqual(['1: a Node module']);
	expect(nodeOnlyUses("const m = await import('node:os');")).toEqual(['1: a Node module']);
	expect(nodeOnlyUses("import 'node:fs';")).toEqual(['1: a Node module']);
	expect(nodeOnlyUses("import fs from 'fs';")).toEqual(['1: a Node module']);
	expect(nodeOnlyUses("import { join } from 'path/posix';")).toEqual(['1: a Node module']);
	expect(nodeOnlyUses('const p = globalThis.process;')).toEqual(['1: process']);
	expect(nodeOnlyUses("process.on('exit', done);")).toEqual(['1: process']);
	expect(nodeOnlyUses('const b = new Buffer(4);')).toEqual(['1: Buffer']);
	expect(nodeOnlyUses("const fs = require('fs');")).toEqual(['1: require']);
	expect(nodeOnlyUses('const home = process.env.HOME;')).toEqual(['1: process']);
	expect(nodeOnlyUses("const b = Buffer.from('x');")).toEqual(['1: Buffer']);
	expect(nodeOnlyUses('let t: NodeJS.Timeout;')).toEqual(['1: the NodeJS namespace']);
	expect(nodeOnlyUses('const here = __dirname;')).toEqual(["1: a CommonJS module's path"]);
	// Words a comment or a label uses are not uses.
	expect(nodeOnlyUses('// the process. A buffer of events. Required.')).toEqual([]);
	expect(nodeOnlyUses("const p = run.process.env; import x from './paths';")).toEqual([]);
});

scanTest('no source of ui/src uses what only Node has', () => {
	const offenders: string[] = [];
	for (const file of listTree(`${uiDir}/src`)) {
		if (!/\.(ts|js|svelte)$/.test(file) || file.startsWith('lib/gen/')) continue;
		for (const use of nodeOnlyUses(readSource(`${uiDir}/src/${file}`))) {
			offenders.push(`src/${file}:${use}`);
		}
	}
	expect(offenders, 'ui/src runs in a browser: Node globals and modules are not there').toEqual([]);
});
