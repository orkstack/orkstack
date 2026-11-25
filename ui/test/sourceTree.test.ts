import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { listDir, listTree, readSource, scanTest, SCAN_GUARD_MS } from './sourceTree';

const here = import.meta.dir;

test('a caller that sorts or empties its list leaves the next one whole', () => {
	for (const list of [listDir, listTree]) {
		const first = list(here);
		const all = [...first];
		first.splice(0);
		expect(list(here)).toEqual(all);
		expect(all).toContain('sourceTree.ts');
	}
});

test('a directory is its own entries and a tree is everything under it, whichever is asked first', () => {
	expect(listDir(`${here}/..`).includes('test/sourceTree.ts')).toBe(false);
	expect(listTree(`${here}/../test`).includes('fixtures/testid-baseline.txt')).toBe(true);
	expect(listDir(`${here}/../test`).includes('fixtures/testid-baseline.txt')).toBe(false);
});

test('a file reads as it is, however its path is spelled', () => {
	const file = `${here}/sourceTree.ts`;
	expect(readSource(file)).toBe(readFileSync(file, 'utf8'));
	expect(readSource(`${here}/fixtures/../sourceTree.ts`)).toBe(readSource(file));
});

/**
 * unguarded names what a test file's text does that it must not: lists a
 * directory by itself, or lists one through sourceTree.ts in a plain test,
 * whether in the test's own body or through a declaration of the file's
 * (`sources()`, a list read once for the file), however deep.
 */
function unguarded(text: string): string[] {
	const found = [...text.matchAll(/\b(?:readdirSync|readdir|opendirSync|globSync|Glob)\b/g)].map((m) => m[0]);
	// A piece runs from one top-level declaration or test call to the next; a
	// test inside a loop begins a piece of its own.
	const pieces = text.split(/(?=^(?:export |async )*(?:function|const|let|class) \w)|(?=(?<![.\w])(?:test(?:\.\w+)?|scanTest)\()/m);
	const lists = new Set(['listDir', 'listTree']);
	const uses = (piece: string) => [...lists].some((name) => new RegExp(`\\b${name}\\b`).test(piece));
	for (let grew = true; grew; ) {
		grew = false;
		for (const piece of pieces) {
			const name = /^(?:export |async )*(?:function|const|let|class) (\w+)/.exec(piece)?.[1];
			if (name && !lists.has(name) && uses(piece)) grew = !!lists.add(name);
		}
	}
	for (const piece of pieces) {
		if (/^test(?:\.\w+)?\(/.test(piece) && uses(piece)) found.push(piece.slice(0, piece.indexOf('\n')));
	}
	return found;
}

test('the check sees a scan in a test, behind a helper, in a loop and by another lister', () => {
	expect(unguarded("test('a', () => {\n\tlistTree(src);\n});\n")).toEqual(["test('a', () => {"]);
	expect(unguarded("function sources() {\n\treturn listDir(d);\n}\nconst all = () => sources();\ntest('b', () => {\n\tall();\n});\n")).toEqual(["test('b', () => {"]);
	expect(unguarded("for (const x of xs) {\n\ttest(x, () => {\n\t\tlistDir(x);\n\t});\n}\n")).toEqual(['test(x, () => {']);
	expect(unguarded("test('g', () => {\n\texpect(/x/.test(listed)).toBe(true);\n});\n")).toEqual([]);
	expect(unguarded("test.skip('c', () => {\n\tlistDir(x);\n});\n")).toEqual(["test.skip('c', () => {"]);
	for (const lister of ['readdirSync(d)', 'await readdir(d)', 'globSync(p)', 'new Bun.Glob(p)']) {
		expect(unguarded(`scanTest('d', async () => {\n\t${lister};\n});\n`).length).toBe(1);
	}
	expect(unguarded("function sources() {\n\treturn listDir(d);\n}\nscanTest('e', () => {\n\tsources();\n});\ntest('f', () => {\n\texpect(1).toBe(1);\n});\n")).toEqual([]);
});

// A test that lists a directory of the repository waits on the host's disk and
// load, so it takes the guard and the shared read, not Bun's 5 s default.
scanTest('a unit test lists directories only through sourceTree.ts, and only under its guard', () => {
	expect(SCAN_GUARD_MS).toBe(60_000);
	const files = listDir(here).filter((f) => f.endsWith('.test.ts') && f !== 'sourceTree.test.ts');
	expect(files.length > 100).toBe(true);
	expect(files.flatMap((f) => unguarded(readSource(`${here}/${f}`)).map((what) => `${f}: ${what}`))).toEqual([]);
});
