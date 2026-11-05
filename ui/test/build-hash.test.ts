import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { BUILD_INPUTS, buildInputsHash } from '../e2e/build-hash';

// e2e/global-setup.ts reuses ui/build when the hash of its inputs matches the
// marker the last build left. The hash must change for any changed byte of
// any input, for an input added or removed, and must not depend on the order
// the inputs are named in.

const fixtures = `${import.meta.dir}/fixtures/build-hash`;
const inputs = ['src', 'config.txt', 'missing.txt'];

test('the hash is stable for the same inputs and differs for one changed byte', () => {
	const a1 = buildInputsHash(`${fixtures}/a`, inputs);
	const a2 = buildInputsHash(`${fixtures}/a`, inputs);
	const b = buildInputsHash(`${fixtures}/b`, inputs);
	expect(a1).toBe(a2);
	expect(a1).toHaveLength(64);
	expect(a1 === b).toBe(false);
});

test('the order the inputs are named in does not matter, their set does', () => {
	const forward = buildInputsHash(`${fixtures}/a`, inputs);
	const reversed = buildInputsHash(`${fixtures}/a`, [...inputs].reverse());
	const fewer = buildInputsHash(`${fixtures}/a`, ['src']);
	expect(forward).toBe(reversed);
	expect(forward === fewer).toBe(false);
});

// A file for every input the build reads, under a ui/ beside an examples/.
const defaultInputFiles = [
	'src/app.txt',
	'src/lib/gen/client.ts',
	'static/favicon.svg',
	'vite.config.ts',
	'tsconfig.json',
	'package.json',
	'bun.lock',
	'../examples/each-fan-out.ork.yaml'
];

test('every default input has a file among the edited ones', () => {
	const uncovered = BUILD_INPUTS.filter(
		(input) => !defaultInputFiles.some((file) => file === input || file.startsWith(`${input}/`))
	);
	expect(uncovered).toEqual([]);
});

for (const edited of defaultInputFiles) {
	test(`with the default inputs an edit to ${edited} changes the hash`, () => {
		const repo = mkdtempSync(`${tmpdir()}/orkstack-build-hash-`);
		try {
			for (const file of defaultInputFiles) {
				const path = `${repo}/ui/${file}`;
				mkdirSync(path.slice(0, path.lastIndexOf('/')), { recursive: true });
				writeFileSync(path, `${file} one`);
			}
			const before = buildInputsHash(`${repo}/ui`);
			writeFileSync(`${repo}/ui/${edited}`, `${edited} two`);
			expect(buildInputsHash(`${repo}/ui`) === before).toBe(false);
		} finally {
			rmSync(repo, { recursive: true, force: true });
		}
	});
}

test('with the default inputs a file the build does not read leaves the hash alone', () => {
	const repo = mkdtempSync(`${tmpdir()}/orkstack-build-hash-`);
	try {
		mkdirSync(`${repo}/ui/src`, { recursive: true });
		writeFileSync(`${repo}/ui/src/app.txt`, 'one');
		const before = buildInputsHash(`${repo}/ui`);
		// The build's own output and the installed packages most of all: a hash
		// over them would change with every build and every install.
		for (const file of ['e2e/some.spec.ts', 'README.md', 'build/index.html', 'node_modules/pkg/index.js']) {
			mkdirSync(`${repo}/ui/${file.slice(0, file.lastIndexOf('/') + 1)}`, { recursive: true });
			writeFileSync(`${repo}/ui/${file}`, file);
		}
		expect(buildInputsHash(`${repo}/ui`)).toBe(before);
	} finally {
		rmSync(repo, { recursive: true, force: true });
	}
});

test('an input beside ui/ is walked: an edited example changes the hash', () => {
	const repo = mkdtempSync(`${tmpdir()}/orkstack-build-hash-`);
	try {
		mkdirSync(`${repo}/ui/src`, { recursive: true });
		mkdirSync(`${repo}/examples`, { recursive: true });
		writeFileSync(`${repo}/ui/src/app.txt`, 'one');
		writeFileSync(`${repo}/examples/each-fan-out.ork.yaml`, 'name: hello\n');
		const before = buildInputsHash(`${repo}/ui`, ['src', '../examples']);
		const sourcesOnly = buildInputsHash(`${repo}/ui`, ['src']);
		writeFileSync(`${repo}/examples/each-fan-out.ork.yaml`, 'name: hello-again\n');
		expect(buildInputsHash(`${repo}/ui`, ['src', '../examples']) === before).toBe(false);
		// The positive control: without the input the same edit is invisible.
		expect(buildInputsHash(`${repo}/ui`, ['src'])).toBe(sourcesOnly);
	} finally {
		rmSync(repo, { recursive: true, force: true });
	}
});
