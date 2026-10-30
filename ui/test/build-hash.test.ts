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

test('the default inputs name the sources, assets, configs, the lockfile and the examples', () => {
	expect(BUILD_INPUTS).toContain('src');
	expect(BUILD_INPUTS).toContain('static');
	expect(BUILD_INPUTS).toContain('bun.lock');
	expect(BUILD_INPUTS).toContain('svelte.config.js');
	// The build writes the example gallery from the repository's examples/.
	expect(BUILD_INPUTS).toContain('../examples');
});

test('an input beside ui/ is walked: an edited example changes the hash', () => {
	const repo = mkdtempSync(`${tmpdir()}/orkstack-build-hash-`);
	try {
		mkdirSync(`${repo}/ui/src`, { recursive: true });
		mkdirSync(`${repo}/examples`, { recursive: true });
		writeFileSync(`${repo}/ui/src/app.txt`, 'one');
		writeFileSync(`${repo}/examples/hello.ork.yaml`, 'name: hello\n');
		const before = buildInputsHash(`${repo}/ui`, ['src', '../examples']);
		const sourcesOnly = buildInputsHash(`${repo}/ui`, ['src']);
		writeFileSync(`${repo}/examples/hello.ork.yaml`, 'name: hello-again\n');
		expect(buildInputsHash(`${repo}/ui`, ['src', '../examples']) === before).toBe(false);
		// The positive control: without the input the same edit is invisible.
		expect(buildInputsHash(`${repo}/ui`, ['src'])).toBe(sourcesOnly);
	} finally {
		rmSync(repo, { recursive: true, force: true });
	}
});
