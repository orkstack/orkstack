import { expect, test } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { BUILD_HASH_FILE, buildInputsHash } from '../e2e/build-hash';
import { ensureUIBuild } from '../e2e/build-ui';

// e2e/build-ui.ts is what lets scripts/proof.sh build the UI during its
// ordinary integration jobs and the Playwright gate skip the build: it builds
// only when the hash beside ui/build is not its inputs' hash, and it never
// leaves a matching hash beside a build that failed. Each case runs a
// throwaway UI directory whose `build` script counts its runs in a file that
// is not a build input.

function uiDir(script: string): string {
	const dir = mkdtempSync(`${tmpdir()}/orkstack-build-ui-`);
	mkdirSync(`${dir}/src`, { recursive: true });
	writeFileSync(`${dir}/src/app.txt`, 'one');
	setScript(dir, script);
	return dir;
}

function setScript(dir: string, script: string): void {
	writeFileSync(`${dir}/package.json`, JSON.stringify({ name: 'fake-ui', scripts: { build: script } }));
}

const builds = 'echo run >> runs.txt && mkdir -p build && echo server > build/index.js';
const runs = (dir: string) => (existsSync(`${dir}/runs.txt`) ? readFileSync(`${dir}/runs.txt`, 'utf8').trim().split('\n').length : 0);
const marker = (dir: string) => `${dir}/build/${BUILD_HASH_FILE}`;

function throws(f: () => void): boolean {
	try {
		f();
	} catch {
		return true;
	}
	return false;
}

test('it builds once, writes the inputs hash, and reuses the build while the inputs are unchanged', () => {
	const dir = uiDir(builds);
	try {
		expect(ensureUIBuild(dir)).toBe('built');
		expect(readFileSync(marker(dir), 'utf8').trim()).toBe(buildInputsHash(dir));
		expect(ensureUIBuild(dir)).toBe('reused');
		expect(runs(dir)).toBe(1);
		// A changed input: built again.
		writeFileSync(`${dir}/src/app.txt`, 'two');
		expect(ensureUIBuild(dir)).toBe('built');
		expect(runs(dir)).toBe(2);
		// A stale hash, and a build with no server entry: built again.
		writeFileSync(marker(dir), 'stale\n');
		expect(ensureUIBuild(dir)).toBe('built');
		rmSync(`${dir}/build/index.js`);
		expect(ensureUIBuild(dir)).toBe('built');
		expect(runs(dir)).toBe(4);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test('a failed build throws and leaves no hash, so the next caller builds again', () => {
	const dir = uiDir(builds);
	try {
		expect(ensureUIBuild(dir)).toBe('built');
		// The inputs change and the build fails: the old hash must not survive
		// beside whatever the failed build left.
		setScript(dir, 'echo run >> runs.txt && exit 1');
		expect(throws(() => ensureUIBuild(dir))).toBe(true);
		expect(existsSync(marker(dir))).toBe(false);
		// A build that exits 0 and writes no server entry fails too.
		setScript(dir, 'echo run >> runs.txt && mkdir -p build');
		rmSync(`${dir}/build/index.js`);
		expect(throws(() => ensureUIBuild(dir))).toBe(true);
		expect(existsSync(marker(dir))).toBe(false);
		// The positive control: a working build after both is built and marked.
		setScript(dir, builds);
		expect(ensureUIBuild(dir)).toBe('built');
		expect(readFileSync(marker(dir), 'utf8').trim()).toBe(buildInputsHash(dir));
		expect(runs(dir)).toBe(4);
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});
