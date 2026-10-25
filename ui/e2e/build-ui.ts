import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { BUILD_HASH_FILE, buildInputsHash } from './build-hash';

// The UI build the specs serve, made or reused by its inputs' hash
// (build-hash.ts). One function, two callers: global-setup.ts, at the start
// of every Playwright run, and scripts/proof.sh, which runs this file with
// `bun e2e/build-ui.ts` while its ordinary integration jobs run — after the
// heavy ones, never beside them — so the Playwright gate finds the build made
// and the hash matching. If that build failed, was cut short, or the inputs
// changed since, the hash does not match and the gate builds as it always did.

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

/**
 * ensureUIBuild serves ui/build when the hash beside it is its inputs' hash
 * and it holds the server entry; otherwise it builds, and writes the hash
 * only once the build has produced that entry. The old hash is removed
 * before a build starts, so a build cut short never leaves a matching hash
 * beside a half-written directory.
 */
export function ensureUIBuild(dir: string = uiDir): 'reused' | 'built' {
	const buildDir = path.join(dir, 'build');
	const hashFile = path.join(buildDir, BUILD_HASH_FILE);
	const hash = buildInputsHash(dir);
	const previous = existsSync(hashFile) ? readFileSync(hashFile, 'utf8').trim() : '';
	if (previous === hash && existsSync(path.join(buildDir, 'index.js'))) {
		console.log(`e2e: ui/build is this tree's own (inputs ${hash.slice(0, 12)}): not rebuilt`);
		return 'reused';
	}
	rmSync(hashFile, { force: true });
	execFileSync('bun', ['run', 'build'], { cwd: dir, stdio: 'inherit' });
	if (!existsSync(path.join(buildDir, 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}
	writeFileSync(hashFile, hash + '\n');
	console.log(`e2e: built ui/build (inputs ${hash.slice(0, 12)})`);
	return 'built';
}

// Run as a script (`bun e2e/build-ui.ts`), not imported: import.meta.main is
// true only for the entry module (and undefined where a runtime lacks it).
if (import.meta.main) {
	ensureUIBuild();
}
