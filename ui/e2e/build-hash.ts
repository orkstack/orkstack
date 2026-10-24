import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';

// The UI build the specs serve (`vite build` into ui/build) is rebuilt only
// when what goes into it changed. Its inputs are hashed — every file under
// the directories and at the paths given, by relative path and bytes, in path
// order — and the hash is written beside the build. A later run with the same
// hash serves the build it finds; any other hash, or no marker, rebuilds.
//
// Two reasons. A proof with several Playwright workers runs the suite as two
// invocations (scripts/proof.sh), and the second's build would be the first's
// again. And `playwright test one.spec.ts` by hand paid a full build every
// time, whatever had changed. A proof's tree is clean and unchanged from start
// to end (AGENTS.md §11.2), so under a proof the reuse is of this tree's own
// build by construction; by hand it is as exact as the input list, which is
// why that list is what this module exports and tests.

/** The marker file's name, written in the build directory. */
export const BUILD_HASH_FILE = '.orkstack-build-hash';

/**
 * The build's inputs, relative to ui/: directories are walked, files are
 * taken as they are. Generated clients live under src/lib/gen, so a proto
 * change reaches the hash through them.
 */
export const BUILD_INPUTS = [
	'src',
	'static',
	'svelte.config.js',
	'vite.config.ts',
	'tsconfig.json',
	'package.json',
	'bun.lock'
];

/**
 * buildInputsHash is the SHA-256 over `inputs` under `root`: for each file,
 * in sorted relative-path order, its path, a NUL, its bytes, a NUL. A path
 * that does not exist contributes its name and nothing else, so a removed
 * file changes the hash as an added one does.
 */
export function buildInputsHash(root: string, inputs: string[] = BUILD_INPUTS): string {
	const files: string[] = [];
	for (const input of inputs) {
		const full = `${root}/${input}`;
		let entries: string[] | undefined;
		try {
			entries = readdirSync(full, { recursive: true });
		} catch {
			// A file, or nothing: readdirSync refuses both.
		}
		if (entries) {
			for (const e of entries) files.push(`${input}/${String(e)}`);
		} else {
			files.push(input);
		}
	}
	files.sort();
	const h = createHash('sha256');
	for (const f of files) {
		h.update(f);
		h.update('\0');
		try {
			h.update(readFileSync(`${root}/${f}`));
		} catch {
			// A directory entry of the walk, or a missing input: its name alone.
		}
		h.update('\0');
	}
	return h.digest('hex');
}
