import { test } from 'bun:test';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';

// The repository's files as the unit tests read them. Many tests hold a rule
// over a whole tree (every source of ui/src, every shipped example), and each
// used to list and read it again. `bun test test/` runs its files in one
// process and evaluates this module once, so a tree is listed and a file read
// once for the whole run, whichever test asks first (under --isolate, once a
// file). Nothing here may be used for a file a test writes: what was read
// stays as it was read.

/**
 * SCAN_GUARD_MS is the ceiling of a test that reads a tree. It is against a
 * hang, and measures nothing: how long the reads take is the host's load, not
 * the code's. Bun's own default, 5 s a test, failed one of them at 10 s on a
 * loaded host. Bun cannot interrupt a synchronous read: a test that passes
 * its ceiling fails once it returns.
 */
export const SCAN_GUARD_MS = 60_000;

/** scanTest is a test that reads a tree of the repository. */
export function scanTest(name: string, fn: () => void | Promise<void>): void {
	test(name, fn, SCAN_GUARD_MS);
}

const listings = new Map<string, readonly string[]>();
const texts = new Map<string, string>();

function listed(dir: string, recursive: boolean): string[] {
	const at = `${recursive ? 'tree' : 'dir'}:${path.resolve(dir)}`;
	let names = listings.get(at);
	if (!names) listings.set(at, (names = readdirSync(dir, { recursive, encoding: 'utf8' })));
	// A copy: callers sort and splice.
	return [...names];
}

/** Every entry under dir, at any depth, relative to it. */
export const listTree = (dir: string): string[] => listed(dir, true);

/** dir's own entries. */
export const listDir = (dir: string): string[] => listed(dir, false);

/** A repository file's text. */
export function readSource(file: string): string {
	const at = path.resolve(file);
	let text = texts.get(at);
	if (text === undefined) texts.set(at, (text = readFileSync(at, 'utf8')));
	return text;
}
