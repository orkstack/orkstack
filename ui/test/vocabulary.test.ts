import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';

// AGENTS.md §7: "Queue means only an internal Temporal task queue and never a
// user-facing OrkStack concept." How long a task waited before an Orker took
// it is said as exactly that — "waited for an Orker" — on every surface a
// user reads: the UI's sources (generated code aside), the CLI's non-test
// sources, and the docs. Internal identifiers (QueueWait, queueWait) are not
// prose and are not checked.
const root = `${import.meta.dir}/../..`;

function files(dir: string, keep: (path: string) => boolean): string[] {
	return readdirSync(dir, { recursive: true })
		.map((rel) => `${dir}/${rel}`)
		.filter(keep);
}

test('no user-facing source says "queue wait"', () => {
	const surfaces = [
		...files(`${root}/ui/src`, (p) => /\.(ts|svelte)$/.test(p) && !p.includes('/lib/gen/')),
		...files(`${root}/cmd/orkctl`, (p) => p.endsWith('.go') && !p.endsWith('_test.go')),
		...files(`${root}/cmd/orkctl/testdata`, (p) => p.endsWith('.golden')),
		...files(`${root}/docs`, (p) => p.endsWith('.md'))
	];
	expect(surfaces.length > 50).toBe(true);
	const offenders = surfaces
		.filter((path) => /queue[\s-]+wait/i.test(readFileSync(path, 'utf8')))
		.map((path) => path.slice(root.length + 1));
	expect(offenders).toEqual([]);
});
