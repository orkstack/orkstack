import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startBrowserServer } from './browser-server';

// Everything every spec needs built, built ONCE per Playwright run rather than
// in each spec's beforeAll (which was 39 vite builds and as many Go builds of
// each binary per run):
//
//   - the binaries the specs start (harness.ts bin()). Under a proof they are
//     the proof's own, built from the tree it proves into proof/<name>/bin and
//     exported as ORK_TEST_BIN_DIR; this only builds what is missing there.
//     Outside a proof they go to a temporary directory global-teardown.ts
//     removes. Never the repository's bin/ (AGENTS.md §11.1).
//   - the UI, `vite build` into ui/build, which every spec serves with
//     `node build`;
//   - the browser server, in a container and so in a network namespace of
//     its own (browser-server.ts says why), its endpoint exported as
//     ORK_E2E_BROWSER_WS for playwright.config.ts to connect every worker
//     to, and its container id as ORK_E2E_BROWSER_CONTAINER for
//     global-teardown.ts to stop. ORK_E2E_BROWSER=host skips it and drives
//     the host's own Chromium, for a developer without the image.
//
// It imports nothing from harness.ts on purpose: the harness holds per-worker
// state (the processes a spec started), and the setup runs in the runner.

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(uiDir, '..');

/** The binaries the specs run, and the package each is built from. */
export const suiteBinaries: Record<string, string> = {
	ork: './cmd/ork',
	orker: './cmd/orker',
	orkctl: './cmd/orkctl',
	'cosign-sign': './test/tools/cosign-sign',
	'deaf-poller': './test/tools/deaf-poller',
	// The helpers specs used to `go run`: a compile and link per call (about
	// 24 calls a run) against a build once here.
	'bucket-ensure': './test/tools/bucket-ensure',
	'reclaim-run': './test/tools/reclaim-run',
	generations: './test/tools/generations',
	'archive-legacy-run': './test/tools/archive-legacy-run'
};

export default function globalSetup(): void {
	let dir = process.env.ORK_TEST_BIN_DIR;
	if (!dir) {
		dir = mkdtempSync(path.join(os.tmpdir(), 'orkstack-e2e-bin-'));
		process.env.ORK_TEST_BIN_DIR = dir;
		process.env.ORK_E2E_OWN_BIN_DIR = dir;
	}
	for (const [name, pkg] of Object.entries(suiteBinaries)) {
		const target = path.join(dir, name);
		if (!existsSync(target)) {
			execFileSync('go', ['build', '-o', target, pkg], { cwd: repoRoot, stdio: 'inherit' });
		}
	}
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}
	if (process.env.ORK_E2E_BROWSER === 'host') {
		console.log('e2e: ORK_E2E_BROWSER=host: driving the host\'s own Chromium, in the host\'s network namespace');
		return;
	}
	const server = startBrowserServer(process.env.ORK_PROOF_ID ?? `e2e-${process.pid}`);
	process.env.ORK_E2E_BROWSER_WS = server.wsEndpoint;
	process.env.ORK_E2E_BROWSER_CONTAINER = server.container;
	console.log(`e2e: browser server ${server.image} in container ${server.container.slice(0, 12)} at ${server.wsEndpoint}`);
}
