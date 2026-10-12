import { defineConfig } from '@playwright/test';
import path from 'node:path';

// The realtime e2e drives the BUILT UI (adapter-node) against a real engine +
// Orker on live local infra (Temporal :7233, MinIO :9000). Processes are
// started and always killed by the spec itself; see e2e/realtime.spec.ts.
//
// Under a proof (ORK_PROOF_DIR set by scripts/proof.sh) Playwright's own
// output — each failing test's trace, screenshot and error context — is kept
// inside the proof directory, beside the transcripts the specs write there. In
// ./test-results the next run wiped it, so a proof's failure could be read
// only as its one line in ui-e2e.log and never explained. Only failures keep a
// trace there: what a passing test proves is what its spec writes itself.
const proofDir = process.env.ORK_PROOF_DIR;

/**
 * NETWORK_CHANGE_RETRY_CEILING is how many navigations the network-change
 * guard (e2e/test.ts) may repeat in one run before the proof's ui-e2e gate
 * fails with "network churn above ceiling; evidence taken on a noisy host"
 * (test/tools/proofrun reads this line). Each repeat is a navigation Chromium
 * aborted because the HOST's network interfaces changed — a container
 * starting or stopping anywhere on the host — so a run above the ceiling was
 * measured on a host too noisy to trust. The serial baseline is 1; while the
 * browser shares the host's network namespace the ceiling is 3. Once the
 * browser runs in a network namespace of its own the ceiling is 0, because
 * then any repeat means the isolation broke.
 */
export const NETWORK_CHANGE_RETRY_CEILING = 3;

export default defineConfig({
	testDir: './e2e',
	outputDir: proofDir ? path.join(proofDir, 'test-results') : 'test-results',
	timeout: 180_000,
	expect: { timeout: 15_000 },
	fullyParallel: false,
	workers: 1,
	retries: 0,
	reporter: [['list']],
	use: {
		trace: proofDir ? 'retain-on-failure' : 'on',
		screenshot: 'only-on-failure',
		video: 'off'
	},
	projects: [{ name: 'chromium', use: { browserName: 'chromium' } }]
});
