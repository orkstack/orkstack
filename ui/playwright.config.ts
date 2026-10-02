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
