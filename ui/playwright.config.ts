import { defineConfig } from '@playwright/test';

// The realtime e2e drives the BUILT UI (adapter-node) against a real engine +
// Orker on live local infra (Temporal :7233, MinIO :9000). Processes are
// started and always killed by the spec itself; see e2e/realtime.spec.ts.
export default defineConfig({
	testDir: './e2e',
	timeout: 180_000,
	expect: { timeout: 15_000 },
	fullyParallel: false,
	workers: 1,
	retries: 0,
	reporter: [['list']],
	use: {
		trace: 'on',
		screenshot: 'only-on-failure',
		video: 'off'
	},
	projects: [{ name: 'chromium', use: { browserName: 'chromium' } }]
});
