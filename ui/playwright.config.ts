import { defineConfig } from '@playwright/test';
import path from 'node:path';

// The e2e specs drive the BUILT UI (adapter-node) against a real engine +
// Orker on live local infra: Temporal (the proof's own, at
// ORK_TEMPORAL_ADDRESS, or `make infra-up`'s :7233) and MinIO :9000.
// Processes are started and always killed by each spec itself.
//
// Spec files run several at a time (`workers`; scripts/proof.sh passes
// PROOF_UI_WORKERS). The browser runs in a network namespace of its own
// (e2e/browser-server.ts), so a container started or stopped on the host —
// by another spec's tasks, or by anything else on the host — never reaches
// it; every spec file owns its ports, its bucket, its Temporal namespace and
// its Orker work roots, so files share nothing but the daemon and the host's
// CPU. Each file is still serial inside (`fullyParallel: false`).
//
// Under a proof (ORK_PROOF_DIR set by scripts/proof.sh) Playwright's own
// output — each failing test's trace, screenshot and error context — is kept
// inside the proof directory, beside the transcripts the specs write there. In
// ./test-results the next run wiped it, so a proof's failure could be read
// only as its one line in ui-e2e.log and never explained. Only failures keep a
// trace, under a proof and by hand alike: what a passing test proves is what
// its spec writes itself, and zipping and carrying a passing test's trace
// over the browser connection was cost for nothing (`--trace on` on the
// command line records one when a passing test must be inspected).
const proofDir = process.env.ORK_PROOF_DIR;

// The browser server e2e/global-setup.ts started (e2e/browser-server.ts): a
// Chromium in a network namespace of its own. Every worker connects to it, and
// `exposeNetwork: '<loopback>'` carries the browser's requests to 127.0.0.1 —
// the engine and UI each spec starts on the host — back over the connection,
// so a spec's URLs are unchanged. The runner's own locale and time zone are
// pinned on the browser, because the container's are en-US and UTC and a
// rendered timestamp must read as it did when Chromium ran on this host.
// This config is evaluated again in every worker, after global setup set the
// variable; in the runner process it is still unset, and the runner opens no
// browser. ORK_E2E_BROWSER=host leaves it unset and drives the host's Chromium.
const browserWS = process.env.ORK_E2E_BROWSER_WS;
const hostIntl = Intl.DateTimeFormat().resolvedOptions();
/** The connection to the isolated browser, for the projects that use it. */
const isolatedBrowser = browserWS
	? { connectOptions: { wsEndpoint: browserWS, exposeNetwork: '<loopback>', timeout: 30_000 } }
	: {};

/**
 * NETWORK_CHANGE_RETRY_CEILING is how many navigations the network-change
 * guard (e2e/test.ts) may repeat in one run before the proof's ui-e2e gate
 * fails with "network churn above ceiling; evidence taken on a noisy host"
 * (test/tools/proofrun reads this line). Each repeat is a navigation Chromium
 * aborted because the HOST's network interfaces changed — a container
 * starting or stopping anywhere on the host — so a run above the ceiling was
 * measured on a host too noisy to trust. While the browser shared the host's
 * network namespace the ceiling was 3. The browser now runs in a network
 * namespace of its own (e2e/browser-server.ts; network-change.spec.ts N5/N6
 * prove it, with a host-namespace browser as the positive control), so the
 * ceiling is 0: any repeat means the isolation broke. network-change.spec.ts
 * provokes repeats on purpose, on a host-namespace browser of its own, and
 * reports them to its transcript rather than the suite's log (test.ts
 * guardPage), so they are never counted here.
 */
export const NETWORK_CHANGE_RETRY_CEILING = 0;

/** The spec files that assert a render budget and therefore run alone (see `projects`). */
export const SOLO_SPECS = [/each-fan-out-ceiling\.spec\.ts$/, /run-page(-reduced)?-ceiling\.spec\.ts$/];

export default defineConfig({
	testDir: './e2e',
	// The binaries and the UI build every spec uses, built once per run.
	globalSetup: './e2e/global-setup.ts',
	globalTeardown: './e2e/global-teardown.ts',
	outputDir: proofDir ? path.join(proofDir, 'test-results') : 'test-results',
	timeout: 180_000,
	expect: { timeout: 15_000 },
	fullyParallel: false,
	workers: Number(process.env.PROOF_UI_WORKERS ?? 2),
	retries: 0,
	reporter: [['list']],
	use: {
		locale: hostIntl.locale,
		timezoneId: hostIntl.timeZone,
		trace: 'retain-on-failure',
		screenshot: 'only-on-failure',
		video: 'off'
	},
	// Three projects, one browser. `chromium` holds every spec file but the
	// two that assert an absolute render budget (the run page at the task
	// ceiling, the fan-out at the item ceiling: "interactive within 5 s"). Each
	// of those files holds nothing but its budget test, so nothing that could
	// run beside the others is serialised behind them (the fan-out's
	// functional tests are each-fan-out.spec.ts, in `chromium`). A
	// budget is a measurement of a quiet host, as a heavy integration test is
	// (test/tools/proofrun/plan.go), so each of those files runs ALONE: a
	// proof with more than one worker runs `--project chromium` first, then
	// the two solo projects on one worker (scripts/proof.sh ui_e2e_locked),
	// and its ledger reads both reports. Measured with three files beside it,
	// the fan-out page took 9.7 s against its 5 s budget; alone it stays
	// within it. They are projects rather than `dependencies`, because
	// Playwright runs a dependency project in full whenever one file is asked
	// for, and `playwright test some.spec.ts` must stay one file's run. By
	// hand, `playwright test --project chromium-solo-each-fan-out` runs one.
	//
	// The run page at the task ceiling is the one spec still driven on the
	// HOST's Chromium. Measured alone on 2026-10-23 (ORK_CEILING_BROWSER):
	// through the connection, four of five opens of the finished 4,000-task
	// page matched the host (4.0–4.5 s to interactive, medians 4.28 s against
	// 4.12 s) and one stalled, its header 41 s after navigation; in two earlier
	// runs the same stall outlasted the spec's 60 s wait. The stall is in the
	// connection's relay of one page's worth of requests, not in the page, and
	// until it is understood the measurement is taken where it is steady. The
	// spec runs alone, after every other file, and its tasks are
	// `network: none`, so no interface of its own changes under it; a repeat
	// the guard makes there still counts against the ceiling, as it should.
	projects: [
		{
			name: 'chromium',
			use: { browserName: 'chromium', ...isolatedBrowser },
			testIgnore: SOLO_SPECS
		},
		{
			name: 'chromium-solo-each-fan-out',
			use: { browserName: 'chromium', ...isolatedBrowser },
			testMatch: /each-fan-out-ceiling\.spec\.ts$/
		},
		{
			name: 'chromium-solo-run-page-ceiling',
			// ORK_CEILING_BROWSER=container drives it through the isolated browser
			// instead: the comparison that establishes the cause.
			use: { browserName: 'chromium', ...(process.env.ORK_CEILING_BROWSER === 'container' ? isolatedBrowser : {}) },
			// The reduced ceiling in the merge gate, and the 4,000-task default
			// where the nightly tier and the pre-release gate run it.
			testMatch: /run-page(-reduced)?-ceiling\.spec\.ts$/
		}
	]
});
