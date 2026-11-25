import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	noteTo,
	orkerEnv,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	spawnLogged,
	tcpOpen,
	storeEnv,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('concurrency');

// A concurrency limit in a browser, against the real stack (E88 P9a):
//
//   C1  the Orkflow page names its limit, in `orkctl orkflow get`'s words;
//   C2  a run past the limit shows the Queued card: why it waits, the
//       limit, its place, the run it waits behind as a link, and when it
//       times out;
//   C3  the Runs list's Queued tile counts it and filters to it, and the
//       dashboard's Running count says how many of the running are queued;
//   C4  on the queued run's open page, when the run holding the key ends,
//       the queued run starts and its card goes, live.
//
// Ports and a Temporal namespace of this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18867';
const TEMPORAL_NAMESPACE = temporalNamespace('concurrency');
const STAMP = Date.now().toString(36);
const ORKFLOW = `concurrency-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'concurrency')
	: path.join(uiDir, 'test-results', 'concurrency');
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, 'concurrency-session.txt'));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// One run at a time on the Orkflow's own key. hold keeps the slot far longer
// than the test reads the queued run; the test then ends the holder itself,
// by a cancel, so the moment the slot frees is the test's own.
function orkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: One run at a time.

concurrency:
  limit: 1

tasks:
  - id: hold
    uses: shell
    timeout_seconds: 600
    cancellation_grace_period_seconds: 5
    with:
      op: exec
      command: ["sh", "-c", "trap 'exit 0' TERM; sleep 500 & wait"]
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18867)) throw new Error('port 18867 already in use (stray process?)');
	ensureActivityImages();
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18867',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv('concurrency'),
		ORK_ORKER_ID: 'orker-e2e-concurrency'
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow() });
	note(`applied ${ORKFLOW}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

// The runs this test started, canceled whatever it ends with and waited for,
// so no 500 s holder outlives the Orker. The queued one goes first, so ending
// the holder starts nothing.
const started: string[] = [];
test.afterEach(async () => {
	for (const runId of [...started].reverse()) {
		await rpc('RunService/CancelRun', { runId, namespace: 'default', reason: 'concurrency e2e: cleanup' }).catch(() => undefined);
	}
	for (const runId of started) {
		await waitFor(
			`run ${runId} to end`,
			async () => /SUCCEEDED|FAILED|CANCELED/.test((await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId, namespace: 'default' })).run?.status ?? ''),
			60_000
		);
	}
});

test('a run past its limit waits in line, says why, and starts when the holder ends', async ({ page }) => {
	test.setTimeout(300_000);

	// C1 — the Orkflow page names the limit.
	await page.goto(`${ENGINE}/orkflows/${ORKFLOW}?ns=default`);
	await expect(page.getByTestId('orkflow-concurrency')).toContainText(
		"limit 1 on this Orkflow's own key; the rest wait in line up to the deployment's default"
	);
	await shot(page, 'orkflow-concurrency.png');

	// The holder runs first; the Orker may still be booting, which the wait
	// for its task's Running bounds.
	const first = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, allowEmptyPool: true })).run.id;
	started.push(first);
	await page.goto(`${ENGINE}/runs/${first}?ns=default`);
	await expect(page.getByTestId('task-hold-status')).toHaveText('Running', { timeout: 120_000 });
	await expect(page.getByTestId('run-queued')).toHaveCount(0);
	note(`holder ${first} running`);

	// C2 — a second run queues behind it.
	const second = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, allowEmptyPool: true })).run.id;
	started.push(second);
	note(`submitted ${second}`);
	await page.goto(`${ENGINE}/runs/${second}?ns=default`);
	const card = page.getByTestId('run-queued');
	await expect(card).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('run-queued-key')).toHaveText(/\S/);
	// A run that waits on arrival needs no sentence beside its heading and key.
	await expect(page.getByTestId('run-queued-headline')).toHaveCount(0);
	await expect(page.getByTestId('run-queued-limit')).toHaveText('1 run at once');
	await expect(page.getByTestId('run-queued-place')).toHaveText(/^(\d+(st|nd|rd|th) in line|Unknown)$/);
	await expect(page.getByTestId('run-queued-times-out')).toHaveText(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
	await expect(page.getByTestId('task-hold-status')).toHaveText('Pending');
	// The run it waits behind is named and opens.
	await expect(page.getByTestId('run-queued-holder')).toHaveCount(1, { timeout: 30_000 });
	await expect(page.getByTestId('run-queued-holder')).toHaveAttribute('href', new RegExp(`/runs/${first}\\b`));
	note(`C2 card: ${await card.innerText()}`);
	await shot(page, 'queued-light.png');
	await page.emulateMedia({ colorScheme: 'dark' });
	await shot(page, 'queued-dark.png');
	await page.emulateMedia({ colorScheme: 'light' });
	await page.setViewportSize({ width: 390, height: 844 });
	await expect(card).toBeVisible();
	await shot(page, 'queued-phone.png');
	await page.setViewportSize({ width: 1280, height: 800 });

	// C3 — the list's Queued tile counts and filters it; the dashboard says it.
	await page.goto(`${ENGINE}/runs?ns=default`);
	const tile = page.getByTestId('runs-tile-queued');
	await expect(tile).toBeVisible({ timeout: 30_000 });
	await expect(tile).toContainText('Queued');
	await expect(tile.locator('.num')).toHaveText('1');
	await tile.click();
	await expect(tile).toHaveAttribute('aria-pressed', 'true');
	await expect(page).toHaveURL(/queued/);
	await expect(page.getByRole('row').filter({ hasText: second.slice(-6) })).toHaveCount(1, { timeout: 30_000 });
	await expect(page.getByRole('row').filter({ hasText: first.slice(-6) })).toHaveCount(0);
	await shot(page, 'runs-queued.png');
	await page.goto(`${ENGINE}/?ns=default`);
	await expect(page.getByTestId('stat-running-note')).toHaveText('1 queued', { timeout: 30_000 });
	await shot(page, 'dashboard-queued.png');

	// C4 — on the queued run's open page, the holder ends; the queued run
	// takes its slot and its card goes without a reload. The card still
	// showing right before the cancel is the positive control.
	await page.goto(`${ENGINE}/runs/${second}?ns=default`);
	await expect(card).toBeVisible({ timeout: 30_000 });
	await rpc('RunService/CancelRun', { runId: first, namespace: 'default', reason: 'concurrency e2e: free the slot' });
	await expect(page.getByTestId('task-hold-status')).toHaveText('Running', { timeout: 120_000 });
	await expect(page.getByTestId('run-queued')).toHaveCount(0, { timeout: 30_000 });
	note(`C4 ${second} started once ${first} ended, its card gone live`);
});
