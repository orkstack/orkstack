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
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-force-cancel');

// Force cancel from a run's page, in a browser, against the real stack (E88
// P10):
//
//   F1  Force cancel opens a dialog that holds its confirm until a reason is
//       given, with the reason why, and names the safe choice; Keep running
//       sends nothing;
//   F2  confirming a healthy run ends it on its own cancel: the pill reads
//       Canceled, never force-canceled, and the Force cancel card says so,
//       with who asked and why;
//   F3  a run with an open child run offers to end it too, and lists it; with
//       that chosen the card says what became of the child, and nothing is
//       left open.
//
// Ports and a Temporal namespace of this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18395';
const TEMPORAL_NAMESPACE = temporalNamespace('run-force-cancel');
const STAMP = Date.now().toString(36);
const CHILD = `fc-child-${STAMP}`;
const PARENT = `fc-parent-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-force-cancel')
	: path.join(uiDir, 'test-results', 'run-force-cancel');
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, 'run-force-cancel-session.txt'));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// slow runs until it is stopped, and stops at once when asked.
const child = `apiVersion: orkstack/v1
kind: Orkflow
name: ${CHILD}
version: "0.1.0"
tasks:
  - id: slow
    uses: shell
    timeout_seconds: 600
    with:
      op: exec
      command: ["sh", "-c", "trap 'exit 0' TERM; sleep 500 & wait"]
`;
const parent = `apiVersion: orkstack/v1
kind: Orkflow
name: ${PARENT}
version: "0.1.0"
tasks:
  - id: below
    uses: orkflow:${CHILD}@0.1.0
`;

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18395)) throw new Error('port 18395 already in use (stray process?)');
	ensureActivityImages();
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18395',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv('run-force-cancel'),
		ORK_ORKER_ID: 'orker-e2e-force-cancel'
	});
	for (const yaml of [child, parent]) await rpc('OrkflowService/ApplyOrkflow', { yaml });
	note(`applied ${CHILD}, ${PARENT}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

async function submit(orkflowName: string): Promise<string> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName, allowEmptyPool: true });
	note(`submitted ${submitted.run.id} of ${orkflowName}`);
	return submitted.run.id;
}

interface RunView {
	run: { status?: string; taskRuns?: { status?: string; child?: { runId?: string } }[] };
}
const read = (runId: string) => rpc<RunView>('RunService/GetRun', { runId });

// Force cancel is in the Cancel dialog: the header keeps the run's name.
async function openForceCancel(page: Page): Promise<void> {
	await page.getByTestId('cancel-run').click();
	await page.getByTestId('force-cancel-instead').click();
	await expect(page.getByTestId('cancel-run-confirm')).toHaveCount(0);
}

test('a run is force-canceled from its page', async ({ page }) => {
	// Longer than every wait below, so a late one fails by its own name.
	test.setTimeout(600_000);
	const run = await submit(CHILD);
	await expect.poll(async () => (await read(run)).run.taskRuns?.[0]?.status, { timeout: 120_000 }).toBe('STATUS_RUNNING');
	await page.goto(`${ENGINE}/runs/${run}?ns=default`);

	// The header keeps its room: the name is there, and the status never runs
	// into the actions.
	await page.setViewportSize({ width: 1280, height: 720 });
	expect((await page.getByTestId('pinned-orkflow-link').boundingBox())!.width).toBeGreaterThan(20);
	const pill = (await page.getByTestId('run-status').boundingBox())!;
	const first = (await page.getByTestId('run-evidence').boundingBox())!;
	expect(pill.x + pill.width).toBeLessThanOrEqual(first.x);

	// F1
	await openForceCancel(page);
	const dialog = page.getByTestId('force-cancel-confirm');
	await expect(dialog).toBeVisible();
	await expect(page.getByTestId('force-cancel-dismiss')).toBeFocused();
	await expect(page.getByTestId('force-cancel-accept')).toHaveAttribute('aria-disabled', 'true');
	await expect(dialog).toContainText('Give a reason');
	await expect(page.getByTestId('force-cancel-below-status')).toHaveText('No open run below it.');
	await expect(page.getByTestId('force-cancel-cascade')).toHaveCount(0);
	await shot(page, 'f1-dialog.png');
	await page.emulateMedia({ colorScheme: 'dark' });
	await shot(page, 'f1-dialog-dark.png');
	await page.emulateMedia({ colorScheme: 'light' });
	await page.getByTestId('force-cancel-dismiss').click();
	await expect(dialog).toHaveCount(0);
	// Keeping it running asked for nothing: there is no force cancel to read.
	await expect(rpc('RunService/GetForceCancel', { runId: run })).rejects.toThrow(/404|not_found/);

	// F2
	await openForceCancel(page);
	await page.getByTestId('force-cancel-reason').fill('stuck after the roll');
	await expect(page.getByTestId('force-cancel-accept')).not.toHaveAttribute('aria-disabled', 'true');
	await page.getByTestId('force-cancel-accept').click();
	await expect(dialog).toHaveCount(0);
	const card = page.getByTestId('run-force-cancel');
	await expect(card).toBeVisible();
	await expect(page.getByTestId('run-force-cancel-reason')).toHaveText('stuck after the roll');
	await expect(card).toContainText('asserted');
	await shot(page, 'f2-in-progress.png');
	await expect(page.getByTestId('run-status')).toContainText('Canceled', { timeout: 120_000 });
	await expect(page.getByTestId('run-force-cancel-state')).toHaveText('The run ended on its own cancel', { timeout: 120_000 });
	await expect(page.getByTestId('cancel-run')).toHaveCount(0);
	await expect(page.getByTestId('force-cancel-after')).toHaveCount(0);
	await expect(page.getByTestId('force-cancel-again')).toHaveCount(0);
	await shot(page, 'f2-ended.png');
});

test('a run and the open run below it are force-canceled', async ({ page }) => {
	test.setTimeout(900_000);
	const run = await submit(PARENT);
	let below = '';
	await expect
		.poll(async () => (below = (await read(run)).run.taskRuns?.[0]?.child?.runId ?? ''), { timeout: 120_000 })
		.not.toBe('');
	await expect.poll(async () => (await read(below)).run.taskRuns?.[0]?.status, { timeout: 120_000 }).toBe('STATUS_RUNNING');
	// The list below is the cluster's index's, which lags a start.
	await expect
		.poll(async () => (await rpc<{ openDescendants?: { runs?: unknown[] } }>('RunService/ForceCancelRun', { runId: run, preview: true })).openDescendants?.runs?.length ?? 0, { timeout: 60_000 })
		.toBe(1);
	await page.goto(`${ENGINE}/runs/${run}?ns=default`);
	await openForceCancel(page);
	const choice = page.getByTestId('force-cancel-cascade');
	await expect(choice).toBeVisible();
	await expect(page.getByTestId('force-cancel-confirm')).toContainText('Also force-cancel its 1 open child run');
	await expect(page.getByTestId('force-cancel-below-status')).toHaveText('1 open run below it.');
	await page.getByTestId('force-cancel-confirm').locator('summary').click();
	await expect(page.getByTestId('force-cancel-below')).toContainText(CHILD);
	await choice.check();
	await expect(page.getByTestId('force-cancel-confirm')).toContainText('force-canceled too');
	await page.getByTestId('force-cancel-reason').fill('stuck tree');
	await shot(page, 'f3-dialog.png');
	await page.getByTestId('force-cancel-accept').click();
	await expect(page.getByTestId('run-force-cancel-cascade')).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId('run-force-cancel-cascade')).toContainText('1 run below ended, and none is open', { timeout: 240_000 });
	await expect(page.getByTestId('run-force-cancel-rows')).toContainText(/Canceled|Force-canceled|Had ended|Ended/);
	await shot(page, 'f3-done.png');
	await page.setViewportSize({ width: 390, height: 844 });
	await shot(page, 'f3-done-phone.png');
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
	expect((await read(below)).run.status).toBe('STATUS_CANCELED');
});
