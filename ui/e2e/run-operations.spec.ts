import { expect, test } from './test';
import { mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
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
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-operations');

// This is an operator flow, not a mocked RPC test. It proves that a browser
// can request cancellation without painting a made-up terminal status, then
// retain its namespace/version coordinates while it prepares a rerun.
const ENGINE = 'http://127.0.0.1:18088';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('run-operations');
const NAME = `run-operations-${Date.now().toString(36)}`;
const COMPLETED_NAME = `${NAME}-completed`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-operations')
	: path.join(uiDir, 'test-results', 'run-operations');
const out = proofOut(PROOF_DIR);

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function workflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
description: Browser run operations coverage.
tasks:
  - id: wait
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo "begin operation"
        sleep 45
        printf '{}' > /orkstack/output/values.json
`;
}

function completedWorkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${COMPLETED_NAME}
version: "0.1.0"
description: Retained output and artifact download coverage.
tasks:
  - id: completed
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le 2105 ]; do echo "retained line $i"; i=$((i+1)); done
        printf REPORT > /orkstack/output/artifacts/report.txt
        printf '{"answer":7}' > /orkstack/output/values.json
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18088)) throw new Error('port 18088 already in use');

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ORK_HTTP_ADDR: '127.0.0.1:18088', ...uiEnv()
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('run-operations') });
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: completedWorkflow() });
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('cancel confirmation, authoritative status, pinned version, deep links and rerun review survive one operator flow', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME });
	const runId = submitted.run.id;
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING', { timeout: 60_000 });
	const initialElapsed = await page.getByTestId('run-elapsed').innerText();
	await expect.poll(() => page.getByTestId('run-elapsed').innerText()).not.toBe(initialElapsed);
	await expect(page.getByTestId('pinned-orkflow-link')).toHaveAttribute('href', new RegExp(`version=0.1.0.*ns=default`));
	await page.getByTestId('cancel-run').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	// F-ui-ux-2: a destructive confirmation takes the focus on Cancel, Tab and
	// Shift+Tab never leave it, and dismissing it hands the focus back.
	await expect(page.getByTestId('cancel-run-dismiss')).toBeFocused();
	const insideConfirm = () =>
		page.evaluate(() => !!document.activeElement?.closest('[data-testid="cancel-run-confirm"]'));
	for (let i = 0; i < 6; i++) {
		await page.keyboard.press('Tab');
		expect(await insideConfirm(), `Tab ${i + 1} stays in the dialog`).toBe(true);
	}
	for (let i = 0; i < 6; i++) {
		await page.keyboard.press('Shift+Tab');
		expect(await insideConfirm(), `Shift+Tab ${i + 1} stays in the dialog`).toBe(true);
	}
	await page.screenshot({ path: out('screenshot-cancel-confirm-focus.png'), fullPage: true });
	await page.getByTestId('cancel-run-dismiss').click();
	await expect(page.getByTestId('cancel-run-confirm')).toHaveCount(0);
	await expect(page.getByTestId('cancel-run')).toBeFocused();
	await page.getByTestId('cancel-run').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	await page.getByTestId('cancel-reason').fill('operator cancelled this run');
	await page.getByTestId('cancel-run-accept').click();
	await expect(page.getByTestId('cancellation-requested')).toContainText('Waiting for the run to stop');
	await page.screenshot({ path: out('screenshot-cancel-requested.png'), fullPage: true });
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED', { timeout: 90_000 });

	await page.getByTestId('task-deep-link-wait').click();
	await expect(page).toHaveURL(/task=wait/);
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default&task=wait&attempt=1`);
	await expect(page).toHaveURL(/task=wait.*attempt=1/);
	await expect(page.getByTestId('task-log-wait')).toBeVisible({ timeout: 30_000 });
	await page.reload();
	await expect(page).toHaveURL(/task=wait.*attempt=1/);
	await expect(page.getByTestId('task-log-wait')).toBeVisible({ timeout: 30_000 });
	await page.getByTestId('task-log-search-wait').fill('begin');
	await expect(page.getByTestId('task-log-wait')).toContainText('begin operation');
	// "Run again" opens a review on THIS page, seeded from the run already
	// loaded here rather than from a URL that would have to carry the input.
	// The coordinates it retains are still checkable: the banner names the run
	// it was copied from, and the draft pins that run's own version.
	await page.getByTestId('run-again').click();
	await expect(page.getByTestId('run-again-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-panel')).toBeVisible();
	await expect(page.getByTestId('run-submit-replay-banner')).toContainText(runId);
	await expect(page.getByTestId('run-submit-version-mode')).toContainText(
		'Pinned explicitly to 0.1.0'
	);
	await page.screenshot({ path: out('screenshot-rerun-review.png'), fullPage: true });
});

test('completed values, bounded pane replay, JSON export and recorded artifacts are downloadable', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: COMPLETED_NAME, input: { data: { requested: 'evidence' } } });
	const runId = submitted.run.id;
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	// activity-shell owns values.json for its script mode; the script's own
	// output remains a retained log, while the public task value records its
	// measured exit and script path.
	await expect(page.getByTestId('task-values-completed')).toContainText('"exitCode": 0');

	const jsonDownload = page.waitForEvent('download');
	await page.getByTestId('export-run-json').click();
	const jsonFile = await jsonDownload;
	expect(jsonFile.suggestedFilename()).toBe(`${runId}.json`);
	const jsonPath = out(`${runId}.json`);
	await jsonFile.saveAs(jsonPath);
	const exported = JSON.parse(readFileSync(jsonPath, 'utf8'));
	expect(exported.id).toBe(runId);
	expect(exported.orkflow).toEqual(expect.objectContaining({ name: COMPLETED_NAME, version: '0.1.0' }));
	expect(exported.input).toEqual({ requested: 'evidence' });
	expect(exported.tasks[0].output).toBeTruthy();

	await page.getByTestId('task-log-toggle-completed').click();
	await expect(page.getByTestId('task-log-dropped-completed')).toContainText('earlier lines', { timeout: 60_000 });
	await page.getByTestId('task-log-search-completed').fill('retained line 2105');
	await expect(page.getByTestId('task-log-completed')).toContainText('retained line 2105');
	const logDownload = page.waitForEvent('download');
	await page.getByTestId('task-log-download-completed').click();
	const logFile = await logDownload;
	expect(logFile.suggestedFilename()).toContain(`${runId}-completed-attempt-`);
	const logPath = out(`${runId}-completed.log`);
	await logFile.saveAs(logPath);
	const retained = readFileSync(logPath, 'utf8');
	expect(retained).toContain('retained line 1');
	expect(retained).toContain('retained line 2105');

	// Recorded artifacts are inspection detail: the overview above carries the
	// task's values and logs, and the download list is behind that task's own
	// Inspect, in that task's own card. Open it through the card's toggle
	// rather than by rewriting the URL, so everything asserted so far was read
	// from the plain view — and the address never moved for it.
	await page.getByTestId('task-inspect-toggle-completed').click();
	await expect(
		page.getByTestId('task-completed').getByTestId('task-artifacts-completed')
	).toBeVisible();
	await expect(page).not.toHaveURL(/inspect=1|events=1/);
	const artifactDownload = page.waitForEvent('download');
	await page.getByTestId('download-artifact-completed-report.txt').click();
	const artifactFile = await artifactDownload;
	expect(artifactFile.suggestedFilename()).toBe('report.txt');
	const artifactPath = out(`${runId}-report.txt`);
	await artifactFile.saveAs(artifactPath);
	expect(readFileSync(artifactPath, 'utf8')).toBe('REPORT');
	await page.screenshot({ path: out('screenshot-completed-retrieval.png'), fullPage: true });
});

// F-ui-ux-4: a slow loader on a main route shows the navigation bar (after
// its 150 ms grace) with <main> aria-busy, and both clear once the page is
// ready. The run page's GetRun is held for a second on the browser side.
test('a slow client navigation shows the progress bar until the destination loads', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: COMPLETED_NAME, input: { data: { requested: 'progress' } } });
	const runId = submitted.run.id;
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${runId}`)).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('nav-progress')).toHaveCount(0);
	await page.route('**/orkstack.v1.RunService/GetRun', async (route) => {
		await new Promise((resolve) => setTimeout(resolve, 1_000));
		await route.continue();
	});
	await page.getByTestId(`run-row-${runId}`).getByRole('link', { name: runId }).click();
	await expect(page.getByTestId('nav-progress')).toBeVisible();
	await expect(page.locator('main')).toHaveAttribute('aria-busy', 'true');
	await page.screenshot({ path: out('screenshot-nav-progress.png'), fullPage: true });
	await expect(page.getByTestId('run-status')).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('nav-progress')).toHaveCount(0);
	await expect(page.locator('main')).toHaveAttribute('aria-busy', 'false');
	await page.unroute('**/orkstack.v1.RunService/GetRun');
	await page.screenshot({ path: out('screenshot-nav-progress-done.png'), fullPage: true });
});
