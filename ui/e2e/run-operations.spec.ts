import { expect, test } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killAll,
	orkerEnv,
	proofOut,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// This is an operator flow, not a mocked RPC test. It proves that a browser
// can request cancellation without painting a made-up terminal status, then
// retain its namespace/version coordinates while it prepares a rerun.
const ENGINE = 'http://127.0.0.1:18088';
const ENGINE_WS = 'ws://127.0.0.1:18088';
const UI_ORIGIN = 'http://127.0.0.1:14180';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-operations';
const NAME = `run-operations-${Date.now().toString(36)}`;
const COMPLETED_NAME = `${NAME}-completed`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-operations')
	: path.join(uiDir, 'test-results', 'run-operations');
const out = proofOut(PROOF_DIR);

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function workflow(): string {
	return `name: ${NAME}
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
	return `name: ${COMPLETED_NAME}
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
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18088, 14180]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18088', ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('run-operations') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14180', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: completedWorkflow() });
});

test.afterAll(async () => { await killAll(); });

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
