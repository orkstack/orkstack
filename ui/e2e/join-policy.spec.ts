import { expect, test, type Page } from './test';
import { mkdirSync, readFileSync } from 'node:fs';
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
	repoRoot,
	rpc as callEngine,
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('join-policy');

// Join policies and failure handlers against a real engine, Orker, Temporal
// and MinIO (docs/DSL.md "Joins and failure handlers"): the Orkflow page's DAG marks joins and
// draws failure handlers dashed; the submit dialog's Plan shows a re-join and
// — with "Assume these tasks fail" — the failure path; and a failing run is
// watched LIVE on its run page (never reloaded): the failing note makes the
// Failing banner appear and the structured error show beside a Running pill,
// and resolving or cancelling the awaited handler ends the run FAILED.
//
// examples/branch-and-recover.ork.yaml is applied AS SHIPPED, and its await
// variant (rollback an awaited failure handler) beside it, so the failing
// phase lasts until the test resolves or cancels it. Both are submitted with
// smoke_seconds=120, so smoke is certainly running when deploy fails.
const ENGINE = 'http://127.0.0.1:18753';
const ENGINE_WS = 'ws://127.0.0.1:18753';
const UI = 'http://127.0.0.1:14753';
const TEMPORAL_NAMESPACE = temporalNamespace('join-policy');
const STAMP = Date.now().toString(36);
const NS = `joins-${STAMP}`;
const NAME = 'branch-and-recover';
const AWAIT_NAME = 'branch-and-recover-await';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'join-policy')
	: path.join(uiDir, 'test-results', 'join-policy');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

const shipped = () => readFileSync(path.join(repoRoot, 'examples', 'branch-and-recover.ork.yaml'), 'utf8');

/** The await variant: the shipped file with rollback replaced by an awaited failure handler. */
function awaitVariant(): string {
	const src = shipped().replace(`name: ${NAME}\n`, `name: ${AWAIT_NAME}\n`);
	const start = src.indexOf('  - id: rollback\n');
	const end = src.indexOf('  - id: escalate\n');
	return (
		src.slice(0, start) +
		'  - id: rollback\n    needs: [deploy]\n    join: any_failed\n    await: {timeout_seconds: 600}\n\n' +
		src.slice(end)
	);
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18753, 14753]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18753',
		ORK_WS_ALLOWED_ORIGINS: UI
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('join-policy') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14753', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'join policies' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: shipped(), namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: awaitVariant(), namespace: NS });
	note(`namespace ${NS}, orkflows ${NAME} (as shipped) and ${AWAIT_NAME}, engine ${ENGINE}, ui ${UI}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

async function openDialog(page: Page, name: string) {
	await page.goto(`${UI}/orkflows/${name}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
}

test('P1 — the DAG marks joins and draws failure handlers dashed', async ({ page }) => {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}`);
	await expect(page.getByTestId('orkflow-dag')).toBeVisible();
	await expect(page.getByTestId('dag-node-verify-join')).toHaveText('join none_failed');
	await expect(page.getByTestId('dag-node-rollback-join')).toHaveText('join any_failed');
	await expect(page.getByTestId('dag-node-rollback')).toHaveAttribute('data-join', 'any_failed');
	await expect(page.getByTestId('dag-edge-deploy-rollback')).toHaveAttribute('data-join', 'any_failed');
	await expect(page.getByTestId('dag-edge-deploy-rollback')).toHaveAttribute('stroke-dasharray', '5 4');
	await expect(page.getByTestId('dag-node-rollback').locator('rect')).toHaveAttribute('stroke-dasharray', '5 4');
	await expect(page.getByTestId('dag-node-deploy')).not.toHaveAttribute('data-join');
	note('P1 verify: join none_failed; rollback: join any_failed, dashed node and edge');
	await shot(page, 'screenshot-dag.png');
});

test('P2 — plan the re-join', async ({ page }) => {
	await openDialog(page, NAME);
	await page.getByTestId('declared-input-route').fill('canary');
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-assumed-outputs').fill('{"decide": {"value": "canary"}}');
	await page.getByTestId('run-submit-plan').click();
	await expect(page.getByTestId('run-plan-table')).toBeVisible();
	await expect(page.getByTestId('run-plan-row-full-detail')).toHaveText(
		'condition "{{ eq .tasks.decide.output.value \\"full\\" }}" evaluated false'
	);
	await expect(page.getByTestId('run-plan-row-verify-detail')).toHaveText(/^join none_failed/);
	await expect(page.getByTestId('run-plan-row-rollback-detail')).toHaveText(
		'join any_failed: none of its needs failed (deploy); it runs only when one of them fails'
	);
	note(`P2 verify: ${await page.getByTestId('run-plan-row-verify-detail').textContent()}`);
	await shot(page, 'screenshot-plan-rejoin.png');
});

test('P3 — plan the failure path', async ({ page }) => {
	await openDialog(page, NAME);
	await page.getByTestId('declared-input-route').fill('canary');
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-assumed-outputs').fill('{"decide": {"value": "canary"}}');
	await page.getByTestId('run-submit-assume-failed').fill('deploy');
	await page.getByTestId('run-submit-plan').click();
	await expect(page.getByTestId('run-plan-row-deploy-outcome')).toHaveText('assumed_failed');
	await expect(page.getByTestId('run-plan-row-announce-outcome')).toHaveText('unreached');
	await expect(page.getByTestId('run-plan-row-announce-detail')).toHaveText('after deploy fails');
	await expect(page.getByTestId('run-plan-row-smoke-outcome')).toHaveText('runs');
	await expect(page.getByTestId('run-plan-row-rollback-outcome')).toHaveText('runs');
	await expect(page.getByTestId('run-plan-row-notify-outcome')).toHaveText('runs');
	await expect(page.getByTestId('run-plan-summary')).toContainText('1 assumed failed');
	await expect(page.getByTestId('run-plan-fails')).toContainText('Failure handlers that would run: rollback, notify.');
	await page.getByTestId('run-submit-plan-cli-trigger').click();
	await expect(page.getByTestId('run-submit-plan-cli-command')).toContainText("--assume-failed 'deploy'");
	await page.getByTestId('run-submit-plan-cli-trigger').click();
	note(`P3 failure sentence: ${await page.getByTestId('run-plan-fails').textContent()}`);
	await shot(page, 'screenshot-plan-assume-failed.png');
	// Clearing the field clears the plan: it answered another draft.
	await page.getByTestId('run-submit-assume-failed').fill('');
	await expect(page.getByTestId('run-plan-table')).toHaveCount(0);
});

/** Submits the await variant failing, from the dialog, and stays on the run page that opens. */
async function submitFailing(page: Page): Promise<string> {
	await openDialog(page, AWAIT_NAME);
	await page.getByTestId('declared-input-route').fill('canary');
	await page.getByTestId('declared-input-fail_deploy').check();
	await page.getByTestId('declared-input-smoke_seconds').fill('120');
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	const runId = decodeURIComponent(new URL(page.url()).pathname.split('/').pop() ?? '');
	note(`submitted ${runId}`);
	return runId;
}

/**
 * Records, from the page's own event socket, the frames that carry the
 * failing note and RUN_FAILED, with their sequence numbers — what the live
 * page actually received.
 */
function recordFrames(page: Page): () => string {
	const lines: string[] = [];
	page.on('websocket', (ws) => {
		ws.on('framereceived', (frame) => {
			const text = typeof frame.payload === 'string' ? frame.payload : frame.payload.toString();
			if (!text.includes('run failing after task') && !text.includes('RUN_FAILED')) return;
			const sequence = /"sequence"\s*:\s*"?(\d+)/.exec(text)?.[1] ?? '?';
			const kind = text.includes('run failing after task') ? 'failing note' : 'RUN_FAILED';
			lines.push(`#${sequence} ${kind}`);
		});
	});
	return () => lines.join('\n');
}

test('P4 — a failing run, watched live', async ({ page }) => {
	const frames = recordFrames(page);
	const runId = await submitFailing(page);
	const banner = page.getByTestId('run-failing');
	await expect(banner).toBeVisible({ timeout: 240_000 });
	await expect(banner).toHaveAttribute('data-handlers', /rollback/);
	await expect(banner).toContainText('rollback (awaiting)', { timeout: 60_000 });
	// The failing note's re-read brings the structured error while the run
	// is still running.
	await expect(page.getByTestId('run-error')).toBeVisible();
	await expect(page.getByTestId('run-status')).toHaveText(/running/i);
	await expect(page.getByTestId('task-deploy')).not.toContainText('run failing after task');
	await expect(page.getByTestId('task-escalate-status')).toHaveText('Waiting for rollback');
	await expect(page.getByTestId('task-rollback-join')).toHaveText('join any_failed');
	await expect(page.getByTestId('task-smoke-status')).toHaveText(/canceled/i);
	await expect(page.getByTestId('task-announce')).toContainText(
		'the run is failing (task "deploy" failed) and this task is not a failure handler'
	);
	await shot(page, 'screenshot-failing-before.png');

	await page.getByTestId('task-rollback-resolve').click();
	await expect(page.getByTestId('await-resolve-dialog')).toBeVisible();
	await page.getByTestId('await-resolve-json-mode').click();
	await page.getByTestId('await-resolve-values').fill('{"rolled_back": true}');
	await page.getByTestId('await-resolve-submit').click();
	await expect(page.getByTestId('await-resolve-dialog')).toHaveCount(0);

	await expect(banner).toHaveCount(0, { timeout: 120_000 });
	await expect(page.getByTestId('run-status')).toHaveText(/failed/i);
	await expect(page.getByTestId('run-error')).toBeVisible();
	await expect(page.getByTestId('task-announce')).toContainText(
		'the run is failing (task "deploy" failed) and this task is not a failure handler'
	);
	note(`P4 ${runId}: banner shown live, cleared on RUN_FAILED; event frames the page received:\n${frames()}`);
	await shot(page, 'screenshot-failing-after.png');
});

test('P5 — cancel a failing run', async ({ page }) => {
	const runId = await submitFailing(page);
	const banner = page.getByTestId('run-failing');
	await expect(banner).toContainText('rollback (awaiting)', { timeout: 240_000 });
	await page.getByTestId('cancel-run').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	await page.getByTestId('cancel-run-accept').click();
	await expect(page.getByTestId('run-status')).toHaveText(/failed/i, { timeout: 120_000 });
	await expect(page.getByTestId('task-rollback-status')).toHaveText(/canceled/i);
	await expect(page.getByTestId('task-escalate-status')).toHaveText('Never started');
	await expect(banner).toHaveCount(0);
	note(`P5 ${runId}: cancelled while failing; run FAILED, rollback CANCELED, escalate never started`);
	await shot(page, 'screenshot-failing-canceled.png');
});
