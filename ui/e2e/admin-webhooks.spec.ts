import { expect, test } from './test';
import { type ChildProcess } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	orkerEnv,
	proofOut,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	waitFor,
	waitForEngine,
	bin,
	temporalAddress,
	temporalReachable
} from './harness';

// Admin → Webhooks with a scope, in a browser, against the real engine, Orker,
// Temporal and MinIO (docs/UI_CLI_PARITY.md "Scoped subscriptions and task
// outcome events").
//
// What is measured here:
//   1. the page states that subscriptions are deployment-wide unless scoped,
//      takes a namespace/orkflow/task scope and a task event, and the row it
//      lists afterwards states that scope;
//   2. a real run delivers TASK_SUCCEEDED to that subscription for the named
//      task only — signed with the one-time secret, carrying taskId and
//      attempt — and nothing for the run's other task;
//   3. an invalid scope that gets past the browser's own pattern check is
//      refused by the server, and the server's message is what the page shows;
//   4. deleting the subscription removes its row.
//
// The receiver is this spec's own node:http server on 127.0.0.1: the engine,
// not a container, POSTs to it.
//
// Cleanup stops exactly the processes this spec started, by handle
// (killStarted), never by name or pattern.

const ENGINE = 'http://127.0.0.1:18093';
const ENGINE_WS = 'ws://127.0.0.1:18093';
const UI_ORIGIN = 'http://127.0.0.1:14187';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-admin-webhooks-${STAMP}`;
const ORKFLOW = `webhook-scope-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-webhooks')
	: path.join(uiDir, 'test-results', 'admin-webhooks');
const out = proofOut(PROOF_DIR);
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function orkflowYaml(): string {
	return `name: ${ORKFLOW}
version: "0.1.0"
description: Scoped webhook browser proof.
tasks:
  - id: extract
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo extracting
        printf '{"rows":3}' > /orkstack/output/values.json
  - id: load
    uses: shell
    needs: [extract]
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo loading
        printf '{"loaded":true}' > /orkstack/output/values.json
`;
}

interface Received {
	path: string;
	headers: IncomingHttpHeaders;
	body: string;
}

interface WebhookRow {
	id: string;
	url: string;
	namespace?: string;
	orkflowName?: string;
	taskId?: string;
}

const received: Received[] = [];
let receiver: Server;
let receiverURL = '';
const mine: ChildProcess[] = [];
let scopedId = '';
let scopedSecret = '';
let controlId = '';

async function listWebhooks(): Promise<WebhookRow[]> {
	const answer = await rpc<{ webhooks?: WebhookRow[] }>('WebhookService/ListWebhooks', {});
	return answer.webhooks ?? [];
}

function deliveriesTo(pathName: string): Received[] {
	return received.filter((r) => r.path === pathName);
}

function bodyOf(r: Received): { runId: string; taskId?: string; attempt?: number; status: string } {
	return JSON.parse(r.body);
}

async function stopMine(): Promise<void> {
	for (const child of mine) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 10_000;
	for (const child of mine) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18093, 14187]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	receiver = createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on('data', (chunk: Buffer) => chunks.push(chunk));
		req.on('end', () => {
			received.push({ path: req.url ?? '', headers: req.headers, body: Buffer.concat(chunks).toString('utf8') });
			res.writeHead(200).end();
		});
	});
	await new Promise<void>((resolve) => receiver.listen(0, '127.0.0.1', resolve));
	receiverURL = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}`;

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	mine.push(
		spawnLogged(out('engine.log'), bin('ork'), [], {
			...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18093', ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
		})
	);
	mine.push(spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ...orkerEnv('admin-webhooks') }));
	mine.push(
		spawnLogged(out('ui.log'), 'node', ['build'], {
			HOST: '127.0.0.1', PORT: '14187', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
		}, uiDir)
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/admin/webhooks`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflowYaml() });
});

test.afterAll(async () => {
	for (const id of [scopedId, controlId]) {
		if (!id) continue;
		try {
			await rpc('WebhookService/DeleteWebhook', { id });
		} catch {
			// already gone: the delete test removes the scoped one
		}
	}
	await stopMine();
	await new Promise<void>((resolve) => (receiver ? receiver.close(() => resolve()) : resolve()));
});

test('registers a scoped task-event webhook and the row states its scope', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await expect(page.getByTestId('webhooks-scope')).toHaveText(
		'Subscriptions are deployment-wide unless they name a scope. The namespace selector does not filter this list; each row states what it covers.'
	);
	await expect(page.getByTestId('webhook-task-caveat')).toContainText('reconcile with GetRun');
	// Six task checkboxes — an awaited task's two included — and TASK_LOG is
	// never offered.
	for (const name of ['TASK_SCHEDULED', 'TASK_STARTED', 'TASK_AWAITING', 'TASK_RESOLVED', 'TASK_SUCCEEDED', 'TASK_FAILED']) {
		await expect(page.getByTestId(`webhook-event-${name}`)).toBeVisible();
	}
	await expect(page.getByTestId('webhook-event-TASK_LOG')).toHaveCount(0);

	await page.getByTestId('webhook-url').fill(`${receiverURL}/scoped`);
	await page.getByTestId('webhook-namespace').fill('default');
	await page.getByTestId('webhook-orkflow').fill(ORKFLOW);
	await page.getByTestId('webhook-task').fill('extract');
	await page.getByTestId('webhook-event-TASK_SUCCEEDED').check();
	await page.getByTestId('register-webhook').click();

	const secretBlock = page.getByTestId('webhook-secret-once');
	await expect(secretBlock).toBeVisible();
	scopedSecret = (await secretBlock.locator('code').innerText()).trim();
	expect(scopedSecret.length).toBeGreaterThan(0);

	const row = (await listWebhooks()).find((hook) => hook.url === `${receiverURL}/scoped`);
	expect(row, 'the registration is listed by the engine').toBeTruthy();
	scopedId = row!.id;
	expect(row).toMatchObject({ namespace: 'default', orkflowName: ORKFLOW, taskId: 'extract' });
	await expect(page.getByTestId(`webhook-scope-${scopedId}`)).toHaveText(`default/${ORKFLOW}/extract`);
	await page.getByTestId(`webhook-${scopedId}`).scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('webhooks-scoped-row.png'), fullPage: true });
});

test('a task-event delivery reaches the receiver with taskId and attempt', async () => {
	expect(scopedId, 'the registration test ran first').not.toBe('');
	// A control subscription on another path of the same receiver wants every
	// task's TASK_SUCCEEDED. Its delivery for `load` is queued by the same
	// Notify call that would queue one for the scoped subscription, so once it
	// has arrived, the scoped path's silence about `load` is a result, not a
	// timeout that happened to pass.
	const control = await rpc<{ webhook: { id: string } }>('WebhookService/RegisterWebhook', {
		url: `${receiverURL}/control`,
		events: ['EVENT_TYPE_TASK_SUCCEEDED', 'EVENT_TYPE_RUN_SUCCEEDED'],
		orkflowName: ORKFLOW
	});
	controlId = control.webhook.id;

	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW });
	const runId = submitted.run.id;
	const forRun = (pathName: string) => deliveriesTo(pathName).filter((r) => bodyOf(r).runId === runId);

	await expect
		.poll(() => forRun('/scoped').filter((r) => r.headers['x-ork-event'] === 'TASK_SUCCEEDED').length, {
			timeout: 120_000
		})
		.toBe(1);
	await expect
		.poll(
			() =>
				forRun('/control').filter(
					(r) => r.headers['x-ork-event'] === 'TASK_SUCCEEDED' && bodyOf(r).taskId === 'load'
				).length,
			{ timeout: 120_000 }
		)
		.toBe(1);
	await expect
		.poll(() => forRun('/control').filter((r) => r.headers['x-ork-event'] === 'RUN_SUCCEEDED').length, {
			timeout: 120_000
		})
		.toBe(1);

	const scoped = forRun('/scoped');
	expect(scoped, 'the scoped subscription received exactly one delivery for the run').toHaveLength(1);
	const delivery = scoped[0];
	const body = bodyOf(delivery);
	expect(delivery.headers['x-ork-event']).toBe('TASK_SUCCEEDED');
	expect(delivery.headers['x-ork-task-id']).toBe('extract');
	expect(body.taskId).toBe('extract');
	expect(body.status).toBe('SUCCEEDED');
	expect(body.attempt).toBeGreaterThanOrEqual(1);
	const expected = 'sha256=' + createHmac('sha256', scopedSecret).update(delivery.body).digest('hex');
	expect(delivery.headers['x-ork-signature'], 'signed with the one-time secret').toBe(expected);
	// Nothing for the run's other task.
	expect(scoped.filter((r) => bodyOf(r).taskId === 'load')).toHaveLength(0);

	writeFileSync(
		out('webhook-task-delivery.json'),
		JSON.stringify(
			{
				runId,
				headers: {
					'x-ork-event': delivery.headers['x-ork-event'],
					'x-ork-task-id': delivery.headers['x-ork-task-id'],
					'x-ork-webhook-id': delivery.headers['x-ork-webhook-id'],
					'x-ork-signature': delivery.headers['x-ork-signature'],
					signatureVerified: delivery.headers['x-ork-signature'] === expected
				},
				body: JSON.parse(delivery.body),
				scopedDeliveriesForRun: scoped.length,
				controlDeliveriesForRun: forRun('/control').map((r) => ({
					event: r.headers['x-ork-event'],
					taskId: bodyOf(r).taskId ?? null
				}))
			},
			null,
			2
		) + '\n'
	);
});

test('an invalid scope is refused with the server\'s message', async ({ page }) => {
	const before = (await listWebhooks()).length;
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.getByTestId('webhook-url').fill(`${receiverURL}/invalid`);
	// Past the browser's own pattern check, so the server is what refuses it.
	await page.getByTestId('webhook-namespace').evaluate((input) => input.removeAttribute('pattern'));
	await page.getByTestId('webhook-namespace').fill('Bad Name');
	await page.getByTestId('register-webhook').click();
	const error = page.getByTestId('webhook-register-error');
	await expect(error).toBeVisible();
	await expect(error).toContainText('invalid namespace scope "Bad Name"');
	await page.screenshot({ path: out('webhooks-invalid-scope.png'), fullPage: true });
	expect((await listWebhooks()).length, 'nothing was stored').toBe(before);
	expect((await listWebhooks()).some((hook) => hook.url === `${receiverURL}/invalid`)).toBe(false);
});

// F-ui-ux-10: a failed subscriptions read offers Retry, and Retry re-reads in
// place. The page is reached by a CLIENT navigation from the overview so the
// loader runs in the browser, where page.route can fail its first read (a
// server render's read never passes through the browser).
test('a failed subscriptions read offers Retry, and Retry renders the list', async ({ page }) => {
	expect(scopedId).not.toBe('');
	await page.goto(`${UI_ORIGIN}/admin`);
	let failed = 0;
	await page.route('**/orkstack.v1.WebhookService/ListWebhooks', async (route) => {
		if (failed === 0) {
			failed++;
			await route.abort();
			return;
		}
		await route.continue();
	});
	await page.getByTestId('admin-tab-webhooks').click();
	await expect(page.getByTestId('webhooks-error')).toBeVisible();
	await expect(page.getByTestId('webhooks-retry')).toBeEnabled();
	await page.screenshot({ path: out('webhooks-list-error-retry.png'), fullPage: true });
	await page.getByTestId('webhooks-retry').click();
	await expect(page.getByTestId('webhooks-error')).toHaveCount(0);
	await expect(page.getByTestId(`webhook-${scopedId}`)).toBeVisible();
	expect(failed).toBe(1);
	await page.screenshot({ path: out('webhooks-list-after-retry.png'), fullPage: true });
});

test('deleting the scoped webhook removes its row', async ({ page }) => {
	expect(scopedId).not.toBe('');
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.getByTestId(`delete-webhook-${scopedId}`).click();
	await expect(page.getByTestId('delete-webhook-confirm')).toBeVisible();
	await page.getByTestId('delete-webhook-accept').click();
	await expect(page.getByTestId(`webhook-${scopedId}`)).toHaveCount(0);
	await page.reload();
	await expect(page.getByTestId(`webhook-${scopedId}`)).toHaveCount(0);
	expect((await listWebhooks()).some((hook) => hook.id === scopedId)).toBe(false);
	scopedId = '';
});
