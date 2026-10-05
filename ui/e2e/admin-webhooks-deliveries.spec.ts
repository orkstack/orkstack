import { expect, test, type Page } from './test';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { createHash, createHmac } from 'node:crypto';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
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
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// Admin → Webhooks → Deliveries, in a browser, against the real engine, Orker,
// Temporal and MinIO (docs/design/proposals/phase-2/delivery-records).
//
// What is measured here:
//   1. a delivery the receiver refused is recorded once it settles, and the
//      panel shows it failed, with every attempt and the SHA-256 of the exact
//      bytes the receiver read;
//   2. Redeliver sends exactly one signed POST naming the delivery it repeats,
//      reports it inline, and the new record lists first;
//   3. outcome and run filters narrow the table, and the URL restores them;
//   4. pagination walks newest first without repeats and survives a reload;
//   5. a failed deliveries read is an error with Retry beside an intact
//      webhook table, and Retry recovers once the engine is back;
//   6. the per-webhook link filters the panel.
//
// The receiver is this spec's own node:http server on 127.0.0.1, answering
// 502 or 200 as told. Cleanup stops exactly the processes this spec started,
// by PID.

const ENGINE_PORT = 18331;
const UI_PORT = 14331;
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const ENGINE_WS = `ws://127.0.0.1:${ENGINE_PORT}`;
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-deliveries-${STAMP}`;
const ORKFLOW = `deliveries-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-webhooks-deliveries')
	: path.join(uiDir, 'test-results', 'admin-webhooks-deliveries');
const out = proofOut(PROOF_DIR);
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function orkflowYaml(): string {
	return `name: ${ORKFLOW}
version: "0.1.0"
description: Delivery records browser proof.
tasks:
  - id: hello
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo hello
        printf '{"ok":true}' > /orkstack/output/values.json
`;
}

interface Received {
	headers: IncomingHttpHeaders;
	body: string;
	status: number;
}

interface DeliveryJSON {
	id: string;
	delivered?: boolean;
	redeliveryOf?: string;
	runId: string;
	event: string;
	attempts?: { statusCode?: number }[];
	bodySha256?: string;
}

const received: Received[] = [];
let answer = 502;
let receiver: Server;
let receiverURL = '';
const mine: ChildProcess[] = [];
let engine: ChildProcess | null = null;
let webhookId = '';
let secret = '';
let firstRun = '';
let failedId = '';

const cluster = clusterEnv(TEMPORAL_NAMESPACE);
function startEngine(): ChildProcess {
	const child = spawnLogged(out(`engine-${mine.length}.log`), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	mine.push(child);
	return child;
}

async function stop(child: ChildProcess | null): Promise<void> {
	if (!child) return;
	if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	const deadline = Date.now() + 10_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

const forRun = (runId: string, event: string) =>
	received.filter((r) => r.headers['x-ork-run-id'] === runId && r.headers['x-ork-event'] === event);

async function listFor(webhook: string): Promise<DeliveryJSON[]> {
	const answered = await rpc<{ deliveries?: DeliveryJSON[] }>('WebhookService/ListDeliveries', { webhookId: webhook, pageSize: 200 });
	return answered.deliveries ?? [];
}

async function submitAndSettle(expectedPerEvent: number): Promise<string> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW });
	const runId = submitted.run.id;
	await expect
		.poll(() => forRun(runId, 'RUN_SUCCEEDED').length, { timeout: 150_000 })
		.toBe(expectedPerEvent);
	// Settled means recorded: the record is written right after the last
	// attempt returns, on the delivery's own goroutine.
	await expect
		.poll(async () => (await listFor(webhookId)).filter((d) => d.runId === runId).length, { timeout: 30_000 })
		.toBe(2);
	return runId;
}

async function expand(page: Page, id: string) {
	// Click a plain cell of the row: its centre can be the run link, which
	// navigates to the run instead of expanding the record.
	// A click that lands before hydration does nothing, so it is retried
	// until the detail row is there — never once it is, which would fold it.
	const detail = page.getByTestId(`delivery-detail-${id}`);
	await expect(async () => {
		if (!(await detail.isVisible())) await page.getByTestId(`delivery-event-${id}`).click();
		await expect(detail).toBeVisible({ timeout: 2_000 });
	}).toPass({ timeout: 20_000 });
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [ENGINE_PORT, UI_PORT]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	receiver = createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on('data', (chunk: Buffer) => chunks.push(chunk));
		req.on('end', () => {
			const status = answer;
			received.push({ headers: req.headers, body: Buffer.concat(chunks).toString('utf8'), status });
			res.writeHead(status).end();
		});
	});
	await new Promise<void>((resolve) => receiver.listen(0, '127.0.0.1', resolve));
	receiverURL = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}/hooks`;

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	engine = startEngine();
	mine.push(spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('admin-deliveries') }));
	mine.push(
		spawnLogged(out('ui.log'), 'node', ['build'], {
			HOST: '127.0.0.1', PORT: String(UI_PORT), ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
		}, uiDir)
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/admin/webhooks`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answered = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answered.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflowYaml() });
});

test.afterAll(async () => {
	if (webhookId) {
		try {
			await rpc('WebhookService/DeleteWebhook', { id: webhookId });
		} catch {
			// the engine may be the one that is down
		}
	}
	for (const child of mine) await stop(child);
	await new Promise<void>((resolve) => (receiver ? receiver.close(() => resolve()) : resolve()));
});

test('a settled delivery appears in the Deliveries panel with its attempts and digest', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-scope')).toContainText('the body itself is not stored');
	await expect(page.getByTestId('deliveries-scope')).toContainText('Showing every namespace');

	// Register through the page, scoped to this spec's orkflow so no other
	// suite's run reaches this receiver.
	await page.getByTestId('webhook-url').fill(receiverURL);
	await page.getByTestId('webhook-orkflow').fill(ORKFLOW);
	await page.getByTestId('webhook-event-RUN_STARTED').check();
	await page.getByTestId('webhook-event-RUN_SUCCEEDED').check();
	await page.getByTestId('register-webhook').click();
	const secretBlock = page.getByTestId('webhook-secret-once');
	await expect(secretBlock).toBeVisible();
	secret = (await secretBlock.locator('code').innerText()).trim();
	const hooks = await rpc<{ webhooks?: { id: string; url: string }[] }>('WebhookService/ListWebhooks', {});
	webhookId = (hooks.webhooks ?? []).find((hook) => hook.url === receiverURL)?.id ?? '';
	expect(webhookId).not.toBe('');

	// The receiver refuses everything: each delivery is a four-attempt ladder.
	answer = 502;
	firstRun = await submitAndSettle(4);
	const failed = (await listFor(webhookId)).find((d) => d.runId === firstRun && d.event === 'EVENT_TYPE_RUN_SUCCEEDED');
	expect(failed).toBeTruthy();
	failedId = failed!.id;

	await page.goto(`${UI_ORIGIN}/admin/webhooks?webhook=${webhookId}`);
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId(`delivery-${failedId}`)).toBeVisible();
	await expect(page.getByTestId(`delivery-outcome-${failedId}`)).toHaveText('failed');
	await expect(page.getByTestId(`delivery-attempt-count-${failedId}`)).toHaveText('4');
	await expect(page.getByTestId(`delivery-last-${failedId}`)).toContainText('502 at ');
	await expand(page, failedId);
	await expect(page.getByTestId(`delivery-attempts-${failedId}`).locator('tbody tr')).toHaveCount(4);
	for (const n of [1, 2, 3, 4]) await expect(page.getByTestId(`delivery-attempt-${failedId}-${n}`)).toContainText('502');
	const bodies = forRun(firstRun, 'RUN_SUCCEEDED');
	expect(bodies).toHaveLength(4);
	const receiverSha = createHash('sha256').update(bodies[0].body).digest('hex');
	await expect(page.getByTestId(`delivery-sha256-${failedId}`)).toHaveText(receiverSha);
	await expect(page.getByTestId(`delivery-body-${failedId}`)).toContainText('body not stored');
	const signature = 'sha256=' + createHmac('sha256', secret).update(bodies[0].body).digest('hex');
	await expect(page.getByTestId(`delivery-signature-${failedId}`)).toHaveText(signature);
	await page.getByTestId(`delivery-detail-${failedId}`).scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('deliveries-failed-row.png'), fullPage: true });
});

test('redelivering sends exactly one signed POST and lists the new record first', async ({ page }) => {
	expect(failedId).not.toBe('');
	answer = 200;
	const before = received.length;
	await page.goto(`${UI_ORIGIN}/admin/webhooks?webhook=${webhookId}`);
	await page.waitForLoadState('networkidle');
	await expand(page, failedId);
	await page.getByTestId(`redeliver-${failedId}`).click();
	const dialog = page.getByTestId('redeliver-confirm');
	await expect(dialog).toBeVisible();
	await expect(page.getByTestId('redeliver-sentence')).toHaveText(
		`OrkStack will POST this event to ${receiverURL} once, now, signed with the subscription's current secret and embedding the run as it reads today. Nothing is queued and nothing retries; you are the retry policy.`
	);
	await page.getByTestId('redeliver-accept').click();
	await expect(page.getByTestId(`redeliver-result-${failedId}`)).toContainText('delivered, 200');
	await expect(dialog).toHaveCount(0);

	expect(received.length - before, 'exactly one more request reached the receiver').toBe(1);
	const again = received[received.length - 1];
	expect(again.headers['x-ork-redelivery-of']).toBe(failedId);
	expect(again.headers['x-ork-attempt']).toBe('1');
	const expected = 'sha256=' + createHmac('sha256', secret).update(again.body).digest('hex');
	expect(again.headers['x-ork-signature'], 'signed with the subscription secret').toBe(expected);
	const newId = String(again.headers['x-ork-delivery']);
	await expect(page.getByTestId(`redeliver-result-${failedId}`)).toContainText(`Redelivered as ${newId}`);

	const first = page.getByTestId('deliveries-table').locator('tbody tr').first();
	await expect(first).toHaveAttribute('data-testid', `delivery-${newId}`);
	await expect(page.getByTestId(`delivery-is-redelivery-${newId}`)).toBeVisible();
	await expand(page, newId);
	await expect(page.getByTestId(`delivery-redelivery-of-${newId}`)).toHaveText(failedId);

	writeFileSync(
		out('webhook-redelivery.json'),
		JSON.stringify(
			{
				original: failedId,
				redelivery: newId,
				requestsAfterRedeliver: received.length - before,
				headers: {
					'x-ork-delivery': again.headers['x-ork-delivery'],
					'x-ork-redelivery-of': again.headers['x-ork-redelivery-of'],
					'x-ork-attempt': again.headers['x-ork-attempt'],
					'x-ork-timestamp': again.headers['x-ork-timestamp'],
					'x-ork-signature': again.headers['x-ork-signature'],
					signatureVerified: again.headers['x-ork-signature'] === expected
				},
				status: again.status,
				bodySha256: createHash('sha256').update(again.body).digest('hex')
			},
			null,
			2
		) + '\n'
	);
	await page.screenshot({ path: out('deliveries-redelivered.png'), fullPage: true });
});

test('filters narrow by outcome and run and the URL restores them', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks?webhook=${webhookId}`);
	await page.waitForLoadState('networkidle');
	await page.getByTestId('deliveries-outcome').click();
	await page.getByRole('option', { name: 'Failed', exact: true }).click();
	await expect(page).toHaveURL(/delivered=false/);
	await page.getByTestId('deliveries-run').fill(firstRun);
	await page.getByTestId('deliveries-apply').click();
	await expect(page).toHaveURL(new RegExp(`run=${firstRun}`));
	const rows = page.getByTestId('deliveries-table').locator('tbody tr[data-testid^="delivery-dlv-"]');
	await expect(rows).toHaveCount(2);
	for (const id of await rows.evaluateAll((els) => els.map((el) => el.getAttribute('data-testid')!.slice('delivery-'.length))))
		await expect(page.getByTestId(`delivery-outcome-${id}`)).toHaveText('failed');
	const window = await page.getByTestId('deliveries-window').innerText();
	await page.reload();
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-run')).toHaveValue(firstRun);
	await expect(page.getByTestId('deliveries-outcome')).toContainText('Failed');
	await expect(rows).toHaveCount(2);
	// The caption's window starts at the run's start, as the engine resolved it.
	expect((await page.getByTestId('deliveries-window').innerText()).split(' to ')[0]).toBe(window.split(' to ')[0]);
});

test('pagination walks newest first without repeats', async ({ page }) => {
	// A second run, accepted this time: two more records, five in all.
	answer = 200;
	await submitAndSettle(1);
	expect((await listFor(webhookId)).length).toBeGreaterThanOrEqual(5);

	await page.goto(`${UI_ORIGIN}/admin/webhooks?webhook=${webhookId}&limit=2`);
	await page.waitForLoadState('networkidle');
	const ids = async () =>
		page
			.getByTestId('deliveries-table')
			.locator('tbody tr[data-testid^="delivery-dlv-"]')
			.evaluateAll((els) => els.map((el) => el.getAttribute('data-testid')!));
	const seen: string[] = [];
	seen.push(...(await ids()));
	await page.getByTestId('deliveries-next').click();
	await expect(page).toHaveURL(/page=2/);
	seen.push(...(await ids()));
	await page.getByTestId('deliveries-next').click();
	await expect(page).toHaveURL(/page=3/);
	const third = await ids();
	seen.push(...third);
	await page.reload();
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-page-status')).toContainText('Page 3');
	expect(await ids()).toEqual(third);
	await page.getByTestId('deliveries-previous').click();
	await expect(page).toHaveURL(/page=2/);
	expect(new Set(seen).size, `no delivery repeats across pages: ${seen.join(', ')}`).toBe(seen.length);
	expect(seen.length).toBeGreaterThanOrEqual(5);
	// Newest first, as the engine answers.
	const all = (await listFor(webhookId)).map((d) => `delivery-${d.id}`);
	expect(seen).toEqual(all.slice(0, seen.length));
});

test('a failed deliveries read shows an error beside an intact webhook table', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks?webhook=${webhookId}`);
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-table')).toBeVisible();

	// Only the deliveries read fails: the webhook table must still render.
	await page.route('**/orkstack.v1.WebhookService/ListDeliveries', (route) => route.abort());
	await page.getByTestId('deliveries-outcome').click();
	await page.getByRole('option', { name: 'Delivered', exact: true }).click();
	await expect(page.getByTestId('deliveries-error')).toBeVisible();
	await expect(page.getByTestId('deliveries-empty')).toHaveCount(0);
	await expect(page.getByTestId(`webhook-${webhookId}`)).toBeVisible();
	await page.unroute('**/orkstack.v1.WebhookService/ListDeliveries');

	// The engine itself down: the read fails with Retry; back up, Retry renders.
	await stop(engine);
	await page.reload();
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-error')).toBeVisible();
	await expect(page.getByTestId('deliveries-error-retry')).toBeVisible();
	await page.screenshot({ path: out('deliveries-read-failed.png'), fullPage: true });
	engine = startEngine();
	await waitForEngine(ENGINE);
	await page.getByTestId('deliveries-error-retry').click();
	await expect(page.getByTestId('deliveries-table')).toBeVisible();
	await expect(page.getByTestId('deliveries-error')).toHaveCount(0);
});

test('the per-webhook link filters the panel', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.waitForLoadState('networkidle');
	await page.getByTestId(`webhook-deliveries-${webhookId}`).click();
	await expect(page).toHaveURL(new RegExp(`webhook=${webhookId}`));
	await expect(page.getByTestId('deliveries-scope-webhook')).toHaveText(webhookId);
	await expect(page.getByTestId('deliveries-webhook')).toContainText(webhookId);
	const rows = page.getByTestId('deliveries-table').locator('tbody tr[data-testid^="delivery-dlv-"]');
	await expect(rows.first()).toBeVisible();
	expect(await rows.count()).toBeGreaterThanOrEqual(5);
});
