import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { createHash, createHmac } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import { OP, addListFilter, selectOption } from './uiHelpers';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
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
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('admin-webhooks-deliv');

// Admin → Webhooks → Deliveries, in a browser, against the real engine, Orker,
// Temporal and MinIO (docs/UI_CLI_PARITY.md "Webhook deliveries", "Filtering
// lists"). The panel's filters are the one filter component (ListFilter),
// riding `deliveryFilter` parameters.
//
// What is measured here:
//   - a delivery the receiver refused is recorded once it settles, and the
//     panel shows it failed, with every attempt and the SHA-256 of the exact
//     bytes the receiver read;
//   - Redeliver sends exactly one signed POST naming the delivery it repeats,
//     reports it inline, and the new record lists first;
//   D1 a subscription's Deliveries link opens deliveryFilter=webhook_id=<id>;
//   D2 event = TASK_FAILED through the enum Select matches nothing here, and
//      RUN_STARTED narrows to that event;
//   D3 delivered = false with a run_id filter narrows to the failed records,
//      the URL restores rows and draft, and the window starts at the run's
//      start;
//   D4 a created_at window through datetime-local, and the caption's
//      resolved window;
//   D5 paging under filters (size 2) walks newest first without repeats and
//      survives a reload; a changed filter clears the cursor;
//   D6 the CLI equivalents;
//   - a failed deliveries read is an error with Retry beside an intact
//     webhook table, and Retry recovers once the engine is back.
//
// The receiver is this spec's own node:http server on 127.0.0.1, answering
// 502 or 200 as told. Cleanup stops exactly the processes this spec started,
// by PID.

const ENGINE_PORT = 18331;
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
// The UI is the engine's own origin: it serves the build beside its API.
const UI_ORIGIN = ENGINE;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-deliveries-${STAMP}`;
const ORKFLOW = `deliveries-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-webhooks-deliveries')
	: path.join(uiDir, 'test-results', 'admin-webhooks-deliveries');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function orkflowYaml(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
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
	const child = spawnLogged(out(`engine-${mine.length}.log`), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`
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

// A filter as the engine's JSON codec reads it: the operator in its proto
// spelling. A removed typed field would be IGNORED and answer a wider list,
// so every request body here names its filters.
const equals = (field: string, value: string) => ({ field, op: 'FILTER_OPERATOR_EQUALS', values: [value] });

async function listFor(webhook: string): Promise<DeliveryJSON[]> {
	const answered = await rpc<{ deliveries?: DeliveryJSON[] }>('WebhookService/ListDeliveries', {
		filters: [equals('webhook_id', webhook)],
		pageSize: 200
	});
	return answered.deliveries ?? [];
}

/** The page with the panel narrowed by these filters (CLI grammar), on its first page. */
function panelUrl(filters: string[], extra: Record<string, string> = {}): string {
	const params = new URLSearchParams({ ns: 'default' });
	for (const text of [...filters].sort()) params.append('deliveryFilter', text);
	for (const [key, value] of Object.entries(extra)) params.set(key, value);
	return `${UI_ORIGIN}/admin/webhooks?${params.toString()}`;
}

const deliveryFilters = (page: Page) => new URL(page.url()).searchParams.getAll('deliveryFilter');

const rowIds = (page: Page) =>
	page
		.getByTestId('deliveries-table')
		.locator('tbody tr[data-testid^="delivery-dlv-"]')
		.evaluateAll((els) => els.map((el) => el.getAttribute('data-testid')!.slice('delivery-'.length)));

async function cliText(page: Page, testid: string): Promise<string> {
	await page.getByTestId(`${testid}-trigger`).click();
	const text = await page.getByTestId(`${testid}-command`).innerText();
	await page.keyboard.press('Escape');
	return text;
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
// UTC, so a datetime-local reading is the instant it sends (D4).
test.use({ timezoneId: 'UTC' });

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(ENGINE_PORT)) throw new Error(`port ${ENGINE_PORT} already in use`);

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

	engine = startEngine();
	mine.push(spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('admin-deliveries') }));
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
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
	removeBucket(BUCKET);
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

	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
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
	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
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
	// The original's link opens the panel filtered to its run and webhook.
	await expect(page.getByTestId(`delivery-redelivery-of-${newId}`)).toHaveAttribute(
		'href',
		new RegExp(`deliveryFilter=run_id%3D${firstRun}&deliveryFilter=webhook_id%3D${webhookId}`)
	);

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

test("D1: a subscription's Deliveries link opens deliveryFilter=webhook_id=<id>", async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.waitForLoadState('networkidle');
	await page.getByTestId(`webhook-deliveries-${webhookId}`).click();
	await expect.poll(() => deliveryFilters(page)).toEqual([`webhook_id=${webhookId}`]);
	// The draft shows the filter the link applied.
	await expect(page.getByTestId('deliveries-filter-field-0')).toHaveText('Webhook (webhook_id)');
	await expect(page.getByTestId('deliveries-filter-value-0')).toHaveValue(webhookId);
	await expect.poll(async () => (await rowIds(page)).length).toBeGreaterThanOrEqual(3);
	note(`D1 the Deliveries link of ${webhookId} opened ${page.url()}`);
});

test('D2: event = TASK_FAILED through the enum Select matches nothing; RUN_STARTED narrows', async ({ page }) => {
	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'deliveries', { field: 'event', op: OP.equals, select: 'TASK_FAILED' });
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual(['event=TASK_FAILED', `webhook_id=${webhookId}`]);
	await expect(page.getByTestId('deliveries-empty')).toHaveText('No deliveries match these filters.');
	await page.screenshot({ path: out('screenshot-deliveries-no-match.png'), fullPage: true });

	// The draft is refilled in canonical order: the event row is first.
	await expect(page.getByTestId('deliveries-filter-value-0')).toHaveText('TASK_FAILED');
	await page.waitForLoadState('networkidle');
	await selectOption(page, 'deliveries-filter-value-0', 'RUN_STARTED');
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual(['event=RUN_STARTED', `webhook_id=${webhookId}`]);
	await expect.poll(async () => (await rowIds(page)).length).toBeGreaterThanOrEqual(1);
	for (const id of await rowIds(page)) await expect(page.getByTestId(`delivery-event-${id}`)).toHaveText('RUN_STARTED');
	note(`D2 event=RUN_STARTED: ${(await rowIds(page)).length} records, every one RUN_STARTED`);
	await page.screenshot({ path: out('screenshot-deliveries-event.png'), fullPage: true });
});

test('D3: delivered = false with a run_id filter narrows, survives a reload, and starts at the run', async ({ page }) => {
	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'deliveries', { field: 'delivered', op: OP.equals, select: 'false' });
	await addListFilter(page, 'deliveries', { field: 'run_id', op: OP.equals, value: firstRun });
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual(['delivered=false', `run_id=${firstRun}`, `webhook_id=${webhookId}`]);
	await expect.poll(async () => (await rowIds(page)).length).toBe(2);
	for (const id of await rowIds(page)) await expect(page.getByTestId(`delivery-outcome-${id}`)).toHaveText('failed');
	await page.screenshot({ path: out('screenshot-deliveries-filtered.png'), fullPage: true });

	await page.reload();
	await page.waitForLoadState('networkidle');
	await expect.poll(async () => (await rowIds(page)).length).toBe(2);
	await expect(page.getByTestId('deliveries-filter-field-0')).toHaveText('Delivered (delivered)');
	await expect(page.getByTestId('deliveries-filter-value-0')).toHaveText('false');
	await expect(page.getByTestId('deliveries-filter-value-1')).toHaveValue(firstRun);
	// One run_id equality and no created_at start: the window starts at the run.
	const run = await rpc<{ run: { createdAt: string } }>('RunService/GetRun', { runId: firstRun });
	const runStart = new Date(run.run.createdAt).toISOString().replace(/\.\d{3}Z$/, 'Z');
	expect((await page.getByTestId('deliveries-window').innerText()).split(' to ')[0]).toBe(runStart);
	note(`D3 delivered=false run_id=${firstRun}: 2 failed records; window starts at the run's start ${runStart}`);
});

test("D4: a created_at window through datetime-local is the caption's resolved window", async ({ page }) => {
	const run = await rpc<{ run: { createdAt: string } }>('RunService/GetRun', { runId: firstRun });
	const minute = (d: Date) => d.toISOString().slice(0, 16);
	const since = minute(new Date(new Date(run.run.createdAt).getTime() - 60 * 60_000));
	const until = minute(new Date(Date.now() + 60 * 60_000));
	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
	await page.waitForLoadState('networkidle');
	const from = await addListFilter(page, 'deliveries', { field: 'created_at', op: OP.atOrAfter, value: since });
	const to = await addListFilter(page, 'deliveries', { field: 'created_at', op: OP.before, value: until });
	await expect(page.getByTestId(`deliveries-filter-utc-${from}`)).toHaveText(`${since}:00Z (UTC)`);
	await expect(page.getByTestId(`deliveries-filter-utc-${to}`)).toHaveText(`${until}:00Z (UTC)`);
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual([`created_at<${until}:00Z`, `created_at>=${since}:00Z`, `webhook_id=${webhookId}`]);
	await expect(page.getByTestId('deliveries-window')).toHaveText(`${since}:00Z to ${until}:00Z`);
	await expect(page.getByTestId(`delivery-${failedId}`)).toBeVisible();
	note(`D4 window ${since}:00Z to ${until}:00Z, caption: ${await page.getByTestId('deliveries-scope').innerText()}`);
	await page.screenshot({ path: out('screenshot-deliveries-window.png'), fullPage: true });

	// An inverted window is the engine's refusal, shown verbatim.
	await page.goto(panelUrl([`webhook_id=${webhookId}`, `created_at>=${until}:00Z`, `created_at<${since}:00Z`]));
	await expect(page.getByTestId('deliveries-error')).toContainText('select no instant');
});

test('D5: paging under filters walks newest first without repeats; a changed filter clears the cursor', async ({ page }) => {
	// A second run, accepted this time: two more records, five in all.
	answer = 200;
	await submitAndSettle(1);
	expect((await listFor(webhookId)).length).toBeGreaterThanOrEqual(5);

	await page.goto(panelUrl([`webhook_id=${webhookId}`], { limit: '2' }));
	await page.waitForLoadState('networkidle');
	const seen: string[] = [];
	seen.push(...(await rowIds(page)));
	await page.getByTestId('deliveries-next').click();
	await expect(page).toHaveURL(/page=2/);
	seen.push(...(await rowIds(page)));
	await page.getByTestId('deliveries-next').click();
	await expect(page).toHaveURL(/page=3/);
	const third = await rowIds(page);
	seen.push(...third);
	await page.reload();
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-page-status')).toContainText('Page 3');
	expect(await rowIds(page)).toEqual(third);
	await page.getByTestId('deliveries-previous').click();
	await expect(page).toHaveURL(/page=2/);
	expect(new Set(seen).size, `no delivery repeats across pages: ${seen.join(', ')}`).toBe(seen.length);
	expect(seen.length).toBeGreaterThanOrEqual(5);
	// Newest first, as the engine answers.
	const all = (await listFor(webhookId)).map((d) => d.id);
	expect(seen).toEqual(all.slice(0, seen.length));

	// A property filter on page 2: back to page 1, no cursor, the limit kept.
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'deliveries', { field: 'delivered', op: OP.equals, select: 'true' });
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual(['delivered=true', `webhook_id=${webhookId}`]);
	const params = new URL(page.url()).searchParams;
	expect(params.get('cursor')).toBeNull();
	expect(params.get('page')).toBeNull();
	expect(params.get('limit')).toBe('2');
	// A property-filter cursor: the delivered records — the second run's two
	// and the redelivery — two per page, no repeat and no gap.
	const delivered = (await listFor(webhookId)).filter((d) => d.delivered).map((d) => d.id);
	expect(delivered).toHaveLength(3);
	expect(await rowIds(page)).toEqual(delivered.slice(0, 2));
	await expect(page.getByTestId('deliveries-page-status')).toContainText('Page 1');
	await page.getByTestId('deliveries-next').click();
	await expect(page).toHaveURL(/page=2/);
	await expect.poll(() => rowIds(page)).toEqual(delivered.slice(2));
	await expect(page.getByTestId('deliveries-next')).toHaveAttribute('aria-disabled', 'true');
	const walked = delivered;
	note(`D5 delivered=true at size 2 walked ${walked.join(' ')}`);
});

test('D6: the CLI equivalents name the same listing', async ({ page }) => {
	await page.goto(panelUrl([`webhook_id=${webhookId}`, 'delivered=false', `run_id=${firstRun}`], { limit: '2' }));
	await page.waitForLoadState('networkidle');
	const filters = await cliText(page, 'deliveries-filters-cli');
	expect(filters).toBe(
		`orkctl webhook deliveries --filter delivered=false --filter run_id=${firstRun} --filter webhook_id=${webhookId}`
	);
	const listing = await cliText(page, 'deliveries-cli');
	expect(listing).toBe(`${filters} --limit 2`);
	note(`D6 CLI equivalent: ${listing}`);
});

test('a failed deliveries read shows an error beside an intact webhook table', async ({ page }) => {
	await page.goto(panelUrl([`webhook_id=${webhookId}`]));
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-table')).toBeVisible();

	// Only the deliveries read fails: the webhook table must still render.
	await page.route('**/orkstack.v1.WebhookService/ListDeliveries', (route) => route.abort());
	await addListFilter(page, 'deliveries', { field: 'delivered', op: OP.equals, select: 'true' });
	await page.getByTestId('deliveries-filters-apply').click();
	await expect(page.getByTestId('deliveries-error')).toBeVisible();
	await expect(page.getByTestId('deliveries-empty')).toHaveCount(0);
	await expect(page.getByTestId(`webhook-${webhookId}`)).toBeVisible();
	await page.unroute('**/orkstack.v1.WebhookService/ListDeliveries');

	// The engine itself down: the read fails with Retry; back up, Retry renders.
	// The engine serves the page as well as the read, so the page stays open
	// and the read that fails is its next one, made in the browser: the
	// filter changed in the tab, with no document to fetch.
	await page.waitForLoadState('networkidle');
	await expect(page.getByTestId('deliveries-filter-field-0')).toHaveText('Delivered (delivered)');
	await selectOption(page, 'deliveries-filter-value-0', 'false');
	await stop(engine);
	await waitFor('engine port closed', async () => !(await tcpOpen(ENGINE_PORT)));
	await page.getByTestId('deliveries-filters-apply').click();
	await expect.poll(() => deliveryFilters(page)).toEqual(['delivered=false', `webhook_id=${webhookId}`]);
	await expect(page.getByTestId('deliveries-error')).toBeVisible();
	await expect(page.getByTestId('deliveries-error-retry')).toBeVisible();
	await page.screenshot({ path: out('deliveries-read-failed.png'), fullPage: true });
	engine = startEngine();
	await waitForEngine(ENGINE);
	await page.getByTestId('deliveries-error-retry').click();
	await expect(page.getByTestId('deliveries-table')).toBeVisible();
	await expect(page.getByTestId('deliveries-error')).toHaveCount(0);
});
