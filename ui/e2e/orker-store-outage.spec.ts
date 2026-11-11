import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from './test';
import { selectTask } from './uiHelpers';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
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
	repoRoot,
	rpc as callEngine,
	spawnLogged,
	startTranscript,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

// Admin → Pools while an Orker cannot reach object storage, against a real
// engine, a real Orker running the shipped store check, real Temporal and the
// real store (docs/DEPLOY.md "When object storage goes away"). The Orker alone
// reaches the store through a small proxy this spec owns; the engine dials it
// directly, so cutting the proxy is one Orker losing the store.
//
// What is measured here, in a browser:
//
//   1. a store outage reads `object storage unreachable` with its reason and
//      since, apart from an operator's `cordoned`, and offers no Uncordon;
//   2. an operator's cordon set beside it shows both, and Uncordon clears only
//      the operator's;
//   3. recovery ends the outage by itself, and leaves an operator's cordon;
//   4. the page is clean under axe (WCAG 2.2 AA) in the outage state;
//   5. a run sent during the outage is accepted, its page and `orkctl run
//      inspect` name the outage as what it waits for, and it succeeds on its
//      first attempt once the store returns and the Orker is free.

const BUCKET = ownBucket('orker-store-outage');
const ENGINE_PORT = '19011';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-outage-${STAMP}`;
const ORKER = `orker-ui-outage-${STAMP}`;
const REASON = 'runtime patch';
const ORKFLOW = `store-outage-wait-${STAMP}`;
let waitingRun = '';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'orker-store-outage')
	: path.join(uiDir, 'test-results', 'orker-store-outage');
const TRANSCRIPT = 'orker-store-outage-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];

// The store's proxy: forwards while `cut` is false; while it is true it closes
// every connection, live and new.
let cut = false;
const live = new Set<net.Socket>();
let proxy: net.Server | undefined;
let proxyPort = 0;

function startProxy(upstreamHost: string, upstreamPort: number): Promise<void> {
	proxy = net.createServer((client) => {
		if (cut) {
			client.destroy();
			return;
		}
		const upstream = net.connect(upstreamPort, upstreamHost);
		for (const socket of [client, upstream]) {
			live.add(socket);
			socket.on('close', () => live.delete(socket));
			socket.on('error', () => {
				client.destroy();
				upstream.destroy();
			});
		}
		client.on('close', () => upstream.destroy());
		upstream.on('close', () => client.destroy());
		client.pipe(upstream);
		upstream.pipe(client);
	});
	return new Promise((resolve) => {
		proxy!.listen(0, '127.0.0.1', () => {
			proxyPort = (proxy!.address() as net.AddressInfo).port;
			resolve();
		});
	});
}

function cutStore(): void {
	cut = true;
	for (const socket of live) socket.destroy();
}

function restoreStore(): void {
	cut = false;
}

async function stop(child: ChildProcess): Promise<void> {
	if (child.exitCode !== null || child.signalCode) return;
	child.kill('SIGTERM');
	const deadline = Date.now() + 15_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, 100));
	}
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

interface ListOrkersAnswer {
	orkers?: { orkerId: string; cordoned?: boolean; storeOutage?: { reason?: string }; unavailableReason?: string }[];
}

const listOrkers = (body: unknown) => callEngine<ListOrkersAnswer>(ENGINE, 'ActivityService/ListOrkers', body);

async function row() {
	const answer = await listOrkers({ filters: [{ field: 'orker_id', op: 'FILTER_OPERATOR_EQUALS', values: [ORKER] }] });
	return answer.orkers?.[0];
}

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

async function open(page: Page): Promise<void> {
	await page.goto(`${ENGINE}/admin/pools?ns=default`);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toBeVisible();
}

async function awaitOutage(page: Page, want: boolean): Promise<void> {
	await waitFor(`the store outage ${want ? 'holds' : 'has ended'}`, async () => Boolean((await row())?.storeOutage) === want, 60_000);
	await page.getByTestId('pools-refresh').click();
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	startTranscript(
		outPath(TRANSCRIPT),
		[
			'# Admin → Pools during an object storage outage, in a browser.',
			'#',
			'# Written by ui/e2e/orker-store-outage.spec.ts against a real engine and a real Orker',
			'# on real Temporal and object storage, with only the Orker going through a proxy.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orker:              ${ORKER}`,
			''
		].join('\n')
	);
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	const store = new URL(storeEnv(BUCKET).ORK_S3_ENDPOINT);
	if (!(await tcpOpen(Number(store.port)))) throw new Error(`the object store is not reachable on ${store.host}`);
	if (await tcpOpen(Number(ENGINE_PORT))) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
	ensureActivityImages();
	await startProxy(store.hostname, Number(store.port));

	mine.push(
		spawnLogged(outPath('engine.log'), bin('ork'), [], {
			...catalogEnv,
			...clusterEnv(TEMPORAL_NAMESPACE),
			...storeEnv(BUCKET),
			...uiEnv(),
			ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`
		})
	);
	await waitForEngine(ENGINE);
	mine.push(
		spawnLogged(outPath(`${ORKER}.log`), bin('orker'), [], {
			...clusterEnv(TEMPORAL_NAMESPACE),
			...storeEnv(BUCKET),
			...orkerEnv(`outage-${STAMP}`),
			ORK_S3_ENDPOINT: `http://127.0.0.1:${proxyPort}`,
			ORK_ORKER_ID: ORKER,
			ORK_ORKER_STORE_CHECK_INTERVAL: '1s',
			ORK_ORKER_STORE_CHECK_TIMEOUT: '500ms',
			ORK_ORKER_STORE_CHECK_FAILURES: '2',
			ORK_ORKER_STORE_CHECK_RECOVERIES: '2'
		})
	);
	await waitFor('the Orker listed and answering', async () => {
		const orker = await row();
		return Boolean(orker) && !orker?.unavailableReason;
	}, 120_000);
	await waitFor('built ui', async () => (await fetch(`${ENGINE}/_app/version.json`)).ok);
	await callEngine(ENGINE, 'OrkflowService/ApplyOrkflow', {
		yaml: `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: One task on the pool in the outage.

tasks:
  - id: wait-store
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["echo", "ran after the outage"]
`
	});
});

test.afterAll(async () => {
	for (const child of mine.splice(0)) await stop(child);
	for (const socket of live) socket.destroy();
	proxy?.close();
	removeBucket(BUCKET);
});

/** At a desktop width the Orkers table fits its frame: every row's actions show without scrolling. */
async function expectOrkersFit(page: Page) {
	const overflow = await page.getByRole('table', { name: 'Orkers of pool main' }).evaluate((table) => {
		const frame = table.parentElement as HTMLElement;
		return frame.scrollWidth - frame.clientWidth;
	});
	expect(overflow).toBeLessThanOrEqual(0);
}

test('A store outage reads apart from an operator\'s cordon, and offers no Uncordon', async ({ page }) => {
	test.setTimeout(180_000);
	await open(page);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('ready');
	await expect(page.getByTestId(`orker-outage-${ORKER}`)).toHaveCount(0);

	cutStore();
	note('cut the store');
	await awaitOutage(page, true);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('object storage unreachable');
	const outage = page.getByTestId(`orker-outage-${ORKER}`);
	await expect(outage).toContainText('Cordoned itself since');
	await expect(outage).toContainText('object storage unreachable: the last check');
	await expect(outage).toContainText('resumes by itself');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-uncordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-cordon-${ORKER}`)).toBeVisible();
	await expect(page.getByTestId('pool-store-outages-main')).toContainText('1 cannot reach object storage');
	await expect(page.getByTestId('pool-all-store-outage-main')).toContainText('resume by themselves');
	await shot(page, 'orker-store-outage.png');
	note(`row: ${(await outage.textContent())?.trim()}`);

	// A phone: the sentence wraps in its cell, so it reads without scrolling.
	await page.setViewportSize({ width: 375, height: 812 });
	const noteBox = await outage.boundingBox();
	expect(noteBox?.width ?? Infinity).toBeLessThanOrEqual(18 * 16);
	// Only its icon shows here, and the top bar's Jump button keeps its name.
	await expect(page.getByRole('button', { name: 'Jump to a run, Orkflow or page' })).toBeVisible();

	// A desktop that leaves the card narrow: the stamp stays in its card, and the
	// long sentence does not squeeze the Orker's id into a sliver.
	await page.setViewportSize({ width: 1280, height: 900 });
	const card = await page.getByTestId('pool-card-main').boundingBox();
	const stamp = await page.getByTestId('pool-card-main').locator('.phead .stamp').boundingBox();
	expect((stamp?.x ?? Infinity) + (stamp?.width ?? 0)).toBeLessThanOrEqual((card?.x ?? 0) + (card?.width ?? 0));
	const idCell = await page.getByTestId(`orker-row-${ORKER}`).locator('th').boundingBox();
	expect(idCell?.width ?? 0).toBeGreaterThanOrEqual(8.5 * 16);
	await expectOrkersFit(page);
	await page.setViewportSize({ width: 1440, height: 900 });

	const violations = (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze()).violations;
	expect(violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(', ')}`)).toEqual([]);
});

test('A run sent during the outage is accepted and says it waits for object storage', async ({ page }) => {
	test.setTimeout(180_000);
	// Polls already out when the cordon landed could still take this task, which
	// would use an attempt: the integration test proves the wait; this page proves
	// what a waiting run says, so it needs the attempt still scheduled.
	await waitFor('the Orker is holding no poll', async () => {
		const answer = await callEngine<{ run: { id: string } }>(ENGINE, 'RunService/SubmitRun', { orkflowName: ORKFLOW });
		waitingRun = answer.run.id;
		await new Promise((r) => setTimeout(r, 7_000));
		const inspected = await callEngine<{ attempts?: { state?: string; storeOutageWait?: unknown; nextAttemptAt?: unknown }[] }>(ENGINE, 'RunService/InspectRun', { runId: waitingRun });
		return (inspected.attempts ?? []).some((a) => a.state === 'LIVE_ATTEMPT_STATE_SCHEDULED' && !a.nextAttemptAt && a.storeOutageWait);
	}, 150_000, 1_000);
	note(`${waitingRun} accepted during the outage and waiting`);

	await page.goto(`${ENGINE}/runs/${waitingRun}?ns=default`);
	await expect(page.getByTestId('run-store-outage-note')).toContainText('wait-store is waiting for object storage', { timeout: 30_000 });
	await expect(page.getByTestId('run-store-outage-note')).toContainText('cannot reach it');
	await expect(page.getByTestId('run-capacity-note')).toHaveCount(0);
	await selectTask(page, 'wait-store');
	const live = page.getByTestId('task-wait-store-live');
	await expect(live).toHaveAttribute('data-live', 'store-outage');
	await expect(live).toContainText('Waiting for object storage');
	await expect(page.getByTestId('task-wait-store-card-status')).toHaveText('Waiting for object storage');
	await expect(page.getByTestId('run-waits-store-outage')).toContainText('uses no attempt');
	await shot(page, 'run-waiting-for-object-storage.png');

	const args = ['--server', ENGINE, '--namespace', 'default', 'run', 'inspect', waitingRun];
	const out = execFileSync(bin('orkctl'), args, { cwd: repoRoot, encoding: 'utf8' });
	note(`$ orkctl ${args.join(' ')}\n${out}`);
	expect(out).toContain('wait-store: waiting for object storage for');
	expect(out).toContain('1 orker of pool main cannot reach it');
});

test('An operator\'s cordon beside an outage shows both, and Uncordon clears only its own', async ({ page }) => {
	test.setTimeout(60_000);
	await open(page);
	await page.getByTestId(`orker-cordon-${ORKER}`).click();
	await page.getByTestId('orker-cordon-reason').fill(REASON);
	await page.getByTestId('orker-cordon-accept').click();
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('cordoned');
	await expect(page.getByTestId(`orker-outage-stamp-${ORKER}`)).toHaveText('object storage unreachable');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toContainText(REASON);
	await expect(page.getByTestId(`orker-outage-${ORKER}`)).toContainText("stays cordoned until an operator's cordon is cleared too");
	await page.setViewportSize({ width: 1280, height: 900 });
	await expectOrkersFit(page);
	await shot(page, 'orker-store-outage-and-operator.png');

	await page.getByTestId(`orker-uncordon-${ORKER}`).click();
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('object storage unreachable');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-uncordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-cordon-${ORKER}`)).toBeFocused();
	await expect(page.getByTestId('pool-orkers-notice-main')).toContainText('It stays cordoned because it cannot reach object storage');
	note("Uncordon cleared the operator's cordon and left the outage");
});

test('Recovery ends the outage by itself, and leaves an operator\'s cordon', async ({ page }) => {
	test.setTimeout(120_000);
	await open(page);
	await page.getByTestId(`orker-cordon-${ORKER}`).click();
	await page.getByTestId('orker-cordon-reason').fill(REASON);
	await page.getByTestId('orker-cordon-accept').click();
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('cordoned');

	restoreStore();
	note('restored the store');
	await awaitOutage(page, false);
	await expect(page.getByTestId(`orker-outage-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-outage-stamp-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('cordoned');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toContainText(REASON);
	await shot(page, 'orker-store-recovered-operator-holds.png');

	await page.getByTestId(`orker-uncordon-${ORKER}`).click();
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('ready');
	note('recovered, then uncordoned: ready');
});

test('The run that waited out the outage succeeds on its first attempt', async ({ page }) => {
	test.setTimeout(180_000);
	await page.goto(`${ENGINE}/runs/${waitingRun}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 150_000 });
	await expect(page.getByTestId('run-store-outage-note')).toHaveCount(0);
	const run = await callEngine<{ run: { taskRuns?: { taskId: string; attempt?: number }[] } }>(ENGINE, 'RunService/GetRun', { runId: waitingRun });
	expect(run.run.taskRuns?.find((t) => t.taskId === 'wait-store')?.attempt).toBe(1);
	note(`${waitingRun} succeeded on attempt 1 after the store returned`);
});
