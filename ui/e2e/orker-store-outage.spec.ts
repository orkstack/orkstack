import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import {
	bin,
	clusterEnv,
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
//   4. the page is clean under axe (WCAG 2.2 AA) in the outage state.

const BUCKET = ownBucket('orker-store-outage');
const ENGINE_PORT = '19011';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-outage-${STAMP}`;
const ORKER = `orker-ui-outage-${STAMP}`;
const REASON = 'runtime patch';

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
	writeFileSync(
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
	await startProxy(store.hostname, Number(store.port));

	mine.push(
		spawnLogged(outPath('engine.log'), bin('ork'), [], {
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
			ORK_ORKER_STORE_CHECK_TIMEOUT: '1s',
			ORK_ORKER_STORE_CHECK_FAILURES: '2',
			ORK_ORKER_STORE_CHECK_RECOVERIES: '2'
		})
	);
	await waitFor('the Orker listed and answering', async () => {
		const orker = await row();
		return Boolean(orker) && !orker?.unavailableReason;
	}, 120_000);
	await waitFor('built ui', async () => (await fetch(`${ENGINE}/_app/version.json`)).ok);
});

test.afterAll(async () => {
	for (const child of mine.splice(0)) await stop(child);
	for (const socket of live) socket.destroy();
	proxy?.close();
	removeBucket(BUCKET);
});

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
	await page.setViewportSize({ width: 1440, height: 900 });

	const violations = (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze()).violations;
	expect(violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(', ')}`)).toEqual([]);
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
	await shot(page, 'orker-store-outage-and-operator.png');

	await page.getByTestId(`orker-uncordon-${ORKER}`).click();
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('object storage unreachable');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-uncordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-cordon-${ORKER}`)).toBeFocused();
	await expect(page.getByTestId('pool-orkers-notice-main')).toContainText('It stays cordoned because object storage unreachable');
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
