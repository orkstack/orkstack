import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { mkdirSync, statSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureBucket,
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

// Admin → Pools for an Orker that offers a device profile (E91 P5;
// docs/DEPLOY.md, "Running a device Orker"), against a real engine and a real
// Orker on real Temporal and object storage. The profile passes the
// usb-serial test node (CONTRIBUTING.md, "The usb-serial test device"), and
// its probe image is one no registry holds: the Orker boots, offers the
// profile and holds it unavailable (PROBE_IMAGE_UNAVAILABLE), cordoning
// itself while its pool stays live. In a browser:
//
//   1. the row says `profile unavailable`, never `cordoned`, and offers Cordon,
//      not Uncordon: no operator set it;
//   2. the profile is listed by name and kind, with its cause, and the row
//      says what was found, its code and that it ends by itself;
//   3. an operator's cordon beside it shows both, and Uncordon clears only
//      the operator's, saying the profile still holds the Orker;
//   4. the pool card says `Profile unavailable`, never `Cordoned`: the pool
//      stays live, and its runs wait for the profile;
//   5. the page is clean under axe (WCAG 2.2 AA), in light and dark and at a
//      phone's width.

const BUCKET = ownBucket('orker-profiles');
const ENGINE_PORT = '19061';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-profiles-${STAMP}`;
const ORKER = `orker-ui-profiles-${STAMP}`;
const DEVICE = '/dev/ttyUSB199';
// A digest no registry holds: the probe image can never be pulled.
const PROBE = `127.0.0.1:5000/orkstack-ui-profiles-missing@sha256:${'0f'.repeat(32)}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'orker-profiles') : path.join(uiDir, 'test-results', 'orker-profiles');
const outPath = proofOut(PROOF_DIR);

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];

async function stop(child: ChildProcess): Promise<void> {
	if (child.exitCode !== null || child.signalCode) return;
	child.kill('SIGTERM');
	const deadline = Date.now() + 15_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

interface ListOrkersAnswer {
	orkers?: { orkerId: string; unavailableReason?: string; profiles?: { name: string }[]; profileCordons?: { reason?: string }[] }[];
}

async function row() {
	const answer = await callEngine<ListOrkersAnswer>(ENGINE, 'ActivityService/ListOrkers', {
		filters: [{ field: 'orker_id', op: 'FILTER_OPERATOR_EQUALS', values: [ORKER] }]
	});
	return answer.orkers?.[0];
}

const shot = (page: Page, name: string) => page.screenshot({ path: outPath(name), fullPage: true });

async function open(page: Page): Promise<void> {
	await page.goto(`${ENGINE}/admin/pools?ns=default`);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toBeVisible();
}

async function axeClean(page: Page): Promise<void> {
	const violations = (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze()).violations;
	expect(violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(', ')}`)).toEqual([]);
}

test.beforeAll(async () => {
	// The host's usb-serial test node is a prerequisite the harness checks
	// itself: a host without it fails here, never skips.
	const node = statSync(DEVICE, { throwIfNoEntry: false });
	if (!node?.isCharacterDevice()) throw new Error(`${DEVICE} is not a character device: set it up as CONTRIBUTING.md, "The usb-serial test device", says`);
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	const store = new URL(storeEnv(BUCKET).ORK_S3_ENDPOINT);
	if (!(await tcpOpen(Number(store.port)))) throw new Error(`the object store is not reachable on ${store.host}`);
	if (await tcpOpen(Number(ENGINE_PORT))) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
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
			...orkerEnv(`profiles-${STAMP}`),
			ORK_ORKER_ID: ORKER,
			ORK_ORKER_PROFILES: [
				'profiles:',
				`  probe_image: ${PROBE}`,
				'  recovery_interval: 1s',
				'  sensor:',
				'    kind: devices',
				'    devices:',
				`      - path: ${DEVICE}`,
				'        container_path: /dev/sensor',
				'        permissions: rw'
			].join('\n')
		})
	);
	await waitFor(
		'the Orker listed with its profile held unavailable',
		async () => {
			const orker = await row();
			return Boolean(orker && !orker.unavailableReason && orker.profiles?.some((p) => p.name === 'sensor') && (orker.profileCordons?.length ?? 0) > 0);
		},
		120_000
	);
	await waitFor('built ui', async () => (await fetch(`${ENGINE}/_app/version.json`)).ok);
});

test.afterAll(async () => {
	for (const child of mine.reverse()) await stop(child);
	removeBucket(BUCKET);
});

test('an unavailable profile reads as its own state with its cause, and only an operator’s cordon offers Uncordon', async ({ page }) => {
	await open(page);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText(/profile unavailable/i);
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-uncordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-cordon-${ORKER}`)).toBeVisible();
	// The pool stays live while only an unavailable profile holds its Orker.
	await expect(page.getByTestId('pool-state-main')).toHaveText('Profile unavailable');
	await expect(page.getByTestId('pool-profile-unavailable-main')).toHaveText('1 profile unavailable');
	const profile = page.getByTestId(`orker-profile-${ORKER}-sensor`);
	await expect(profile).toContainText('sensor');
	await expect(profile).toContainText('devices');
	await expect(page.getByTestId(`orker-profile-state-${ORKER}-sensor`)).toHaveText('unavailable');
	const cause = page.getByTestId(`orker-profile-cause-${ORKER}-sensor`);
	await expect(cause).toContainText('Profile sensor unavailable since');
	await expect(cause).toContainText('(PROBE_IMAGE_UNAVAILABLE)');
	await expect(cause).toContainText('Once that is put right, it returns by itself after consecutive good checks.');
	await expect(page.getByTestId(`orker-profile-units-${ORKER}-sensor`)).toHaveCount(0);
	await axeClean(page);
	await shot(page, 'profile-unavailable.png');
	await page.getByTestId(`orker-row-${ORKER}`).screenshot({ path: outPath('profile-unavailable-row.png') });
});

test('an operator’s cordon beside a profile shows both, and Uncordon clears only the operator’s', async ({ page }) => {
	await open(page);
	try {
		await page.getByTestId(`orker-cordon-${ORKER}`).click();
		await page.getByTestId('orker-cordon-reason').fill('kernel patch');
		await page.getByTestId('orker-cordon-accept').click();
		await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText(/cordoned/i);
		await expect(page.getByTestId(`orker-reason-${ORKER}`)).toContainText('kernel patch');
		// The operator's cordon takes the Orker out of its pool.
		await expect(page.getByTestId('pool-state-main')).not.toHaveText('Profile unavailable');
		// The profile's cause keeps a stamp of its own beside the operator's.
		await expect(page.getByTestId(`orker-profile-stamp-${ORKER}`)).toHaveText(/profile unavailable/i);
		await expect(page.getByTestId(`orker-profile-cause-${ORKER}-sensor`)).toContainText('(PROBE_IMAGE_UNAVAILABLE)');
		await shot(page, 'profile-and-operator-cordon.png');
		await page.getByTestId(`orker-uncordon-${ORKER}`).click();
		await expect(page.getByTestId('pool-orkers-notice-main')).toHaveText(`Cleared the operator's cordon on ${ORKER}. It stays cordoned because a profile is out of service.`);
		await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText(/profile unavailable/i);
		// The card follows the list beneath it, with no reload.
		await expect(page.getByTestId('pool-state-main')).toHaveText('Profile unavailable');
	} finally {
		// Whatever failed above, the tests after this one meet no operator's cordon.
		await callEngine(ENGINE, 'ActivityService/CordonOrker', { orkerId: ORKER, cordoned: false }).catch(() => undefined);
	}
});

for (const [scheme, width, height, tag] of [
	['dark', 1440, 900, 'dark-wide'],
	['light', 390, 844, 'light-phone'],
	['dark', 390, 844, 'dark-phone']
] as const) {
	test(`the profile and its cause read in ${tag}`, async ({ browser }) => {
		const ctx = await browser.newContext({ colorScheme: scheme, viewport: { width, height } });
		const page = await ctx.newPage();
		try {
			await open(page);
			await expect(page.getByTestId(`orker-profile-cause-${ORKER}-sensor`)).toBeVisible();
			await axeClean(page);
			await shot(page, `profile-unavailable-${tag}.png`);
		} finally {
			await ctx.close();
		}
	});
}
