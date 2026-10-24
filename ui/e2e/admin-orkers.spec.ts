import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
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
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('admin-orkers');

// Admin → Pools → Show Orkers, against a real engine, two real Orkers on
// `main`, real Temporal and real MinIO (docs/DEPLOY.md "Removing an Orker safely: cordon and drain").
//
// What is measured here, each in a browser:
//
//   1. the pool card lists its two Orkers `ready`; the cordon dialog refuses a
//      blank reason, cordons with one, and the row reads `cordoned` with the
//      reason; the row's CLI equivalent quotes a reason containing a quote;
//   2. Uncordon returns the row to `ready`;
//   3. an Orker that stops answering (SIGSTOP: its poller record stays fresh,
//      it answers nothing) is `unreachable` with the reason and no task
//      count — never idle — and `ready` again after SIGCONT;
//   4. a server error in the cordon dialog is shown in the dialog: an Orker
//      that has stopped (SIGTERM) and left the poller window is cordoned from
//      a stale list, the dialog shows NotFound, and the list re-reads
//      without it.
//
// Process hygiene: this spec signals ONLY the processes it started, by
// handle (killStarted).

const ENGINE_PORT = '18171';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14271';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;

const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-orkers-${STAMP}`;
const ORKER_A = `orker-ui-a-${STAMP}`;
const ORKER_B = `orker-ui-b-${STAMP}`;
const REASON = "it's the CVE-2026-1234 runtime patch";

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-orkers')
	: path.join(uiDir, 'test-results', 'admin-orkers');
const TRANSCRIPT = 'admin-orkers-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];
let orkerB: ChildProcess | undefined;

async function stop(child: ChildProcess | undefined): Promise<void> {
	if (!child || child.exitCode !== null || child.signalCode) return;
	child.kill('SIGCONT');
	child.kill('SIGTERM');
	const deadline = Date.now() + 15_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, 100));
	}
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

function startOrker(id: string, root: string): ChildProcess {
	const child = spawnLogged(outPath(`${id}.log`), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv(root),
		ORK_ORKER_ID: id
	});
	mine.push(child);
	return child;
}

interface ListOrkersAnswer {
	orkers?: { orkerId: string; unavailableReason?: string }[];
}

const listOrkers = (body: unknown) => callEngine<ListOrkersAnswer>(ENGINE, 'ActivityService/ListOrkers', body);

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

async function openOrkers(page: Page): Promise<void> {
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=default`);
	await expect(page.getByTestId('pool-card-main')).toBeVisible();
	await page.getByTestId('pool-orkers-toggle-main').click();
	await expect(page.getByTestId('pool-orkers-caption-main')).toContainText(
		'the same whichever namespace is selected'
	);
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# Admin → Pools → Show Orkers: the Orker inventory and the cordon in a browser.',
			'#',
			'# Written by ui/e2e/admin-orkers.spec.ts against a real engine and two real Orkers',
			'# on real Temporal + MinIO.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkers:             ${ORKER_A}, ${ORKER_B} (both on pool main)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	mine.push(
		spawnLogged(outPath('engine.log'), bin('ork'), [], {
			...clusterEnv(TEMPORAL_NAMESPACE),
			...storeEnv(BUCKET),
			ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
			ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
		})
	);
	await waitForEngine(ENGINE);
	startOrker(ORKER_A, `orkers-a-${STAMP}`);
	orkerB = startOrker(ORKER_B, `orkers-b-${STAMP}`);
	await waitFor(
		'both Orkers listed and answering',
		async () => {
			const answer = await listOrkers({ filters: [{ field: 'pool', op: 'FILTER_OPERATOR_EQUALS', values: ['main'] }] });
			const ready = (answer.orkers ?? []).filter((o) => !o.unavailableReason).map((o) => o.orkerId);
			return ready.includes(ORKER_A) && ready.includes(ORKER_B);
		},
		120_000
	);
	mine.push(
		spawnLogged(
			outPath('ui.log'),
			'node',
			['build'],
			{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
			uiDir
		)
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	note(`engine up at ${ENGINE}; ui at ${UI_ORIGIN}; ${ORKER_A} and ${ORKER_B} listed on main`);
});

test.afterAll(async () => {
	for (const child of mine.splice(0)) await stop(child);
	removeBucket(BUCKET);
});

test('Pools shows its Orkers and cordons one with a reason', async ({ page }) => {
	await openOrkers(page);
	for (const id of [ORKER_A, ORKER_B]) {
		await expect(page.getByTestId(`orker-row-${id}`)).toBeVisible();
		await expect(page.getByTestId(`orker-state-${id}`)).toHaveText('ready');
	}

	await page.getByTestId(`orker-cordon-${ORKER_A}`).click();
	const dialog = page.getByTestId('orker-cordon-dialog');
	await expect(dialog).toBeVisible();
	await expect(page.getByTestId('orker-cordon-body')).toContainText(`${ORKER_A} is running 0 tasks and hosts 0 pins`);
	await expect(page.getByTestId('orker-cordon-accept')).toBeDisabled();
	await page.getByTestId('orker-cordon-reason').fill('   ');
	await expect(page.getByTestId('orker-cordon-accept')).toBeDisabled();
	await page.getByTestId('orker-cordon-reason').fill(REASON);
	await expect(page.getByTestId('orker-cordon-accept')).toBeEnabled();
	await shot(page, 'admin-orkers-cordon-dialog.png');
	await page.getByTestId('orker-cordon-accept').click();
	await expect(dialog).toBeHidden();

	await expect(page.getByTestId(`orker-state-${ORKER_A}`)).toHaveText('cordoned');
	await expect(page.getByTestId(`orker-reason-${ORKER_A}`)).toContainText(REASON);
	await expect(page.getByTestId(`orker-state-${ORKER_B}`)).toHaveText('ready');

	await page.getByTestId(`orker-cli-${ORKER_A}-trigger`).click();
	const popover = page.getByTestId(`orker-cli-${ORKER_A}-popover`);
	const expected = `orkctl orker cordon ${ORKER_A} --reason 'it'"'"'s the CVE-2026-1234 runtime patch'`;
	await expect(popover).toContainText(expected);
	await shot(page, 'admin-orkers-cordoned.png');
	note(`cordoned ${ORKER_A} with ${JSON.stringify(REASON)}; CLI equivalent: ${expected}`);
	const answer = await listOrkers({ filters: [{ field: 'orker_id', op: 'FILTER_OPERATOR_EQUALS', values: [ORKER_A] }] });
	note(`ListOrkers(${ORKER_A}) -> ${JSON.stringify(answer.orkers?.[0])}`);
});

test('Uncordon returns the row to ready', async ({ page }) => {
	await openOrkers(page);
	await expect(page.getByTestId(`orker-state-${ORKER_A}`)).toHaveText('cordoned');
	await page.getByTestId(`orker-uncordon-${ORKER_A}`).click();
	await expect(page.getByTestId(`orker-state-${ORKER_A}`)).toHaveText('ready');
	await expect(page.getByTestId(`orker-reason-${ORKER_A}`)).toHaveCount(0);
	await shot(page, 'admin-orkers-uncordoned.png');
	note(`uncordoned ${ORKER_A}: the row reads ready`);
});

test('An Orker that stops answering is unreachable, not idle', async ({ page }) => {
	test.setTimeout(240_000);
	await openOrkers(page);
	await expect(page.getByTestId(`orker-state-${ORKER_B}`)).toHaveText('ready');
	orkerB!.kill('SIGSTOP');
	note(`SIGSTOP ${ORKER_B} (pid ${orkerB!.pid})`);
	try {
		await page.getByTestId('pool-orkers-refresh-main').click();
		await expect(page.getByTestId(`orker-state-${ORKER_B}`)).toHaveText('unreachable', { timeout: 60_000 });
		await expect(page.getByTestId(`orker-unavailable-${ORKER_B}`)).toContainText('Could not be asked');
		await expect(page.getByTestId(`orker-tasks-${ORKER_B}`)).toHaveCount(0);
		await expect(page.getByTestId(`orker-state-${ORKER_A}`)).toHaveText('ready');
		await shot(page, 'admin-orkers-unreachable.png');
		note(`while stopped: ${(await page.getByTestId(`orker-unavailable-${ORKER_B}`).textContent())?.trim()}`);
	} finally {
		orkerB!.kill('SIGCONT');
		note(`SIGCONT ${ORKER_B}`);
	}
	await expect(async () => {
		await page.getByTestId('pool-orkers-refresh-main').click();
		await expect(page.getByTestId(`orker-state-${ORKER_B}`)).toHaveText('ready', { timeout: 10_000 });
	}).toPass({ timeout: 90_000 });
	await shot(page, 'admin-orkers-ready-again.png');
	note(`after SIGCONT ${ORKER_B} reads ready`);
});

test('A server error in the cordon dialog is shown, not swallowed', async ({ page }) => {
	test.setTimeout(300_000);
	await openOrkers(page);
	await expect(page.getByTestId(`orker-state-${ORKER_B}`)).toHaveText('ready');
	// The list above is now stale on purpose: stop B and wait until nothing
	// polls its channel, without re-reading the page.
	await stop(orkerB);
	note(`SIGTERM ${ORKER_B}; waiting for its poller record to leave the freshness window`);
	await waitFor(
		`${ORKER_B} to leave the poller window`,
		async () => ((await listOrkers({ filters: [{ field: 'orker_id', op: 'FILTER_OPERATOR_EQUALS', values: [ORKER_B] }] })).orkers ?? []).length === 0,
		200_000
	);
	note(`${ORKER_B} is no longer listed by the engine`);

	await page.getByTestId(`orker-cordon-${ORKER_B}`).click();
	await page.getByTestId('orker-cordon-reason').fill('cordon a host that is gone');
	await page.getByTestId('orker-cordon-accept').click();
	const error = page.getByTestId('orker-cordon-dialog-error');
	await expect(error).toBeVisible();
	await expect(error).toContainText(`no Orker "${ORKER_B}" is polling`);
	await expect(page.getByTestId(`orker-row-${ORKER_B}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-row-${ORKER_A}`)).toBeVisible();
	await shot(page, 'admin-orkers-cordon-error.png');
	note(`the dialog shows: ${(await error.textContent())?.trim()}`);
});
