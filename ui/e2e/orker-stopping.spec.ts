import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { mkdirSync } from 'node:fs';
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

// Admin → Pools while an Orker stops drain-first, against a real engine, a real
// Orker, real Temporal and the real store (docs/DEPLOY.md "Removing an Orker
// safely"). The Orker is sent SIGTERM while it runs a task, with a grace long
// enough that the task is still running while the page is read.
//
// What is measured here, in a browser:
//
//   1. a stopping Orker reads `stopping`, apart from an operator's cordon, with
//      a note saying it takes no new work and finishes what it runs, since
//      when, and when its grace ends;
//   2. it offers no Cordon, and its pool counts it stopping;
//   3. the page is clean under axe (WCAG 2.2 AA) in that state;
//   4. a second SIGTERM ends the grace, and the Orker exits.

const BUCKET = ownBucket('orker-stopping');
const ENGINE_PORT = '19051';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-stopping-${STAMP}`;
const ORKER = `orker-ui-stopping-${STAMP}`;
const ORKFLOW = `stopping-hold-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'orker-stopping')
	: path.join(uiDir, 'test-results', 'orker-stopping');
const TRANSCRIPT = 'orker-stopping-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];
let orker: ChildProcess | undefined;

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
	orkers?: { orkerId: string; cordoned?: boolean; stopping?: object; runningTasks?: number; unavailableReason?: string }[];
}

async function row() {
	const answer = await callEngine<ListOrkersAnswer>(ENGINE, 'ActivityService/ListOrkers', {
		filters: [{ field: 'orker_id', op: 'FILTER_OPERATOR_EQUALS', values: [ORKER] }]
	});
	return answer.orkers?.[0];
}

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	startTranscript(
		outPath(TRANSCRIPT),
		[
			'# Admin → Pools while an Orker stops drain-first, in a browser.',
			'#',
			'# Written by ui/e2e/orker-stopping.spec.ts against a real engine and a real Orker',
			'# on real Temporal and object storage.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orker:              ${ORKER}`,
			''
		].join('\n')
	);
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (await tcpOpen(Number(ENGINE_PORT))) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
	ensureActivityImages();
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
	orker = spawnLogged(outPath(`${ORKER}.log`), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv(`stopping-${STAMP}`),
		ORK_ORKER_ID: ORKER,
		// Long enough that the task is still running while the page is read;
		// the spec ends it with a second signal.
		ORK_ORKER_STOP_GRACE: '30m'
	});
	mine.push(orker);
	await waitFor('the Orker listed and answering', async () => {
		const answer = await row();
		return Boolean(answer) && !answer?.unavailableReason;
	}, 120_000);
	await waitFor('built ui', async () => (await fetch(`${ENGINE}/_app/version.json`)).ok);
	await callEngine(ENGINE, 'OrkflowService/ApplyOrkflow', {
		yaml: `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: One task a stopping Orker lets finish.

tasks:
  - id: hold
    uses: shell
    timeout_seconds: 600
    cancellation_grace_period_seconds: 1
    with:
      op: exec
      command: ["sleep", "600"]
`
	});
});

test.afterAll(async () => {
	for (const child of mine.splice(0)) await stop(child);
	removeBucket(BUCKET);
});

test('A stopping Orker reads stopping, offers no cordon, and its pool counts it', async ({ page }) => {
	test.setTimeout(240_000);
	const run = await callEngine<{ run: { id: string } }>(ENGINE, 'RunService/SubmitRun', { orkflowName: ORKFLOW });
	note(`${run.run.id} submitted`);
	await waitFor('the task running on the Orker', async () => (await row())?.runningTasks === 1, 120_000);

	orker!.kill('SIGTERM');
	note('sent SIGTERM to the Orker');
	await waitFor('the Orker reads stopping', async () => Boolean((await row())?.stopping), 60_000);

	await page.goto(`${ENGINE}/admin/pools?ns=default`);
	await expect(page.getByTestId(`orker-state-${ORKER}`)).toHaveText('stopping');
	const stoppingNote = page.getByTestId(`orker-stopping-${ORKER}`);
	await expect(stoppingNote).toContainText('takes no new work and finishes what it runs');
	await expect(stoppingNote).toContainText('Stopping since ');
	await expect(stoppingNote).toContainText('its grace ends ');
	await expect(page.getByTestId(`orker-reason-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-cordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-uncordon-${ORKER}`)).toHaveCount(0);
	await expect(page.getByTestId(`orker-tasks-${ORKER}`)).toHaveText('1');
	await expect(page.getByTestId('pool-stopping-main')).toContainText('1 stopping');
	await shot(page, 'orker-stopping.png');
	note(`row: ${(await stoppingNote.textContent())?.trim()}`);

	// A phone: the note wraps in its cell.
	await page.setViewportSize({ width: 375, height: 812 });
	const box = await stoppingNote.boundingBox();
	expect(box?.width ?? Infinity).toBeLessThanOrEqual(18 * 16);
	await shot(page, 'orker-stopping-phone.png');
	await page.setViewportSize({ width: 1440, height: 900 });

	const violations = (await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze()).violations;
	expect(violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(', ')}`)).toEqual([]);
});

test('A second signal ends the grace, and the Orker exits', async () => {
	test.setTimeout(120_000);
	// Signals within a second of each other count as one stop.
	await new Promise((r) => setTimeout(r, 1_500));
	orker!.kill('SIGTERM');
	note('sent a second SIGTERM');
	await waitFor('the Orker exits', async () => orker!.exitCode !== null || Boolean(orker!.signalCode), 90_000);
	expect(orker!.exitCode).toBe(0);
	note(`the Orker exited ${orker!.exitCode}`);
});
