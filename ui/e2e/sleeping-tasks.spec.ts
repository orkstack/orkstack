import { selectTask, taskMapCellCenter } from './uiHelpers';
import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureBucket,
	killStarted,
	noteTo,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('sleeping-tasks');

// Sleeping tasks against a real engine, Temporal and MinIO, with no Orker at
// all: a sleep starts no container. A sleeping task's row says how long it
// sleeps, its panel names its wake-up time and counts down to it, its graph
// node and its task map cell name the wake-up time, a short sleep wakes and
// succeeds live from the event stream, and an operator's cancel ends a long
// one CANCELED before its time.
const ENGINE = 'http://127.0.0.1:18429';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('sleeping-tasks');
const STAMP = Date.now().toString(36);
const NS = `sleeps-${STAMP}`;
const NAME = 'soak-then-promote';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'sleeping-tasks')
	: path.join(uiDir, 'test-results', 'sleeping-tasks');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

// soak sleeps an hour, so the test decides when the run ends; blink and its
// dependant wake within seconds, so the page watches a sleep end live.
function workflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
description: A long soak beside a short blink.
tasks:
  - id: soak
    sleep:
      seconds: 3600
  - id: blink
    sleep:
      seconds: 3
  - id: after
    needs: [blink]
    sleep:
      seconds: 1
`;
}

interface RunAnswer {
	run?: { id?: string; taskRuns?: { taskId?: string; status?: string; sleep?: { seconds?: number; wakeAt?: string } }[] };
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18429)) throw new Error('port 18429 already in use');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ...uiEnv(), ORK_HTTP_ADDR: '127.0.0.1:18429'
	});
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'sleeping tasks' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow(), namespace: NS });
	note(`namespace ${NS}, orkflow ${NAME}@0.1.0, engine ${ENGINE}, ui ${UI}, no Orker`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

async function submit(): Promise<string> {
	const answer = await rpc<RunAnswer>('RunService/SubmitRun', { orkflowName: NAME, namespace: NS });
	const runId = answer.run?.id ?? '';
	expect(runId).not.toBe('');
	return runId;
}

/** soak's wake-up time as the run records it, once it is sleeping. */
async function soakWakeAt(runId: string): Promise<string> {
	let wakeAt = '';
	await waitFor('soak to be sleeping with a wake-up time', async () => {
		const answer = await rpc<RunAnswer>('RunService/GetRun', { runId, namespace: NS });
		const soak = answer.run?.taskRuns?.find((t) => t.taskId === 'soak');
		wakeAt = soak?.status === 'STATUS_SLEEPING' ? (soak.sleep?.wakeAt ?? '') : '';
		return wakeAt !== '';
	});
	return wakeAt;
}

test('a sleeping task shows its wake-up time on its row, its panel, its node and its map cell', async ({ page }) => {
	const runId = await submit();
	const wakeAt = await soakWakeAt(runId);
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);

	// The row: the status. How long it sleeps is its panel's to say.
	await expect(page.getByTestId('task-soak-status')).toHaveText('Sleeping', { timeout: 60_000 });
	await expect(page.getByTestId('task-soak')).not.toContainText('sleeps');

	// The panel: its state, when it wakes, and how long is left.
	await selectTask(page, 'soak');
	await expect(page.getByTestId('task-soak-sleep-state')).toHaveText('Sleeping for 1h');
	const wakesLocale = await page.evaluate((iso) => new Date(iso).toLocaleString(), wakeAt);
	await expect(page.getByTestId('task-soak-sleep-wakes')).toHaveText(wakesLocale);
	await expect(page.getByTestId('task-soak-sleep-remaining')).toHaveText(/^in (59m|1h0m)$/);
	// No container: no log pane follows a sleep.
	await expect(page.getByTestId('task-log-state-soak')).toHaveCount(0);

	// The graph node and the task map's cell name the wake-up time.
	const clock = await page.evaluate((iso) => new Date(iso).toLocaleTimeString('en-GB', { hour12: false }), wakeAt);
	await expect(page.getByTestId('run-dag-node-soak')).toContainText(`wakes ${clock}`);
	const cell = await taskMapCellCenter(page, 0);
	await page.mouse.move(cell.x, cell.y);
	await expect(page.getByTestId('task-map-readout')).toContainText(`soak · Sleeping · wakes ${clock}`);
	note(`run ${runId}: soak SLEEPING, wake_at ${wakeAt}; panel "${wakesLocale}", node and map "wakes ${clock}"`);
	await shot(page, 'screenshot-sleeping.png');

	// The short sleep and its dependant wake live, from the event stream.
	await expect(page.getByTestId('task-after-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('task-blink-status')).toHaveText('Succeeded');
	await selectTask(page, 'blink');
	await expect(page.getByTestId('task-blink-sleep-state')).toHaveText('Woke after 3s');
	await expect(page.getByTestId('run-status')).toHaveText('Running');
	await shot(page, 'screenshot-woke.png');

	// An operator's cancel ends the long sleep before its time.
	await rpc('RunService/CancelRun', { runId, namespace: NS, reason: 'e2e: end the soak' });
	await expect(page.getByTestId('run-status')).toHaveText('Canceled', { timeout: 60_000 });
	await expect(page.getByTestId('task-soak-status')).toHaveText('Canceled');
	await selectTask(page, 'soak');
	await expect(page.getByTestId('task-soak-sleep-state')).toHaveText('Canceled while sleeping');
	note(`run ${runId}: blink and after woke and succeeded live; cancel ended soak CANCELED`);
	await shot(page, 'screenshot-canceled.png');
});
