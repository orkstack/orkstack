import { selectTask } from './uiHelpers';
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
import { fillYaml } from './yaml';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-timeout');

// A run's own timeout (docs/DSL.md "Run timeout") against a real engine,
// Temporal and the object store, with no Orker at all: the run's one task
// sleeps an hour, so only the run's 60 s timeout can end it, and its failure
// handler is a sleep too. The Orkflow page names the timeout; the run header
// counts down to the deadline from the snapshot; past it the run fails
// RUN_TIMED_OUT, the header says when, the stopped row says why and the
// handler still ran; and the editor refuses an integer timeout.
const ENGINE = 'http://127.0.0.1:18719';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('run-timeout');
const STAMP = Date.now().toString(36);
const NS = `run-timeout-${STAMP}`;
const NAME = 'bounded-soak';
const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'run-timeout') : path.join(uiDir, 'test-results', 'run-timeout');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

function workflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
description: An hour-long soak a one-minute run timeout ends, and a handler after it.
timeout: 1m
tasks:
  - id: soak
    sleep:
      seconds: 3600
  - id: report
    needs: [soak]
    join: any_failed
    sleep:
      seconds: 1
`;
}

interface RunAnswer {
	run?: {
		id?: string;
		status?: string;
		error?: { code?: string };
		timeout?: { seconds?: string; deadline?: string; hardEnd?: string; timedOut?: boolean };
		taskRuns?: { taskId?: string; status?: string; error?: { code?: string } }[];
	};
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18719)) throw new Error('port 18719 already in use');

	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: '127.0.0.1:18719',
		ORK_RUN_TIMEOUT_HANDLER_GRACE: '30s'
	});
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'run timeout' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow(), namespace: NS });
	note(`namespace ${NS}, orkflow ${NAME}@0.1.0 (timeout 1m), engine ${ENGINE}, no Orker`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

/** The countdown the header shows: `1m · 1m left` or `1m · 42s left`. */
const COUNTDOWN = /^1m · (?:1m|\d+s) left$/;

/** The seconds left the header shows. */
async function secondsLeft(page: Page): Promise<number> {
	const text = ((await page.getByTestId('run-timeout-value').textContent()) ?? '').trim();
	if (text === '1m · 1m left') return 60;
	const match = /^1m · (\d+)s left$/.exec(text);
	if (!match) throw new Error(`the Timeout fact reads ${JSON.stringify(text)}, not a countdown`);
	return Number(match[1]);
}

test('the Orkflow page names the run timeout', async ({ page }) => {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}`);
	await expect(page.getByTestId('orkflow-timeout')).toContainText('Timeout');
	await expect(page.getByTestId('orkflow-timeout')).toContainText('1m');
	await shot(page, 'screenshot-orkflow.png');
});

test('the run header counts down, and past the deadline says the run timed out and why the row stopped', async ({ page }) => {
	const answer = await rpc<RunAnswer>('RunService/SubmitRun', { orkflowName: NAME, namespace: NS });
	const runId = answer.run?.id ?? '';
	expect(runId).not.toBe('');

	// Precondition: the run started and its deadline is recorded, before it.
	let deadline = '';
	await waitFor('the run to start with a deadline', async () => {
		const read = await rpc<RunAnswer>('RunService/GetRun', { runId, namespace: NS });
		deadline = read.run?.timeout?.deadline ?? '';
		return read.run?.status === 'STATUS_RUNNING' && deadline !== '';
	});
	expect(Date.parse(deadline)).toBeGreaterThan(Date.now());

	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('run-timeout')).toContainText('Timeout');
	await expect(page.getByTestId('run-timeout-value')).toHaveText(COUNTDOWN, { timeout: 15_000 });
	await expect(page.getByTestId('run-timeout-value')).toHaveAttribute('data-tip', `Deadline ${new Date(Date.parse(deadline)).toISOString()}`);
	const first = await secondsLeft(page);
	// The time left goes down from the snapshot's deadline, without a re-read;
	// a page that loaded too close to the deadline to see it fails here, by name.
	expect(first, 'the page loaded with too little time left to watch it count down').toBeGreaterThanOrEqual(5);
	await expect.poll(() => secondsLeft(page), { timeout: 10_000 }).toBeLessThan(first);
	note(`run ${runId}: deadline ${deadline}; header counted down from ${first}s`);
	await shot(page, 'screenshot-counting.png');

	// Past the deadline: the run fails RUN_TIMED_OUT, and the page says when.
	await expect(page.getByTestId('run-status')).toHaveText('Failed', { timeout: 120_000 });
	const clock = await page.evaluate((iso) => new Date(iso).toLocaleTimeString('en-GB', { hour12: false }), deadline);
	await expect(page.getByTestId('run-timeout-value')).toHaveText(`1m · timed out at ${clock}`);
	await expect(page.getByTestId('task-soak-status')).toHaveText('Failed');
	await expect(page.getByTestId('task-soak-status-note')).toHaveText('stopped when the run timed out');
	// The failure handler still ran, within the grace.
	await expect(page.getByTestId('task-report-status')).toHaveText('Succeeded');
	// The cause: the stopped task's reason, and the run's own in Why it failed.
	await selectTask(page, 'soak');
	await expect(page.getByTestId('task-soak-failure')).toContainText('TASK_STOPPED_BY_RUN_TIMEOUT');
	await expect(page.getByTestId('run-why')).toContainText('RUN_TIMED_OUT');

	const final = await rpc<RunAnswer>('RunService/GetRun', { runId, namespace: NS });
	expect(final.run?.error?.code).toBe('RUN_TIMED_OUT');
	expect(final.run?.timeout?.timedOut).toBe(true);
	expect(final.run?.taskRuns?.find((t) => t.taskId === 'soak')?.error?.code).toBe('TASK_STOPPED_BY_RUN_TIMEOUT');
	note(`run ${runId}: FAILED RUN_TIMED_OUT; header "1m · timed out at ${clock}"; soak stopped, report succeeded`);
	await shot(page, 'screenshot-timed-out.png');
});

test('the editor refuses an integer timeout', async ({ page }) => {
	await page.goto(`${UI}/orkflows/+new?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', workflow().replace('timeout: 1m', 'timeout: 3600'));
	const finding = page.locator('[data-testid="apply-yaml-finding"][data-line="6"]');
	await expect(finding).toBeVisible();
	await expect(finding).toHaveAttribute('data-source', 'schema');
	await shot(page, 'screenshot-editor.png');
});
