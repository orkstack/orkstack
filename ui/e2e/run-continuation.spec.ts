import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killStarted,
	noteTo,
	orkerEnv,
	proofOut,
	repoRoot,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';

// A run that carries on into a fresh history, in a browser, against the real
// stack (internal/engine/continuation.go; docs/DEPLOY.md §2, "Runs that span
// several executions").
//
// What is measured is that no client can tell: the run page follows the run
// live through every switch to SUCCEEDED with no reconnect or error, its
// event list is contiguous, the runs list shows the run ONCE at its own
// created_at, in created_at order beside two newer runs, and InspectRun
// answers during a hold. That the run really switched — at least twice, once
// after a hold — is confirmed by test/tools/generations, which reads the
// cluster's executions and the engine's operator log: the page never names a
// generation (AGENTS.md §7).
//
// Topology: one engine, one Orker, the production UI build, in a Temporal
// namespace of this spec's own, with continuation on by default — nothing
// sets engine.continuation.enabled, and an inherited value is dropped — and
// tuned for a short run: a 600-event history
// limit (its hold line 360 events of history and reserve), a 120-event
// budget, and a 4-minute restore timeout, whose worst-case history (127
// events) fits the quarter of the limit the engine's boot check requires. A
// 1,000-event limit lets this fan-out finish before its hold line and switch
// only at natural idle points.

const ENGINE = 'http://127.0.0.1:18663';
const ENGINE_WS = 'ws://127.0.0.1:18663';
const UI_ORIGIN = 'http://127.0.0.1:14663';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-continuation';
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const LONG = `continuation-${STAMP}`;
const SHORT = `continuation-short-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-continuation')
	: path.join(uiDir, 'test-results', 'run-continuation');
const TRANSCRIPT = 'run-continuation-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));
const ENGINE_LOG = outPath('engine.log');

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// 25 tasks and a 20-item fan-out: a chain whose idle points are natural
// switch points, then the fan-out (four at a time, each item a few seconds)
// whose refills leave no idle point — so the run holds near its hold line to
// make one — then a flat stage and a last task.
const PARTS = Array.from({ length: 20 }, (_, i) => ({ name: `p${i}` }));

function longOrkflow(): string {
	const tasks: string[] = [];
	for (let i = 1; i <= 10; i++) {
		tasks.push(`  - id: c${i}
    uses: shell
    needs: [${i > 1 ? `c${i - 1}` : ''}]
    timeout_seconds: 120
    with: {op: exec, command: [sh, -c, "echo chain ${i}"]}`);
	}
	tasks.push(`  - id: convert
    uses: shell
    needs: [c10]
    timeout_seconds: 120
    each: {over: '{{ .inputs.parts }}', as: part, max_items: 50, max_parallel: 4}
    with: {op: exec, command: [sh, -c, "echo converting {{ .each.index }}; sleep 2"]}`);
	const stage: string[] = [];
	for (let i = 1; i <= 13; i++) {
		stage.push(`f${i}`);
		tasks.push(`  - id: f${i}
    uses: shell
    needs: [convert]
    timeout_seconds: 120
    with: {op: exec, command: [sh, -c, "echo stage ${i}"]}`);
	}
	tasks.push(`  - id: merge
    uses: shell
    needs: [${stage.join(', ')}]
    timeout_seconds: 120
    with: {op: exec, command: [sh, -c, "echo merged"]}`);
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${LONG}
version: "0.1.0"
description: A run long enough to carry on into fresh histories, twice at least.

inputs:
  parts:
    type: array

tasks:
${tasks.join('\n')}
`;
}

function shortOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${SHORT}
version: "0.1.0"
tasks:
  - id: only
    uses: shell
    timeout_seconds: 60
    with: {op: exec, command: [sh, -c, "echo short"]}
`;
}

type Generations = { executions: number; continued: number; holds: number };

function generations(runID: string): Generations {
	const out = execFileSync('go', ['run', './test/tools/generations', '-run', runID, '-log', ENGINE_LOG], {
		cwd: repoRoot,
		env: { ...process.env, ...clusterEnv(TEMPORAL_NAMESPACE) },
		encoding: 'utf8'
	});
	return JSON.parse(out) as Generations;
}

function holdLogged(runID: string): boolean {
	try {
		return readFileSync(ENGINE_LOG, 'utf8')
			.split('\n')
			.some((line) => line.includes('run pausing new tasks so it can carry on into a fresh history') && line.includes(runID));
	} catch {
		return false;
	}
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# A run that carries on into fresh histories: the run page follows it',
			'# live through every switch, its events stay contiguous, the runs list',
			'# shows it once at its own created_at, and InspectRun answers during a',
			'# hold. test/tools/generations confirms the switches the page cannot see.',
			'#',
			'# Written by ui/e2e/run-continuation.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${LONG}, ${SHORT} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	// Prerequisites, stated as failures rather than skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18663, 14663]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(ENGINE_LOG, bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18663',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN,
		ORK_HISTORY_MAX_EVENTS: '600',
		// The default (on) decides, never the environment the spec runs in.
		ORK_CONTINUATION_ENABLED: undefined,
		ORK_CONTINUATION_EVENTS: '120',
		ORK_CONTINUATION_BYTES: '1048576',
		ORK_CONTINUATION_RESTORE_TIMEOUT: '4m'
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...s3Env,
		...orkerEnv('run-continuation')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14663',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: longOrkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: shortOrkflow() });
	note(`applied ${LONG} (25 tasks, a 20-item fan-out) and ${SHORT}`);
});

test.afterAll(async () => {
	await killStarted();
});

test('a run that carries on into fresh histories reads as one run, live, to the end', async ({ page }) => {
	test.setTimeout(15 * 60_000);
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: LONG,
		input: { data: { parts: PARTS } }
	});
	const runID = submitted.run.id;
	note(`submitted run ${runID}`);

	// The page is open from the run's first moments, events inspector shown,
	// so every state it reaches arrives on its live stream.
	await page.goto(`${UI_ORIGIN}/runs/${runID}?ns=default&events=1`);
	await expect(page.getByTestId('run-status')).toHaveText(/PENDING|RUNNING/, { timeout: 60_000 });
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE', { timeout: 60_000 });
	expect(generations(runID).executions).toBe(1);
	await shot(page, 'screenshot-before-switch.png');
	note('before the first switch: one execution, the page LIVE');

	// A hold: the engine logged it; the page still says RUNNING and LIVE, and
	// InspectRun answers for the run while it holds.
	await waitFor('the run to hold new dispatch', async () => holdLogged(runID), 8 * 60_000);
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING');
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE');
	const inspected = await rpc<{ runStatus?: string; attempts?: unknown[]; computedAt?: string }>('RunService/InspectRun', {
		runId: runID
	});
	expect(inspected.runStatus).toBe('STATUS_RUNNING');
	expect(inspected.computedAt).toBeTruthy();
	await shot(page, 'screenshot-during-hold.png');
	note(`during a hold: InspectRun answered ${inspected.runStatus} with ${(inspected.attempts ?? []).length} pending attempts; the page RUNNING and LIVE`);

	// Two newer runs, created after the long one, while it is still going.
	const newer: string[] = [];
	for (let i = 0; i < 2; i++) {
		const run = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: SHORT });
		newer.push(run.run.id);
	}
	note(`two newer runs: ${newer.join(', ')}`);

	await waitFor('the run to carry on past its hold', async () => generations(runID).continued >= 2, 8 * 60_000);
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE');
	await shot(page, 'screenshot-after-hold.png');
	note('after the hold: the run carried on; the page still LIVE');

	// The end: SUCCEEDED live, every card SUCCEEDED, no banner, contiguous events.
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 10 * 60_000 });
	await expect(page.getByTestId('task-summary')).toContainText('25/25 succeeded');
	await expect(page.getByTestId('live-stopped')).toHaveCount(0);
	await expect(page.getByTestId('event-log-offline')).toHaveCount(0);
	const lastSeq = Number((await page.getByTestId('event-last-seq').innerText()).replace(/\D/g, ''));
	await expect(page.getByTestId('event-count')).toHaveText(`${lastSeq} events`);
	const seen = generations(runID);
	note(`the end: SUCCEEDED, ${lastSeq} contiguous events; executions ${seen.executions}, continued ${seen.continued}, holds ${seen.holds}`);
	expect(seen.continued).toBeGreaterThanOrEqual(2);
	expect(seen.holds).toBeGreaterThanOrEqual(1);
	await shot(page, 'screenshot-end.png');

	// The runs list: one row for the run, at GetRun's created_at, after the
	// two newer runs in created_at order — a chain's later executions started
	// after them, and only its own created_at places it.
	const got = await rpc<{ run: { createdAt: string } }>('RunService/GetRun', { runId: runID });
	await waitFor('the newer runs to finish', async () => {
		for (const id of newer) {
			const r = await rpc<{ run: { status?: string } }>('RunService/GetRun', { runId: id });
			if (r.run.status !== 'STATUS_SUCCEEDED') return false;
		}
		return true;
	});
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.locator(`tr[data-testid="run-row-${runID}"]`)).toHaveCount(1, { timeout: 60_000 });
	const order = await page
		.locator('tr[data-testid^="run-row-"]')
		.evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')?.replace('run-row-', '')));
	const ours = [newer[1], newer[0], runID].map((id) => order.indexOf(id));
	expect(ours.every((i) => i >= 0)).toBe(true);
	expect(ours).toEqual([...ours].sort((a, b) => a - b));
	const listed = await rpc<{ runs: { id: string; createdAt: string }[] }>('RunService/ListRuns', { pageSize: 200 });
	const rows = listed.runs.filter((r) => r.id === runID);
	expect(rows.length).toBe(1);
	expect(rows[0].createdAt).toBe(got.run.createdAt);
	note(`runs list: one row for ${runID} at created_at ${got.run.createdAt}, after ${newer[1]} and ${newer[0]}`);
});
