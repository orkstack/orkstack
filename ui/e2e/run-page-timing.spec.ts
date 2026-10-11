import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	containerHost,
	ensureActivityImages,
	killStarted,
	noteTo,
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

// Task timing on the run detail page, in a browser, against the real stack:
// was this task slow, or was it waiting?
//
//   P1  while a task has not finished its card reads `Since scheduled …`, and
//       the value grows;
//   P2  once it has, the card's bar splits its time into WAIT (scheduled → the
//       Orker's start) and RUN (that start → finished); a task submitted while
//       no Orker polled waits at least the stimulus, and the wait segment is
//       drawn wider than the run segment;
//   P3  the attempt timeline lists every phase the attempt passed through;
//   P4  the card's CLI equivalent is `orkctl run timing <run> --task <task>`,
//       and the CLI's own output for the same run is recorded beside the
//       screenshots so the two can be compared by eye;
//   P5  a task that failed once and succeeded on attempt 2 shows two phase
//       lines and how long it waited for an Orker.
//
// Topology: one engine, the production UI build, and the Orker started AFTER
// the first submission, so the wait for an Orker is real. Ports and a Temporal
// namespace of this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18293';
const ENGINE_WS = 'ws://127.0.0.1:18293';
const UI_ORIGIN = 'http://127.0.0.1:14293';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-timing';
const STAMP = Date.now().toString(36);
const WAIT_ORKFLOW = `run-page-timing-${STAMP}`;
const FLAKY_ORKFLOW = `run-page-timing-flaky-${STAMP}`;

/** The stimulus: how long the first run waits with no Orker polling. */
const STIMULUS_MS = 5_000;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page-timing')
	: path.join(uiDir, 'test-results', 'run-page-timing');
const TRANSCRIPT = 'run-page-timing-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

let probeServer: Server | undefined;
let probeRequests = 0;
let probeUrl = '';
let orkctl = '';
/**
 * Whether THIS worker has started the Orker. The first test starts it after
 * its stimulus; the second needs one polling and starts it itself when the
 * first did not get that far (a failed test restarts the worker, and
 * beforeAll with it), so one failure is reported once, not twice.
 */
let orkerStarted = false;

function startOrker() {
	if (orkerStarted) return;
	orkerStarted = true;
	spawnLogged(outPath('orker.log'), './bin/orker', [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		...orkerEnv('run-page-timing')
	});
	note('orker started');
}

function waitOrkflow(): string {
	return `name: ${WAIT_ORKFLOW}
version: "0.1.0"
description: Task timing — submitted while no Orker polls, so it waits.

tasks:
  - id: first
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo first"]
`;
}

function flakyOrkflow(): string {
	return `name: ${FLAKY_ORKFLOW}
version: "0.1.0"
description: Task timing — a task that fails once and succeeds on attempt 2.

tasks:
  - id: flaky
    uses: http
    timeout_seconds: 120
    retry:
      maximum_attempts: 2
      initial_interval_seconds: 1
    with:
      op: get
      url: "${probeUrl}"
`;
}

/** runTimingCLI records what `orkctl run timing` prints for the same run. */
function runTimingCLI(runId: string, task: string): string {
	const args = ['--server', ENGINE, '--namespace', 'default', 'run', 'timing', runId, '--task', task];
	const out = execFileSync(orkctl, args, { cwd: repoRoot, encoding: 'utf8' });
	note(`$ orkctl ${args.join(' ')}\n${out}`);
	return out;
}

function msOf(value: string | null): number {
	// Number(null) is 0: an absent data-ms (an UNAVAILABLE segment) must fail
	// as "not measured", never read as a measured zero.
	const parsed = value === null || value === '' ? NaN : Number(value);
	if (!Number.isFinite(parsed)) throw new Error(`not a measured duration: ${value}`);
	return parsed;
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	// Appended, not overwritten: a failed test restarts the worker and this
	// hook with it, and the transcript must keep what the failed test noted.
	appendFileSync(
		outPath(TRANSCRIPT),
		[
			'# Task timing on the run detail page: since-scheduled while running,',
			'# the wait/run bar once finished, every attempt\'s phases, and the',
			'# `orkctl run timing` output for the same runs.',
			'#',
			'# Written by ui/e2e/run-page-timing.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${WAIT_ORKFLOW}, ${FLAKY_ORKFLOW} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18293, 14293]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// The retry stub: the first request loses its connection, every later one
	// is answered — deterministic, no clock.
	probeServer = createServer((req, res) => {
		probeRequests += 1;
		if (probeRequests === 1) {
			req.socket.destroy();
			return;
		}
		res.writeHead(200, { 'content-type': 'application/json' });
		res.end(JSON.stringify({ served: probeRequests }));
	});
	await new Promise<void>((resolve) => probeServer!.listen(0, '0.0.0.0', resolve));
	const probeAddr = probeServer.address();
	if (!probeAddr || typeof probeAddr === 'string') throw new Error('stub server has no port');
	probeUrl = `http://${containerHost}:${probeAddr.port}/timing-probe`;

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orkctl', './cmd/orkctl'], { cwd: repoRoot });
	orkctl = path.join(repoRoot, 'bin', 'orkctl');
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	spawnLogged(outPath('engine.log'), './bin/ork', [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18293',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	await waitForEngine(ENGINE);
	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14293', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	await rpc('OrkflowService/ApplyOrkflow', { yaml: waitOrkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: flakyOrkflow() });
	note(`applied ${WAIT_ORKFLOW} and ${FLAKY_ORKFLOW}; no Orker is running yet`);
});

test.afterAll(async () => {
	await killStarted();
	await new Promise<void>((resolve) => (probeServer ? probeServer.close(() => resolve()) : resolve()));
});

test('a task that waited for an Orker shows since-scheduled, then its wait/run split and its phases', async ({ page }) => {
	// Nobody polls the pool yet, so the submission has to say it knows.
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: WAIT_ORKFLOW,
		allowEmptyPool: true
	});
	const run = submitted.run.id;
	note(`submitted run ${run} with no Orker polling`);

	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);

	// P1 — while it waits, the only honest number is how long since it was
	// scheduled, and it grows.
	const elapsed = page.getByTestId('task-first-elapsed');
	await expect(elapsed).toHaveText(/^Since scheduled /, { timeout: 60_000 });
	const firstRead = await elapsed.innerText();
	await expect.poll(() => elapsed.innerText(), { timeout: 10_000 }).not.toBe(firstRead);
	const secondRead = await elapsed.innerText();
	note(`P1 ${firstRead} -> ${secondRead}`);
	await shot(page, 'since-scheduled.png');

	// The stimulus, not synchronization: this wait IS what is measured.
	await new Promise((resolve) => setTimeout(resolve, STIMULUS_MS));
	startOrker();
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });

	// P2 — the bar: wait at least the stimulus, run shorter, drawn wider.
	await expect(page.getByTestId('task-first-timing')).toBeVisible({ timeout: 30_000 });
	const waitMs = msOf(await page.getByTestId('task-first-wait').getAttribute('data-ms'));
	const runMs = msOf(await page.getByTestId('task-first-run').getAttribute('data-ms'));
	expect(waitMs).toBeGreaterThanOrEqual(STIMULUS_MS);
	expect(runMs).toBeLessThan(waitMs);
	const waitBox = await page.getByTestId('task-first-wait-segment').boundingBox();
	const runBox = await page.getByTestId('task-first-run-segment').boundingBox();
	expect(waitBox && runBox && waitBox.width > runBox.width).toBe(true);
	note(
		`P2 ${await page.getByTestId('task-first-wait').innerText()} (${waitMs}ms) · ` +
			`${await page.getByTestId('task-first-run').innerText()} (${runMs}ms); ` +
			`segments ${waitBox?.width.toFixed(1)}px > ${runBox?.width.toFixed(1)}px`
	);
	await shot(page, 'timing-bar.png');

	// P3 — every phase of attempt 1, the Orker's own record.
	await page.getByTestId('task-inspect-toggle-first').click();
	const phases = page.getByTestId('attempt-first-1-phases');
	await expect(phases).toBeVisible({ timeout: 30_000 });
	const phaseText = await phases.innerText();
	for (const name of ['image', 'run', 'commit']) expect(phaseText).toContain(`${name} `);
	// Decided on attempt 1: the Wait segment already IS the wait for an Orker.
	await expect(page.getByTestId('attempt-first-orker-wait')).toHaveCount(0);
	note(`P3 attempt 1 phases: ${phaseText}`);
	await shot(page, 'timing-phases.png');

	// P4 — the CLI that prints the same thing, and what it printed.
	await page.getByTestId('run-task-first-timing-cli-trigger').click();
	await expect(page.getByTestId('run-task-first-timing-cli-command')).toHaveText(
		`orkctl --namespace 'default' 'run' 'timing' '${run}' '--task' 'first'`
	);
	note(`P4 card CLI: ${await page.getByTestId('run-task-first-timing-cli-command').innerText()}`);
	const cli = runTimingCLI(run, 'first');
	expect(cli).toContain('first');
	expect(cli).toContain('attempt 1  SUCCEEDED');
});

test('a task that failed once shows both attempts\' phases and how long it waited for an Orker', async ({ page }) => {
	startOrker();
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: FLAKY_ORKFLOW
	});
	const run = submitted.run.id;
	note(`submitted run ${run} (flaky)`);

	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	await expect(page.getByTestId('task-flaky-timing')).toBeVisible({ timeout: 30_000 });

	// P5 — two phase lines, attempt 1 ending in `exit` and attempt 2 in
	// `commit`, and the wait for an Orker stated off attempt 1's own record.
	await page.getByTestId('task-inspect-toggle-flaky').click();
	const first = page.getByTestId('attempt-flaky-1-phases');
	const second = page.getByTestId('attempt-flaky-2-phases');
	await expect(first).toBeVisible({ timeout: 30_000 });
	await expect(second).toBeVisible();
	expect(await first.innerText()).toMatch(/exit \S+$/);
	expect(await second.innerText()).toMatch(/commit \S+$/);
	const queueWait = page.getByTestId('attempt-flaky-orker-wait');
	await expect(queueWait).toHaveText(/^Waited for an Orker \S+ on pool \S+$/);
	note(`P5 ${await queueWait.innerText()}`);
	note(`P5 attempt 1 phases: ${await first.innerText()}`);
	note(`P5 attempt 2 phases: ${await second.innerText()}`);
	await shot(page, 'timing-retried.png');
	runTimingCLI(run, 'flaky');

	// F-ui-perf-1: the page renders from GetRun alone; the attempt timeline is
	// read afterwards and fills in when it lands. Held back for two seconds,
	// the run, its status and the card are on screen while it is pending.
	let seedAsked = 0;
	await page.route('**/orkstack.v1.RunService/ListTaskAttempts', async (route) => {
		const body = JSON.parse(route.request().postData() ?? '{}');
		if (!body.taskId) {
			seedAsked++;
			await new Promise((resolve) => setTimeout(resolve, 2_000));
		}
		await route.continue();
	});
	// The browser's own GetRun reads: the first loader answer is the server's,
	// so one here is the re-read the socket's replayed RUN_SUCCEEDED starts.
	let rereads = 0;
	page.on('request', (request) => {
		if (request.url().endsWith('/orkstack.v1.RunService/GetRun')) rereads++;
	});
	await page.reload();
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED');
	await expect(page.getByTestId('task-flaky-timing')).toBeVisible();
	// The collapsed card's retry badge is the seed's: absent while it is pending.
	await expect(page.getByTestId('task-flaky-retries')).toHaveCount(0);
	await shot(page, 'timing-before-seed.png');
	await expect(page.getByTestId('task-flaky-retries')).toBeVisible({ timeout: 30_000 });
	// Once, though the socket's replayed RUN_SUCCEEDED re-reads the run: a
	// finished run's timeline is final, so that later loader answer keeps it.
	await expect.poll(() => rereads, { timeout: 30_000 }).toBeGreaterThan(0);
	await page.waitForLoadState('networkidle');
	expect(seedAsked).toBe(1);
	note(`P5 one seed read across ${rereads + 1} loader answers (the server's and ${rereads} re-read)`);
	await page.getByTestId('task-inspect-toggle-flaky').click();
	await expect(page.getByTestId('task-flaky-attempts')).toBeVisible();
	await expect(page.getByTestId('attempt-flaky-2-phases')).toBeVisible({ timeout: 30_000 });
	await shot(page, 'timing-after-seed.png');
	note('P5 the run rendered with its attempt timeline pending (held 2 s); the timeline filled in when it arrived');
	await page.unroute('**/orkstack.v1.RunService/ListTaskAttempts');
});
