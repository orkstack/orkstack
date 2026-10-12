import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import path from 'node:path';
import {
	activityRef,
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
	waitFor,
	waitForEngine,
	bin,
	temporalAddress,
	temporalReachable
} from './harness';

// The run detail page, in a browser, against the real stack.
//
// What is measured here:
//
//   1. task logs are SCOPED: one pane per task, one attempt at a time, with a
//      selector on a task that retried, and a container application's own two
//      streams told apart on screen;
//   2. the event stream is an INSPECTION VIEW: the page loads with no event
//      list in the DOM, `?events=1` opens one, and closing it does not cost a
//      single live task update — one socket keeps feeding the reducer and the
//      capped buffer whatever the page is rendering;
//   3. the inspector is legible: aligned columns, a chip per kind, and
//      failures distinct twice;
//   4. every live behavior realtime.spec.ts proves still holds on this page.
//
// Topology: one engine, one Orker, the production UI build. Isolation per
// AGENTS.md §11.1 — a Temporal namespace of this spec's own, so a parallel
// agent cannot steal these activities, and stamped orkflow names so two runs
// of this spec cannot collide in the default ork namespace.

// Ports of this spec's own, so the whole e2e directory can run in one go.
const ENGINE = 'http://127.0.0.1:18084';
const ENGINE_WS = 'ws://127.0.0.1:18084';
const UI_ORIGIN = 'http://127.0.0.1:14176';

const TEMPORAL_NAMESPACE =
	process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-page';
// The image every container task here runs. It is activity-shell rather than a
// bare `alpine:3.20`, because an activity must write
// /orkstack/output/values.json and a stock base image does not — an image that
// exits 0 without one fails VALUES_MISSING. It is still an ordinary OCI image,
// built by `docker build` and pulled by digest.
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const LIVE_ORKFLOW = `run-page-live-${STAMP}`;
const EVENTS_ORKFLOW = `run-page-events-${STAMP}`;
const LIVE_FAILURE_ORKFLOW = `run-page-live-failure-${STAMP}`;
// The stub the retried task calls. It destroys the FIRST connection it is
// given and answers every one after it, which activity-http reports as a
// request it could not perform — a failed activity, and therefore a second
// attempt.
//
// The task that calls it is a CONTAINER, so the URL it is given must be
// reachable from inside one: `127.0.0.1` there is the container's own
// loopback, not this process's. The stub therefore listens on all interfaces
// and is addressed through the daemon's host alias.
let probeServer: Server | undefined;
let probeUrl = '';
let probeRequests = 0;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page')
	: path.join(uiDir, 'test-results', 'run-page');
const TRANSCRIPT = 'run-page-session.txt';

// The container application's line budget: one stdout and one stderr line a
// second, for long enough that a browser can be watching while it writes —
// through the first test AND into the second, which asserts on a live pane.
const CONTAINER_LINES = 30;

// These tests share one live run on purpose — the composition is the claim,
// and a fresh run per test would triple the wall clock for nothing.
test.describe.configure({ mode: 'serial' });

let liveRun = '';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

const submit = (orkflowName: string) =>
	rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName });

/**
 * liveOrkflow: a container that talks on BOTH file descriptors for half a
 * minute, and a task that fails its first try and succeeds its second.
 *
 * The container is the point of claim 1's third clause: those bytes
 * are the application's own, and they have to be on screen while the run is
 * still RUNNING. There is nothing else in the stream: every task IS a
 * container, and the Orker narrates nothing of its own into a task's live log.
 *
 * `talk` runs through `uses: shell` rather than naming a bare image like
 * `alpine:3.20` directly. That is the contract, not a convenience: an activity
 * must write /orkstack/output/values.json, and an image that exits 0 without
 * one is a ContractViolation (VALUES_MISSING) — measured, by running it.
 * activity-shell IS an ordinary OCI image, really pulled by digest and really
 * run, so "a real docker container wrote these bytes" is still exactly what is
 * proven here; it is simply an image that holds up its end of the ABI.
 *
 * `flaky` is the point of the attempt selector — a Temporal activity retry is
 * invisible in a task's status, so the only way to know a task has two
 * attempts is to be told, and the only way to read the first one is to ask.
 *
 * `flaky` is an http task and not a shell one, and that is not a taste. No
 * exit code is magic (AGENTS.md §3.2) and a non-zero exit is an ordinary
 * retryable ActivityFailed, so a shell task COULD retry — but it would retry
 * on a timer, and the stub below produces its failure deterministically by
 * destroying the first connection: no clock, no sleep, no race. activity-http
 * performs the request and fails the activity only when it could not be
 * performed at all, which is exactly the destroyed connection and exactly not
 * the 200 that follows it.
 */
function liveOrkflow(probeUrl: string): string {
	return `name: ${LIVE_ORKFLOW}
version: "0.1.0"
description: Live run page — a chatty container and a task that retries.

tasks:
  - id: talk
    uses: shell
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le ${CONTAINER_LINES} ]; do
          echo "container stdout line $i"
          echo "container stderr line $i" 1>&2
          i=$((i+1))
          sleep 1
        done
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

/**
 * eventsOrkflow: a task that takes its time, and then one that fails.
 *
 * Both halves are load-bearing. `slow` finishes while the inspector is OPEN
 * and `boom` fails while it is CLOSED, which is the regression the split
 * comment on the page's socket guards: a page that subscribed with the
 * inspector would go deaf the moment a reader closed it.
 */
function eventsOrkflow(): string {
	return `name: ${EVENTS_ORKFLOW}
version: "0.1.0"
description: Event inspector — a slow task, then a failing one.

tasks:
  - id: slow
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le 10 ]; do
          echo "slow step $i"
          i=$((i+1))
          sleep 1
        done
  - id: boom
    uses: shell
    needs: [slow]
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo \\"about to fail\\"; exit 3"]
`;
}

/**
 * A focused live-watch regression: measured exit 0, then measured exit 3.
 *
 * `never` is the third case, and the only one that never runs: the run fails
 * on `boom`, so `never` is resolved without ever reaching an Orker. The engine
 * records STATUS_CANCELED for it — correct, that IS what ended it — with no
 * started_at, and that absence is what the page must render as "never
 * started" rather than as a CANCELED badge indistinguishable from a task that
 * was killed mid-run.
 */
function liveFailureOrkflow(): string {
	return `name: ${LIVE_FAILURE_ORKFLOW}
version: "0.1.0"
description: Live terminal attempt facts.

tasks:
  - id: ok
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "exit 0"]
  - id: boom
    uses: shell
    needs: [ok]
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "sleep 5; exit 3"]
  - id: never
    uses: shell
    needs: [boom]
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo unreachable"]
`;
}

/** paneLines reads one pane's rendered lines as (stream, text) pairs. */
async function paneLines(
	page: Page,
	taskId: string
): Promise<{ stream: string; text: string }[]> {
	return page
		.getByTestId(`task-log-${taskId}`)
		.getByTestId('task-log-line')
		.evaluateAll((nodes) =>
			nodes.map((node) => ({
				stream: node.getAttribute('data-stream') ?? '',
				text: (node.textContent ?? '').trim()
			}))
		);
}

test('per-task Inspect reveals that task alone, and Show events opens the event list above the tasks', async ({
	page
}) => {
	const submitted = await submit(LIVE_FAILURE_ORKFLOW);
	const run = submitted.run.id;
	const sockets: string[] = [];
	page.on('websocket', (ws) => sockets.push(ws.url()));
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 120_000 });
	await expect(page.getByTestId('task-boom-error')).toBeVisible();

	// THE OVERVIEW. There is no page-level inspector of any kind any more —
	// not the old bottom section, not a status trail, not an event list — and
	// no task's debug detail is on screen until that task is asked.
	await expect(page.getByTestId('inspect-toggle')).toHaveCount(0);
	await expect(page.getByTestId('run-inspector')).toHaveCount(0);
	await expect(page.getByTestId('run-events')).toHaveCount(0);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('task-ok-inspect')).toHaveCount(0);
	await expect(page.getByTestId('task-boom-inspect')).toHaveCount(0);
	await expect(page.getByTestId('task-ok-image')).toHaveCount(0);
	await expect(page.getByTestId('task-ok-exit-code')).toHaveCount(0);
	// Recorded artifacts are DEBUG DETAIL, not overview. The always-visible
	// card carries status, id, elapsed and "Show output" — a task that
	// produced VALUES/STDOUT/STDERR does not push a download list into it.
	await expect(page.getByTestId('task-artifacts-ok')).toHaveCount(0);
	await expect(page.getByTestId('task-artifacts-boom')).toHaveCount(0);
	await expect(page.getByTestId('task-ok')).not.toContainText('Artifacts');
	// The one page-level control left is the event list's, and it says so.
	await expect(page.getByTestId('events-toggle')).toBeVisible();
	await expect(page.getByTestId('events-toggle')).toHaveText('Show events');
	await shot(page, 'screenshot-run-overview.png');

	await page.getByTestId('task-log-toggle-boom').click();
	await expect(page.getByTestId('task-log-state-boom')).toHaveText('ENDED');
	const socketCount = sockets.length;

	// PER TASK, IN THAT TASK'S OWN CARD. `ok`'s Inspect reveals what `ok` ran
	// and what `ok` produced, inside `ok`'s card — the same disclosure shape
	// as its "Show output", not a separate section elsewhere on the page.
	await page.getByTestId('task-inspect-toggle-ok').click();
	await expect(page.getByTestId('task-ok').getByTestId('task-ok-inspect')).toBeVisible();
	await expect(page.getByTestId('task-ok-image')).toContainText('@sha256:');
	await expect(page.getByTestId('task-ok-exit-code')).toHaveText('0');
	await expect(page.getByTestId('task-ok-commit')).toContainText('commit.json');
	const artifactsOk = page.getByTestId('task-ok').getByTestId('task-artifacts-ok');
	await expect(artifactsOk).toBeVisible();
	await expect(artifactsOk).toContainText('VALUES');
	await expect(artifactsOk).toContainText('STDOUT');
	await expect(artifactsOk).toContainText('STDERR');
	await expect(artifactsOk).toContainText('Metadata');
	// SCOPED: one task's disclosure is one task's. `boom`'s stayed shut.
	await expect(page.getByTestId('task-boom-inspect')).toHaveCount(0);
	await expect(page.getByTestId('task-boom-exit-code')).toHaveCount(0);
	// And it is LOCAL state, not URL state — a per-card disclosure, like
	// "Show output", is not something anybody sends to somebody else.
	await expect(page).not.toHaveURL(/inspect=1|events=1/);

	// The sibling opens on its own, and nothing already on screen moves.
	await page.getByTestId('task-inspect-toggle-boom').click();
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3');
	await expect(page.getByTestId('task-log-boom')).toBeVisible();
	await expect(page.getByTestId('task-boom-error')).toBeVisible();
	expect(sockets).toHaveLength(socketCount);

	// INDEPENDENT: closing one leaves the other open.
	await page.getByTestId('task-inspect-toggle-ok').click();
	await expect(page.getByTestId('task-ok-inspect')).toHaveCount(0);
	await expect(page.getByTestId('task-boom-inspect')).toBeVisible();
	await shot(page, 'screenshot-task-inspect.png');

	// SHOW EVENTS is the page-level control, and what it opens renders ABOVE
	// the task list — the run's history reads before the tasks it produced,
	// not after a scroll past all of them.
	await page.getByTestId('events-toggle').click();
	await page.waitForURL(/events=1/);
	await expect(page.getByTestId('run-events')).toBeVisible();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('events-toggle')).toHaveText('Hide events');
	expect(
		await page.evaluate(() => {
			const events = document.querySelector('[data-testid="run-events"]');
			const tasks = document.querySelector('[data-testid="task-list"]');
			if (!events || !tasks) return null;
			return Boolean(events.compareDocumentPosition(tasks) & Node.DOCUMENT_POSITION_FOLLOWING);
		})
	).toBe(true);
	await expect(page.getByTestId('run-inspector')).toHaveCount(0);

	// The event list is the URL's, so history moves it and nothing else.
	await page.goBack();
	await expect(page).not.toHaveURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('task-log-boom')).toBeVisible();
	await page.goForward();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	expect(sockets).toHaveLength(socketCount);

	// A RELOAD tells the two kinds of state apart: `?events=1` is in the
	// address and comes back with it; a per-task disclosure is component
	// state and does not, exactly like "Show output".
	await page.reload();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('task-boom-inspect')).toHaveCount(0);
	await page.getByTestId('events-toggle').click();
	await expect(page).not.toHaveURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('task-boom-error')).toBeVisible();

	await page.setViewportSize({ width: 390, height: 844 });
	await shot(page, 'screenshot-run-overview-mobile.png');
	await page.getByTestId('task-inspect-toggle-ok').click();
	await expect(page.getByTestId('task-ok-image')).toBeVisible();
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
	await shot(page, 'screenshot-task-inspect-mobile.png');
	note(
		'the overview hides image/exit/commit/artifacts and carries no page-level inspector; each ' +
			"task's own Inspect reveals that task's pinned details and recorded artifacts " +
			'(VALUES/STDOUT/STDERR with metadata) inside its own card, independently of its ' +
			'siblings and without touching the URL; "Show events" opens the event list ABOVE the ' +
			'task list as ?events=1, survives back/forward/reload without a second socket, and the ' +
			'per-task disclosures — component state, like "Show output" — do not survive a reload'
	);
});

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The run detail page: scoped task logs, attempts, container',
			'# stdout/stderr, and an event stream shown on request.',
			'#',
			'# Written by ui/e2e/run-page.spec.ts. Every line below was produced by',
			'# the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${LIVE_ORKFLOW}, ${EVENTS_ORKFLOW}, ${LIVE_FAILURE_ORKFLOW} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	// Prerequisites, stated as failures rather than skips: the caller is
	// expected to probe every one of them BEFORE it runs this spec, so an
	// absence never reaches here and a failure here is never an absence
	// (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18084, 14176]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	// The retry stub: hermetic, on a port the OS picks, and deterministic —
	// the first request loses its connection, every one after it is answered.
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
	probeUrl = `http://${containerHost}:${probeAddr.port}/attempt-probe`;

	// The first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);

	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18084',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...s3Env,
		...orkerEnv('run-page')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14176',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// The engine refuses a submission whose pool has nobody serving it, so the
	// Orker has to be polling before anything is started.
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: liveOrkflow(probeUrl) });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: eventsOrkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: liveFailureOrkflow() });
	note(`applied ${LIVE_ORKFLOW}, ${EVENTS_ORKFLOW}, and ${LIVE_FAILURE_ORKFLOW}`);
});

test.afterAll(async () => {
	await killStarted();
	probeServer?.close();
});

// Claims 1 and 2, on the page as it LOADS: task output is what the page is
// for, and the event stream is not on it until it is asked for.
test('the page loads with task panes and no event list, and each task keeps its own', async ({
	page
}) => {
	page.on('pageerror', (err) => note(`BROWSER ERROR: ${err.message}`));
	page.on('console', (msg) => {
		if (msg.type() === 'error') note(`BROWSER CONSOLE ERROR: ${msg.text()}`);
	});

	const submitted = await submit(LIVE_ORKFLOW);
	liveRun = submitted.run.id;
	note(`started run ${liveRun} (${LIVE_ORKFLOW})`);

	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(liveRun);

	// NO EVENT LIST IN THE DOM. Not hidden, not collapsed, not scrolled past:
	// absent.
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('event-log')).toHaveCount(0);
	await expect(page.getByTestId('event-entry')).toHaveCount(0);
	await expect(page.getByTestId('events-toggle')).toBeVisible();
	await expect(page.getByTestId('events-toggle')).toHaveText('Show events');
	// …and each task's own Inspect is present and SHUT: the control is on the
	// card, the detail it discloses is not.
	await expect(page.getByTestId('task-inspect-toggle-talk')).toBeVisible();
	await expect(page.getByTestId('task-talk-inspect')).toHaveCount(0);
	note(
		'the run page loaded with no event list in the DOM, a "Show events" control, and a ' +
			'closed per-task "Inspect" on the task card'
	);
	await shot(page, 'screenshot-run-page-loaded.png');

	// SCOPED: a pane per task, opened one at a time, holding one task's output
	// and nobody else's.
	await expect(page.getByTestId('task-log-talk')).toHaveCount(0);
	await expect(page.getByTestId('task-log-flaky')).toHaveCount(0);

	await page.getByTestId('task-log-toggle-talk').click();
	// Poll for the LINE THIS TEST ASSERTS ON, not for a non-empty pane. Under
	// the OCI Activity Contract a task's stream carries the workload's bytes
	// and nothing else, so a pane is empty until the application writes — but
	// the image still has to be pulled and the workspace built first, so
	// "non-empty" and "the application has spoken" are still different
	// moments, and only the second one is what the scoping assertion below
	// needs. A `length > 0` poll would read a pane whose task has not started
	// talking yet, and flake.
	await expect
		.poll(
			async () =>
				(await paneLines(page, 'talk')).some((l) => l.text.startsWith('container stdout line')),
			{ timeout: 120_000 }
		)
		.toBe(true);
	await expect(page.getByTestId('task-log-flaky')).toHaveCount(0);

	await page.getByTestId('task-log-toggle-flaky').click();
	// The same shape on the sibling: activity-http announces the request it is
	// about to make before it makes it, so this is the first line either way.
	await expect
		.poll(
			async () => (await paneLines(page, 'flaky')).some((l) => l.text.startsWith('http GET ')),
			{ timeout: 120_000 }
		)
		.toBe(true);

	const talkTexts = (await paneLines(page, 'talk')).map((l) => l.text);
	const flakyTexts = (await paneLines(page, 'flaky')).map((l) => l.text);
	// Each pane holds its own task's stream: the lines that task's own
	// workload wrote, and nothing from the other. (`[ork] ` notices are the
	// one exception the Orker still reserves for itself.)
	expect(talkTexts.some((t) => t.startsWith('container stdout line'))).toBe(true);
	expect(talkTexts.some((t) => t.startsWith('http GET'))).toBe(false);
	expect(flakyTexts.some((t) => t.startsWith('http GET '))).toBe(true);
	expect(flakyTexts.some((t) => t.startsWith('container st'))).toBe(false);
	// The scoping, said as a property: neither pane holds a line of the other.
	expect(talkTexts.filter((t) => flakyTexts.includes(t))).toEqual([]);
	note(
		`two panes open at once: talk holds ${talkTexts.length} of its own lines and flaky ` +
			`${flakyTexts.length} of its own, with nothing in common`
	);
});

// Claim 1, the container half: the application's own two streams, live,
// told apart on screen — from the in-band marker the Orker framed them with,
// not from a guess about the text.
test("a container application's stdout and stderr arrive live and render apart", async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await page.getByTestId('task-log-toggle-talk').click();

	// LIVE: lines are on screen while the run itself still reads RUNNING, so
	// nothing here came from an artifact published at the end. The poll carries
	// EVERY condition the block below asserts — six lines in the pane and at
	// least one of each of the application's own two streams — because a bare
	// count cannot tell "the application has written on both descriptors" from
	// "it has written six times on one".
	await expect
		.poll(
			async () => {
				const lines = await paneLines(page, 'talk');
				return {
					six: lines.length >= 6,
					stdout: lines.some((l) => l.text.startsWith('container stdout line')),
					stderr: lines.some((l) => l.text.startsWith('container stderr line'))
				};
			},
			{ timeout: 120_000 }
		)
		.toEqual({ six: true, stdout: true, stderr: true });
	const runStatusWhenLive = (await page.getByTestId('run-status').innerText()).trim();
	const taskStatusWhenLive = (await page.getByTestId('task-talk-status').innerText()).trim();
	expect(runStatusWhenLive).toBe('RUNNING');
	expect(taskStatusWhenLive).toBe('RUNNING');
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('LIVE');

	const live = await paneLines(page, 'talk');
	// The APPLICATION's lines, told apart by the marker the Orker framed them
	// with. They are selected by name rather than by stream so that the
	// stream assertion below is a CHECK and not a tautology.
	const stdout = live.filter((l) => l.text.startsWith('container stdout line'));
	const stderr = live.filter((l) => l.text.startsWith('container stderr line'));
	expect(stdout.length).toBeGreaterThan(0);
	expect(stderr.length).toBeGreaterThan(0);
	expect(stdout.every((l) => l.stream === 'stdout')).toBe(true);
	expect(stderr.every((l) => l.stream === 'stderr')).toBe(true);
	// The marker is the WIRE, not the reading: it is stripped for display, and
	// the CLI is the reader that keeps it verbatim.
	expect(live.some((l) => l.text.includes('[stderr]'))).toBe(false);
	note(
		`container output live at ${runStatusWhenLive}: ${stdout.length} stdout and ` +
			`${stderr.length} stderr lines on screen, no marker rendered`
	);

	// VISUALLY DISTINCT, measured as the browser paints it rather than as a
	// class name: two colours, not one, and the legend that says which is which.
	const colours = await page
		.getByTestId('task-log-talk')
		.getByTestId('task-log-line')
		.evaluateAll((nodes) => {
			const seen: Record<string, string> = {};
			for (const node of nodes) {
				const stream = node.getAttribute('data-stream') ?? '';
				if (!seen[stream]) seen[stream] = getComputedStyle(node).color;
			}
			return seen;
		});
	expect(colours.stdout).toBeTruthy();
	expect(colours.stderr).toBeTruthy();
	expect(colours.stderr).not.toBe(colours.stdout);
	await expect(page.getByTestId('task-log-stderr-legend-talk')).toBeVisible();
	note(`stdout renders ${colours.stdout} and stderr ${colours.stderr}; legend shown`);
	await shot(page, 'screenshot-container-streams-live.png');

	// And the pane still ends where a pane ends: the stream terminates and the
	// whole output is dense, in the engine's order.
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('ENDED', { timeout: 180_000 });
	const all = await paneLines(page, 'talk');
	expect(all.filter((l) => l.text.startsWith('container stdout line'))).toHaveLength(
		CONTAINER_LINES
	);
	expect(all.filter((l) => l.text.startsWith('container stderr line'))).toHaveLength(
		CONTAINER_LINES
	);
	note(`talk ended with ${all.length} lines: ${CONTAINER_LINES} per stream, none lost`);
});

// Claim 1, the attempt half: a task that retried offers its tries, and
// picking one shows THAT try's output — the one that failed included.
test('a retried task offers its attempts, and attempt 1 still reads back', async ({ page }) => {
	await waitFor(
		'the flaky task succeeds on its second attempt',
		async () => {
			const answer = await rpc<{ run?: { taskRuns?: { taskId: string; status: string }[] } }>(
				'RunService/GetRun',
				{ runId: liveRun }
			);
			return (answer.run?.taskRuns ?? []).some(
				(t) => t.taskId === 'flaky' && t.status === 'STATUS_SUCCEEDED'
			);
		},
		180_000
	);

	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await page.getByTestId('task-log-toggle-flaky').click();

	// The selector appears because the task retried, and it offers `latest`
	// plus every try by number.
	const selector = page.getByTestId('task-log-attempt-select-flaky');
	const attempts = page.getByTestId('task-log-attempt-select-flaky-listbox');
	await expect(selector).toBeVisible({ timeout: 60_000 });
	// The tries are read from the open panel: this is the app's own Select, so
	// its options exist while it is open and nowhere else. Escape closes it
	// again without choosing anything — the same guarantee a reader gets.
	await selector.click();
	expect(await attempts.getByRole('option').allInnerTexts()).toEqual(['latest', '1', '2']);
	await page.keyboard.press('Escape');
	await expect(attempts).toHaveCount(0);

	// `latest` is the default and shows the try that ran last: the one that
	// reached the stub and got an answer. Two requests, one completion.
	await expect
		.poll(
			async () =>
				(await paneLines(page, 'flaky')).map((l) => l.text).filter((t) => t.startsWith('http ')),
			{ timeout: 60_000 }
		)
		.toEqual([`http GET ${probeUrl}`, expect.stringContaining('-> 200')]);
	// WHICH try this pane is showing is asserted from the page's own attempt
	// label rather than sniffed out of the log text: the Orker narrates NOTHING
	// into a task's live log — the stream carries the workload's own bytes and
	// nothing else (§3.6) — so the element whose job this is holds the evidence.
	await expect(page.getByTestId('task-log-attempt-flaky')).toHaveText('attempt 2');
	await shot(page, 'screenshot-attempt-latest.png');

	// Attempt 1 — the try that FAILED — is still in the store and reads back.
	// The same task, the same pane, a different attempt: the request that
	// never got an answer, with no completion behind it.
	await selector.click();
	await page.getByTestId('task-log-attempt-select-flaky-option-1').click();
	await expect
		.poll(
			async () =>
				(await paneLines(page, 'flaky')).map((l) => l.text).filter((t) => t.startsWith('http ')),
			{ timeout: 60_000 }
		)
		.toEqual([`http GET ${probeUrl}`]);
	const firstTexts = (await paneLines(page, 'flaky')).map((l) => l.text);
	expect(firstTexts.some((t) => t.includes('-> 200'))).toBe(false);
	await expect(page.getByTestId('task-log-attempt-flaky')).toHaveText('attempt 1');
	note(
		'attempt selector on task flaky: latest shows the try that answered, "1" shows the ' +
			'try whose connection died — the request, and no completion behind it'
	);
	await shot(page, 'screenshot-attempt-one.png');

	// And back: the selector is a view, not a one-way door.
	await selector.click();
	await page.getByTestId('task-log-attempt-select-flaky-option-0').click();
	await expect
		.poll(async () => (await paneLines(page, 'flaky')).some((l) => l.text.includes('-> 200')), {
			timeout: 60_000
		})
		.toBe(true);
});

// The retry, said IN THE STATUS AREA rather than only in the log pane.
//
// This is the claim the comment on `flaky` above used to end on: a Temporal
// retry is invisible in a task's status, so a task that failed once and passed
// on its second try renders exactly like one that passed immediately. The
// attempt records the Orker files per attempt are what closes that, and here
// they are read back through the page: a badge on the card, a count in the run
// header, and a timeline that carries the ERROR of the try that failed — the
// one thing Temporal itself discards when it schedules the next attempt.
test('a retried task shows its retry on the card, in the header, and as a timeline', async ({
	page
}) => {
	await waitFor(
		'the flaky task succeeds on its second attempt',
		async () => {
			const answer = await rpc<{ run?: { taskRuns?: { taskId: string; status: string }[] } }>(
				'RunService/GetRun',
				{ runId: liveRun }
			);
			return (answer.run?.taskRuns ?? []).some(
				(t) => t.taskId === 'flaky' && t.status === 'STATUS_SUCCEEDED'
			);
		},
		180_000
	);

	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(liveRun);

	// THE CARD. `flaky` succeeded — its pill says so — and the badge beside it
	// says that took two goes. `talk` succeeded first time and says nothing:
	// the badge exists to mark the exception, not to decorate every task.
	await expect(page.getByTestId('task-flaky-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	await expect(page.getByTestId('task-flaky-retries')).toHaveText('retried ×1', {
		timeout: 60_000
	});
	await expect(page.getByTestId('task-talk-retries')).toHaveCount(0);

	// THE HEADER. One retry in this run, counted from the records rather than
	// from TaskRun.attempt (which is 0 for a Temporal timeout and the
	// committing attempt for an adopted commit).
	await expect(page.getByTestId('run-retries')).toHaveText(/Retries:\s*1/);

	// THE TIMELINE, inside the task's own Inspect — the same disclosure the
	// image, exit code and artifacts live behind.
	await page.getByTestId('task-inspect-toggle-flaky').click();
	const timeline = page.getByTestId('task-flaky-attempts');
	await expect(timeline).toBeVisible();
	await expect(timeline).toContainText('Attempts (2)');

	// Attempt 1 is the try whose connection the stub destroyed: FAILED, with
	// the error that reached nothing before these records existed.
	const first = page.getByTestId('task-flaky-attempt-1');
	await expect(first).toHaveAttribute('data-outcome', 'FAILED');
	await expect(page.getByTestId('task-flaky-attempt-1-outcome')).toHaveText('FAILED');
	// The error is disclosed, not shouted: a timeline row is a summary until
	// the reader asks for the failure behind it.
	await expect(page.getByTestId('task-flaky-attempt-1-error')).toHaveCount(0);
	await page.getByTestId('task-flaky-attempt-1-error-toggle').click();
	const failure = page.getByTestId('task-flaky-attempt-1-error');
	await expect(failure).toBeVisible();
	// The whole Error, as the rest of the page renders one: a §8.2 reason code
	// and the Orker's own sentence, not a flattened string.
	await expect(page.getByTestId('task-flaky-attempt-1-error-code')).not.toBeEmpty();
	const attemptOneMessage = (
		await page.getByTestId('task-flaky-attempt-1-error-message').innerText()
	).trim();
	expect(attemptOneMessage.length).toBeGreaterThan(0);

	// Attempt 2 is the try that got its 200. It has no error to disclose.
	const second = page.getByTestId('task-flaky-attempt-2');
	await expect(second).toHaveAttribute('data-outcome', 'SUCCEEDED');
	await expect(page.getByTestId('task-flaky-attempt-2-error-toggle')).toHaveCount(0);
	note(
		`attempt timeline on flaky: #1 FAILED (${attemptOneMessage}) and #2 SUCCEEDED, with the ` +
			'failed try\'s error read back from the record its Orker filed'
	);
	await shot(page, 'screenshot-attempt-timeline.png');

	// "View logs" PINS THE PANE to that try. It is the same control the pane's
	// own selector uses — URL state — so what the reader ends up looking at is
	// a link they can send: the request that never got an answer, with no
	// completion behind it.
	await page.getByTestId('task-flaky-attempt-1-logs').click();
	await page.waitForURL(/task=flaky/);
	await expect(page).toHaveURL(/attempt=1/);
	await expect(page.getByTestId('task-log-attempt-flaky')).toHaveText('attempt 1', {
		timeout: 60_000
	});
	await expect
		.poll(
			async () =>
				(await paneLines(page, 'flaky')).map((l) => l.text).filter((t) => t.startsWith('http ')),
			{ timeout: 60_000 }
		)
		.toEqual([`http GET ${probeUrl}`]);
	note('"View logs" on attempt 1 pinned the pane to attempt 1 and put it in the address');
	await shot(page, 'screenshot-attempt-timeline-pinned.png');

	// AND IT SURVIVES A RELOAD: the badge, the header chip and the timeline
	// come from the store through ListTaskAttempts, not from anything the live
	// socket happened to deliver into this tab.
	await page.reload();
	await expect(page.getByTestId('task-flaky-retries')).toHaveText('retried ×1');
	await expect(page.getByTestId('run-retries')).toHaveText(/Retries:\s*1/);
	await page.getByTestId('task-inspect-toggle-flaky').click();
	await expect(page.getByTestId('task-flaky-attempt-1')).toHaveAttribute('data-outcome', 'FAILED');
	await expect(page.getByTestId('task-flaky-attempt-2')).toHaveAttribute(
		'data-outcome',
		'SUCCEEDED'
	);
});

// Claims 2, 3 and 4: the inspector opens on request, closes without taking
// the live updates with it, and is legible when it is open.
test('the event view opens and closes while task state keeps updating', async ({ page }) => {
	const sockets: string[] = [];
	page.on('websocket', (ws) => sockets.push(ws.url()));
	page.on('pageerror', (err) => note(`BROWSER ERROR: ${err.message}`));

	const submitted = await submit(EVENTS_ORKFLOW);
	const run = submitted.run.id;
	note(`started run ${run} (${EVENTS_ORKFLOW})`);

	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(run);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);

	// A TASK THAT HAS NOT STARTED SAYS SO, AND SHOWS NO STATUS.
	//
	// `boom` needs `slow`, so for the first ten seconds of this run it is not
	// queued anywhere and no Orker has seen it. It used to draw a "PENDING"
	// pill, which is a status about something that is not happening. The card
	// now says which dependency it is waiting on, greyed, with no pill — and
	// the SUCCEEDED/FAILED assertions further down are the other half of the
	// proof: the pill appears the moment the task is really dispatched.
	const boomStatus = page.getByTestId('task-boom-status');
	await expect(boomStatus).toHaveAttribute('data-state', 'not-started');
	await expect(boomStatus).toHaveText(/Waiting for slow/);
	await expect(page.getByTestId('task-boom')).toHaveAttribute('data-dispatched', 'false');
	await expect(page.getByTestId('task-boom')).not.toContainText('PENDING');
	await shot(page, 'screenshot-task-waiting.png');
	note('task boom rendered as "Waiting for slow" with no status pill');

	await page.getByTestId('task-inspect-toggle-boom').click();
	// A generated proto3 int32 reads as 0 even before an attempt finishes. The
	// page must not draw that absence as a successful measurement. Read it from
	// boom's OWN card — that is the only place this fact is rendered now.
	const boomExitCode = page.getByTestId('task-boom').getByTestId('task-boom-exit-code');
	await expect(boomExitCode).toHaveText('Not measured');
	await expect(boomExitCode).toHaveAttribute('data-state', 'unknown');

	// A marker that survives only if the page never reloads: the toggle is a
	// same-route navigation, so nothing below may re-enter the loader.
	await page.evaluate(() => {
		(window as unknown as { __orkLiveMarker: string }).__orkLiveMarker = 'alive';
	});

	// OPEN. The state is in the URL, so this view is a link somebody can send.
	await page.getByTestId('events-toggle').click();
	await page.waitForURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('event-entry').first()).toBeVisible({ timeout: 60_000 });

	// THE BACKLOG IS THERE. RUN_CREATED arrived before the inspector existed,
	// and it is on screen: the buffer is filled whether or not it is rendered.
	await expect(
		page.getByTestId('event-entry').filter({ hasText: 'RUN_CREATED' })
	).toHaveCount(1);
	note('opening ?events=1 showed the backlog, RUN_CREATED included');

	// TASK STATE KEEPS UPDATING WHILE IT IS OPEN.
	await expect(page.getByTestId('task-slow-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note('task slow reached SUCCEEDED with the event inspector OPEN');
	await shot(page, 'screenshot-event-inspector.png');

	// LEGIBLE: the toolbar filters the buffer in place, chronology preserved.
	await expect(page.getByTestId('event-count')).toBeVisible();
	await expect(page.getByTestId('event-last-seq')).toContainText('#');
	const allRows = await page.getByTestId('event-entry').count();
	await page.getByTestId('event-filter-run').click();
	// A filter click repaints the list on Svelte's schedule, so wait for the
	// repaint the assertions describe instead of reading whatever the DOM holds
	// one tick after the click. The wait also refuses an EMPTY list, which an
	// `every()` would have called a pass.
	await expect
		.poll(async () => {
			const rows = await page.getByTestId('event-entry').allTextContents();
			return rows.length > 0 && rows.every((row) => row.includes('RUN_'));
		})
		.toBe(true);
	const runRows = await page.getByTestId('event-entry').allTextContents();
	expect(runRows.length).toBeGreaterThan(0);
	expect(runRows.length).toBeLessThan(allRows);
	expect(runRows.every((row) => row.includes('RUN_'))).toBe(true);
	await page.getByTestId('event-filter-tasks').click();
	await expect
		.poll(async () => {
			const rows = await page.getByTestId('event-entry').allTextContents();
			return rows.length > 0 && rows.every((row) => row.includes('TASK_'));
		})
		.toBe(true);
	const taskRows = await page.getByTestId('event-entry').allTextContents();
	expect(taskRows.length).toBeGreaterThan(0);
	expect(taskRows.every((row) => row.includes('TASK_'))).toBe(true);
	await page.getByTestId('event-filter-all').click();

	// CLOSE. The list leaves the DOM; nothing else does.
	await page.getByTestId('events-toggle').click();
	await expect(page).toHaveURL(/ns=default/);
	await expect(page).not.toHaveURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('event-entry')).toHaveCount(0);

	// AND THE LIVE UPDATES SURVIVE IT. This is the regression the split
	// comment on the page's socket guards: `boom` fails, and both the task and
	// the run reach their terminal state with the event list closed.
	await expect(page.getByTestId('task-boom-status')).toHaveText('FAILED', { timeout: 120_000 });
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 60_000 });
	note('task boom reached FAILED and the run FAILED with the event inspector CLOSED');

	// TASK_FAILED itself carries only status/message. RUN_FAILED causes one
	// terminal GetRun re-read (not a poll and not a reload), after which the
	// task card has the Orker's actual attempt facts and structured category.
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3', { timeout: 60_000 });
	await expect(page.getByTestId('task-boom-exit-code')).toHaveAttribute('data-state', 'measured');
	await expect(page.getByTestId('task-boom-error-category')).toHaveText('Activity failed');
	await expect(page.getByTestId('task-boom-error-code')).toHaveText('CONTAINER_EXIT_NONZERO');
	// A genuine successful exit 0 is still a measurement, not "unknown" — read
	// from the succeeded task's own Inspect, opened here for the first time.
	await page.getByTestId('task-inspect-toggle-slow').click();
	await expect(page.getByTestId('task-slow-exit-code')).toHaveText('0');
	await expect(page.getByTestId('task-slow-exit-code')).toHaveAttribute('data-state', 'measured');
	note('live terminal refresh rendered boom exit 3 / Activity failed and slow exit 0 without reload');
	await shot(page, 'screenshot-events-closed-still-live.png');

	// The page never reloaded through any of it, and exactly ONE event socket
	// served the whole session: opening and closing the inspector is a
	// rendering decision, not a subscription.
	const marker = await page.evaluate(
		() => (window as unknown as { __orkLiveMarker?: string }).__orkLiveMarker
	);
	expect(marker).toBe('alive');
	const eventSockets = sockets.filter((u) => u.includes('/ws/runs/') && !u.includes('/tasks/'));
	expect(eventSockets).toHaveLength(1);
	note(`one event socket for the whole open/close session: ${eventSockets[0]}`);

	// REOPEN: the events that arrived while it was closed are there, and the
	// failures are distinct twice — a rose chip and a tinted row.
	await page.getByTestId('events-toggle').click();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(
		page.getByTestId('event-entry').filter({ hasText: 'RUN_FAILED' })
	).toHaveCount(1);
	const failures = page.locator('[data-testid="event-entry"][data-failure="true"]');
	expect(await failures.count()).toBeGreaterThanOrEqual(2); // TASK_FAILED and RUN_FAILED
	const tint = await failures.first().evaluate((el) => getComputedStyle(el).backgroundColor);
	const plain = await page
		.locator('[data-testid="event-entry"][data-failure="false"]')
		.first()
		.evaluate((el) => getComputedStyle(el).backgroundColor);
	expect(tint).not.toBe(plain);
	note(`failure rows tinted ${tint} against ${plain} for the rest`);

	await page.getByTestId('event-filter-failures').click();

	// Same as the chips above: wait for the repaint that is asserted.
	await expect
		.poll(async () => {
			const rows = await page.getByTestId('event-entry').allTextContents();
			return rows.length >= 2 && rows.every((row) => row.includes('FAILED'));
		})
		.toBe(true);
	const onlyFailures = await page.getByTestId('event-entry').allTextContents();
	expect(onlyFailures.length).toBeGreaterThanOrEqual(2);
	expect(onlyFailures.every((row) => row.includes('FAILED'))).toBe(true);
	await shot(page, 'screenshot-event-failures.png');

	// DEEP LINK: the URL is the state, so a fresh load of it opens the view.
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default&events=1`);
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	note('a fresh load of ?events=1 opens the inspector: the URL is the state');
	await page.getByTestId('task-inspect-toggle-boom').click();
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3');
	await expect(page.getByTestId('task-boom-error-category')).toHaveText('Activity failed');
	await page.reload();
	// The per-task disclosure is component state and a reload discards it, so
	// it is asked for again — the FACT behind it is what has to survive.
	await page.getByTestId('task-inspect-toggle-boom').click();
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3');
	await expect(page.getByTestId('task-boom-error-category')).toHaveText('Activity failed');
	note('a reload of the finished run kept exit 3 and the Activity failed category');
});

test('a task watched live acquires its real exit code and Error.category without reload', async ({
	page
}) => {
	const submitted = await submit(LIVE_FAILURE_ORKFLOW);
	const run = submitted.run.id;
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText(/PENDING|RUNNING/);
	await page.getByTestId('task-inspect-toggle-boom').click();
	await page.getByTestId('task-inspect-toggle-ok').click();
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('Not measured');
	await expect(page.getByTestId('task-boom-exit-code')).toHaveAttribute('data-state', 'unknown');

	// A property on window is direct evidence that the terminal snapshot re-read
	// was a SvelteKit invalidation, not a browser reload.
	await page.evaluate(() => {
		(window as unknown as { __orkTerminalMarker: string }).__orkTerminalMarker = 'alive';
	});
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 120_000 });
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3', { timeout: 60_000 });
	await expect(page.getByTestId('task-boom-exit-code')).toHaveAttribute('data-state', 'measured');
	await expect(page.getByTestId('task-boom-error-category')).toHaveText('Activity failed');
	await expect(page.getByTestId('task-boom-error-code')).toHaveText('CONTAINER_EXIT_NONZERO');
	await expect(page.getByTestId('task-ok-exit-code')).toHaveText('0');
	await expect(page.getByTestId('task-ok-exit-code')).toHaveAttribute('data-state', 'measured');
	expect(
		await page.evaluate(
			() => (window as unknown as { __orkTerminalMarker?: string }).__orkTerminalMarker
		)
	).toBe('alive');
	note(`live-watched run ${run}: ok measured 0; boom measured 3 / Activity failed without reload`);
	await shot(page, 'screenshot-live-terminal-facts.png');

	// A TASK THE RUN ENDED BEFORE IT EVER STARTED SHOWS NO STATUS AT ALL.
	//
	// `never` carries STATUS_CANCELED on the wire, deliberately — that is the
	// lifecycle truth. But it never reached an Orker, so it has no started_at,
	// and drawing the same CANCELED badge a killed task gets said something
	// false about it. The card is greyed, says "Never started", and the word
	// CANCELED appears nowhere inside it.
	const neverStatus = page.getByTestId('task-never-status');
	await expect(neverStatus).toHaveAttribute('data-state', 'not-started', { timeout: 60_000 });
	await expect(neverStatus).toHaveText(/Never started/);
	await expect(page.getByTestId('task-never')).toHaveAttribute('data-dispatched', 'false');
	await expect(page.getByTestId('task-never')).not.toContainText('CANCELED');
	await shot(page, 'screenshot-task-never-started.png');
	note('task never: rendered greyed as "Never started", with no CANCELED badge');

	await page.reload();
	// Both disclosures are re-opened: a reload keeps the URL's state and not a
	// card's, and what is being checked here is the reloaded FACTS.
	await page.getByTestId('task-inspect-toggle-boom').click();
	await page.getByTestId('task-inspect-toggle-ok').click();
	await expect(page.getByTestId('task-ok-exit-code')).toHaveText('0');
	await expect(page.getByTestId('task-boom-exit-code')).toHaveText('3');
	await expect(page.getByTestId('task-boom-error-category')).toHaveText('Activity failed');
	// THE SNAPSHOT PATH, not just the live one: a reload re-reads GetRun, and
	// the "never started" rendering has to survive it. That is the read that
	// broke before — the snapshot carried a stamped started_at, so a reloaded
	// page could not tell the two CANCELED cases apart at all.
	await expect(page.getByTestId('task-never')).toHaveAttribute('data-dispatched', 'false');
	await expect(page.getByTestId('task-never-status')).toHaveText(/Never started/);
	await expect(page.getByTestId('task-never')).not.toContainText('CANCELED');
});

// A FAILED task hands the reader the command that runs it on their own
// machine against this run's recorded bytes (`orkctl task run`). The page runs
// nothing: it copies a command that names the namespace, the run and the task,
// the local-tool exception docs/UI_CLI_PARITY.md records. A succeeded or a
// never-started task offers no such command.
test('a failed task offers the command that runs it locally; a succeeded or never-started task does not', async ({
	page
}) => {
	const submitted = await submit(LIVE_FAILURE_ORKFLOW);
	const run = submitted.run.id;
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 120_000 });

	const trigger = page.getByTestId('run-task-boom-replay-cli-trigger');
	await expect(trigger).toBeVisible();
	await trigger.click();
	const command = page.getByTestId('run-task-boom-replay-cli-command');
	await expect(command).toHaveText(
		`orkctl --namespace 'default' 'task' 'run' '--from-run' '${run}' '--task' 'boom'`
	);
	await expect(page.getByTestId('run-task-ok-replay-cli-trigger')).toHaveCount(0);
	await expect(page.getByTestId('task-never')).toHaveAttribute('data-dispatched', 'false');
	await expect(page.getByTestId('run-task-never-replay-cli-trigger')).toHaveCount(0);
	await shot(page, 'screenshot-task-local-replay.png');
	note(`run ${run}: boom (FAILED) offers ${await command.innerText()}; ok (SUCCEEDED) and never (never started) offer none`);
});

// The retry HISTORY on the task card: whether this orkflow's `flaky` task
// retries in its other runs, asked of the archive only when a reader asks.
// The first run retried it; a second run that did not still says the task is
// flaky, with the share of runs that retried.
test("a retried task's badge carries its retry history once asked, and a run that did not retry still says the task is flaky", async ({
	page
}) => {
	// Runs to wait for, archive writes and a cache TTL: more than the
	// default budget, never a sleep.
	test.setTimeout(420_000);
	const HISTORY_KEY = 'orkstack.run.stability';
	// The same question the page asks, so waiting on it also leaves the
	// engine's short-lived cache holding the fresh answer.
	const historyRequest = {
		namespace: 'default',
		orkflowName: LIVE_ORKFLOW,
		lookback: '604800s',
		groupBy: ['FAILURE_GROUP_BY_TASK']
	};
	const scanned = async () =>
		Number(
			(await rpc<{ runsScanned?: string }>('StatsService/GetFlakeIndex', historyRequest)).runsScanned ?? '0'
		);
	const succeeded = async (runId: string) =>
		(await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status === 'STATUS_SUCCEEDED';

	await waitFor('the retried run has succeeded', () => succeeded(liveRun), 180_000);
	await waitFor('the retried run is in the flake index', async () => (await scanned()) === 1, 120_000);

	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await page.evaluate((key) => localStorage.removeItem(key), HISTORY_KEY);
	await page.reload();
	const badge = page.getByTestId('task-flaky-retries');
	await expect(badge).toHaveText('retried ×1', { timeout: 60_000 });
	await expect(page.getByTestId('run-stability-note')).toHaveCount(0);

	await page.getByTestId('run-stability-toggle').click();
	await expect(page.getByTestId('run-stability-note')).toContainText('1 finished run ', { timeout: 60_000 });
	// The in-run label is unchanged; the title gains the history.
	await expect(badge).toHaveText('retried ×1');
	await expect(badge).toHaveAttribute('title', /retried in 1 of 1 runs/);
	note(`retry history on ${liveRun}: badge "${await badge.innerText()}", title "${await badge.getAttribute('title')}"`);
	note(`run-stability-note: ${(await page.getByTestId('run-stability-note').innerText()).trim()}`);
	await page.getByTestId('run-stability-cli-trigger').click();
	const command = (await page.getByTestId('run-stability-cli-command').innerText()).trim();
	expect(command).toContain(`stats flakes --orkflow ${LIVE_ORKFLOW} --since 168h`);
	note(`run-stability CLI equivalent: ${command}`);
	await page.keyboard.press('Escape');

	// A second run of the same orkflow: the stub now answers first time, so
	// `flaky` does not retry in it.
	const second = (await submit(LIVE_ORKFLOW)).run.id;
	note(`started run ${second} (${LIVE_ORKFLOW}), whose flaky task does not retry`);
	await waitFor('the second run has succeeded', () => succeeded(second), 240_000);
	await waitFor('both runs are in the flake index', async () => (await scanned()) === 2, 120_000);

	// Asked for nothing, the page shows nothing: this run did not retry.
	await page.evaluate((key) => localStorage.setItem(key, 'closed'), HISTORY_KEY);
	await page.goto(`${UI_ORIGIN}/runs/${second}?ns=default`);
	await expect(page.getByTestId('task-flaky-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	await expect(page.getByTestId('task-flaky-retries')).toHaveCount(0);

	await page.getByTestId('run-stability-toggle').click();
	const flaky = page.getByTestId('task-flaky-retries');
	await expect(flaky).toHaveText('flaky 50%', { timeout: 60_000 });
	await expect(flaky).toHaveAttribute('data-flaky', 'true');
	await expect(flaky).toHaveAttribute('title', /retried in 1 of 2 runs/);
	await expect(page.getByTestId('task-talk-retries')).toHaveCount(0);
	note(`run ${second}: task-flaky-retries reads "${await flaky.innerText()}" (data-flaky=true), task-talk-retries absent`);
	await shot(page, 'screenshot-run-page-flake-history.png');

	// The open state is remembered: the next navigation applies it unasked.
	await page.reload();
	await expect(page.getByTestId('task-flaky-retries')).toHaveText('flaky 50%', { timeout: 60_000 });
	note('after a reload the remembered open state shows "flaky 50%" without a click');
});
