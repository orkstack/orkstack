import { expect, test, type Page } from '@playwright/test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, createWriteStream, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { catalogEnv, ensureActivityImages, orkerEnv } from './harness';
import { selectNamespace } from './uiHelpers';

// The whole logs-dashboard story, in one browser session against the real
// stack.
//
// The claim is a COMPOSITION, which is why the per-feature specs are not
// enough for it: two ork namespaces, two Orker
// pools, an orkflow edited and promoted, a task's output followed live, and a
// dashboard — all read through a replica that started nothing.
//
// The topology is the point:
//
//   engine A (:18082)  every write that starts a run. Nothing reads it.
//   engine B (:18083)  every read the browser makes, and the log socket. It
//                      accepted no submission and holds nothing about these
//                      runs in memory: it answers from Temporal and the
//                      bucket, which are the only shared records there are.
//   UI      (:14175)   the production build, proxying Connect to B and
//                      pointing the browser's WebSockets at B.
//   orker main         the default pool
//   orker gpu          ORK_ORKER_POOL=gpu
//
// Isolation (AGENTS.md §11.1): one Temporal namespace of this spec's own.
// BOTH engines and BOTH Orkers share it — the deliberate exception the rule
// names, because "any replica answers for any run" is the condition under
// test and splitting the namespace would make it fail for an unrelated reason.

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(uiDir, '..');

// Ports of this spec's own, so one `playwright test` can run every spec in the
// directory without two of them fighting over a listener.
const ENGINE_A = 'http://127.0.0.1:18082';
const ENGINE_B = 'http://127.0.0.1:18083';
const ENGINE_B_WS = 'ws://127.0.0.1:18083';
const UI_ORIGIN = 'http://127.0.0.1:14175';
const B_PORT = '18083';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-logs-dashboard';

// Per-run ork namespaces, and per-run orkflow names inside them.
//
// namespaces-editor.spec.ts fixes its two namespace names so the registry does not grow by
// two on every run. This one deliberately does not, because what it asserts is
// an AGGREGATE over a shared bucket. The dashboard's recent-failures table is
// the top `MostFailingLimit = 5` rows of a 24h window (internal/api/stats.go),
// and rows tied at failed=1/total=1 are ordered by orkflow name ASCENDING
// (`mostFailing`). A fixed namespace collects one `flaky-<stamp>` row per proof
// run, so once five of them sit in the window the newest — this run's own — is
// always the row that gets cut, and the assertion in test 5 can never pass
// again. Measured on the committed tree: three runs, three failures at
// `failure-flaky-<stamp>`, with five older stamps holding the table.
//
// A namespace of this run's own makes every number on the dashboard this run's:
// the failures table has one row and it is ours. The cost is registry entries;
// the empty one is handed back in afterAll, while the other deliberately retains
// this run's orkflows and recent runs as dashboard history.
const STAMP = Date.now().toString(36);
const ALPHA = `alpha-${STAMP}`;
const BETA = `beta-${STAMP}`;
const ORKFLOW = `chatty-${STAMP}`;
const FAILING = `flaky-${STAMP}`;
// The orkflow whose `flaky` task fails its first attempt and passes its
// second: a SUCCEEDED run that retried. Named to sort first among ALPHA's
// orkflows, so a tie on rate is broken in its favour and it is row 0.
const RETRYING = `a-retrying-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'logs-dashboard')
	: path.join(uiDir, 'test-results', 'logs-dashboard');
const TRANSCRIPT = 'live-composition.txt';

// The lines the slow task writes, and the burst the chatty one writes. 40
// immediate lines make the pane scrollable (so scrolling up is a real
// gesture); the 15 that follow arrive one a second, which is slower than any
// flush interval — a reader that only saw the end would see nothing.
const TALK_BURST = 40;
const TALK_TOTAL = 55;
const CHATTER_LINES = 5000;
// The pane keeps and renders this many lines (TaskLogPane's default).
const SCROLLBACK = 2000;

// These tests are sequential and stateful ON PURPOSE: each builds on what the
// last one left in the cluster and the bucket, which is the only way to prove
// a composition rather than five fixtures. Serial mode is the declaration of
// that — without it one failure restarts the worker and re-enters beforeAll,
// minting a new STAMP, and the survivors go looking for a run nobody started.
test.describe.configure({ mode: 'serial' });

const children: ChildProcess[] = [];
// The run the dashboard test fails on purpose, which the fingerprint test
// after it groups, deep-links to and then reclaims.
let failedRun = '';
let run1 = '';
let run2 = '';

function outPath(name: string): string {
	mkdirSync(PROOF_DIR, { recursive: true });
	return path.join(PROOF_DIR, name);
}

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

/** note appends one stamped line to this run's live transcript. */
function note(line: string): void {
	appendFileSync(outPath(TRANSCRIPT), `${new Date().toISOString()}  ${line}\n`);
}

function spawnLogged(
	name: string,
	command: string,
	args: string[],
	env: NodeJS.ProcessEnv,
	cwd: string = repoRoot
): ChildProcess {
	const out = createWriteStream(outPath(name), { flags: 'w' });
	const child = spawn(command, args, {
		cwd,
		env: { ...process.env, ...env },
		stdio: ['ignore', 'pipe', 'pipe']
	});
	child.stdout?.pipe(out);
	child.stderr?.pipe(out);
	children.push(child);
	return child;
}

async function waitFor(
	label: string,
	probe: () => Promise<boolean>,
	timeoutMs = 120_000
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			if (await probe()) return;
		} catch {
			// not ready yet
		}
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await new Promise((r) => setTimeout(r, 250));
	}
}

function tcpOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
	return new Promise((resolve) => {
		const sock = net.connect({ port, host });
		sock.once('connect', () => {
			sock.destroy();
			resolve(true);
		});
		sock.once('error', () => resolve(false));
		sock.setTimeout(1500, () => {
			sock.destroy();
			resolve(false);
		});
	});
}

async function killAll(): Promise<void> {
	const survivors = children.splice(0);
	for (const child of survivors) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 10_000;
	for (const child of survivors) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
	// AGENTS.md §11.1: never leave a stray engine or Orker behind, and anchor both
	// patterns — unanchored, `bin/ork` also matches `bin/orkctl`.
	try {
		execSync("pkill -f '/bin/orker$' || true; pkill -f '/bin/ork$' || true", { timeout: 5_000 });
	} catch {
		// pkill exit status is informational only
	}
}

/** rpc calls one named engine directly, so a test can say WHICH replica. */
async function rpc<T>(base: string, method: string, body: unknown): Promise<T> {
	const res = await fetch(`${base}/orkstack.v1.${method}`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
	const text = await res.text();
	if (!res.ok) throw new Error(`${method} on ${base} failed: ${res.status} ${text}`);
	return JSON.parse(text) as T;
}

interface RunAnswer {
	run?: {
		id: string;
		status: string;
		orkflowVersion?: string;
		taskRuns?: { taskId: string; pool?: string; status: string }[];
	};
}

const getRunFrom = (base: string, runId: string) =>
	rpc<RunAnswer>(base, 'RunService/GetRun', { runId, namespace: ALPHA });

// The wire spells an enum with its declared proto name. `orkctl --json` prints
// the short one, which is a rendering; this is the contract.
const STATUS_FAILED = 'STATUS_FAILED';

/**
 * chattyOrkflow: one task that talks slowly on the `gpu` pool, and one that
 * floods on the default pool. Two pools in one run, so routing is observable
 * in the same document the log pane is proven against.
 */
function chattyOrkflow(version: string, description: string): string {
	return `name: ${ORKFLOW}
version: "${version}"
description: ${description}
namespace: ${ALPHA}

tasks:
  - id: talk
    uses: shell
    pool: gpu
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le ${TALK_BURST} ]; do
          echo "talk line $i"
          i=$((i+1))
        done
        while [ $i -le ${TALK_TOTAL} ]; do
          echo "talk line $i"
          i=$((i+1))
          sleep 1
        done
  - id: chatter
    uses: shell
    pool: main
    needs: [talk]
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le ${CHATTER_LINES} ]; do
          echo "chatter line $i"
          i=$((i+1))
        done
`;
}

const failingOrkflow = () => `name: ${FAILING}
version: "0.1.0"
description: a task that fails, so the dashboard has a failure to report
namespace: ${ALPHA}

tasks:
  - id: boom
    uses: shell
    timeout_seconds: 60
    with:
      op: exec
      command: ["sh", "-c", "echo \\"about to fail\\"; exit 3"]
`;

/**
 * retryingOrkflow: `flaky` reads its own attempt number from task.json — the
 * OCI contract's §3.2 field, read with the jq the shell image ships — and
 * fails attempt 1, so it retries once and succeeds, deterministically, with no
 * stub and no clock. `steady` never retries.
 */
const retryingOrkflow = () => `name: ${RETRYING}
version: "0.1.0"
description: a task that needs a second attempt, so the dashboard has a retry to report
namespace: ${ALPHA}

tasks:
  - id: flaky
    uses: shell
    timeout_seconds: 30
    retry:
      maximum_attempts: 3
      initial_interval_seconds: 1
    with:
      op: exec
      command: ["sh", "-c", "[ \\"$(jq -r .task.attempt /orkstack/input/task.json)\\" -ge 2 ]"]
  - id: steady
    uses: shell
    timeout_seconds: 30
    with:
      op: exec
      command: ["sh", "-c", "true"]
`;

/** openStability opens the dashboard's Stability disclosure unless the
 * remembered state already has. */
async function openStability(page: Page): Promise<void> {
	const details = page.getByTestId('stability');
	await expect(details).toBeVisible({ timeout: 60_000 });
	const isOpen = await details.evaluate((el) => (el as HTMLDetailsElement).open);
	if (!isOpen) await page.getByTestId('stability-toggle').click();
}

/** lineNumbers reads the numbers off a pane's rendered lines, in DOM order. */
async function lineNumbers(page: Page, taskId: string, prefix: string): Promise<number[]> {
	const texts = await page.getByTestId(`task-log-${taskId}`).getByTestId('task-log-line').allInnerTexts();
	const numbers: number[] = [];
	for (const text of texts) {
		const match = text.trim().match(new RegExp(`^${prefix} line (\\d+)$`));
		if (match) numbers.push(Number(match[1]));
	}
	return numbers;
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# One live composition: two namespaces, two pools, an edit and a',
			'# promotion, a followed log and a dashboard, read through a replica that',
			'# started nothing.',
			'#',
			'# Written by ui/e2e/logs-dashboard.spec.ts. Every line below was produced',
			'# by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespaces:     ${ALPHA} (used), ${BETA} (empty on purpose)`,
			`# orkflows:           ${ORKFLOW}, ${FAILING}`,
			`# engine A (writes):  ${ENGINE_A}`,
			`# engine B (reads):   ${ENGINE_B}`,
			''
		].join('\n')
	);

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18082, 18083, 14175]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// The three first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// Both replicas and both Orkers read and write the SAME bucket: orkflow
	// definitions, the version pointer, the namespace registry, the run archive
	// and every log chunk live there, which is what lets B answer for a run A
	// started (AGENTS.md §7).
	const store = {
		ORK_ARTIFACT_BACKEND: 's3',
		ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
		ORK_S3_REGION: 'us-east-1',
		ORK_S3_BUCKET: 'orkstack-artifacts',
		ORK_S3_ACCESS_KEY: 'orkstack',
		ORK_S3_SECRET_KEY: 'orkstack-secret',
		ORK_S3_PATH_STYLE: 'true'
	};
	const cluster = {
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		ORK_TEMPORAL_NAMESPACE: TEMPORAL_NAMESPACE
	};

	spawnLogged('engine-a.log', './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...store,
		ORK_HTTP_ADDR: '127.0.0.1:18082'
	});
	spawnLogged('engine-b.log', './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...store,
		ORK_HTTP_ADDR: '127.0.0.1:18083',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	// Each Orker gets its OWN work root: it is the bind-mountable path the
	// activity workspace needs, and it is also what the Orker derives its
	// identity from, so two Orkers on one host do not collide on the identity
	// lock their startup reaper takes (see orkerEnv).
	spawnLogged('orker-main.log', './bin/orker', [], {
		...cluster,
		...store,
		...orkerEnv('logs-main')
	});
	spawnLogged('orker-gpu.log', './bin/orker', [], {
		...cluster,
		...store,
		...orkerEnv('logs-gpu'),
		ORK_ORKER_POOL: 'gpu'
	});

	for (const [label, base] of [
		['engine A', ENGINE_A],
		['engine B', ENGINE_B]
	] as const) {
		await waitFor(`${label} connect api`, async () => {
			const res = await fetch(`${base}/orkstack.v1.OrkflowService/ListOrkflows`, {
				method: 'POST',
				headers: { 'content-type': 'application/json' },
				body: '{}'
			});
			return res.ok;
		});
	}

	spawnLogged(
		'ui.log',
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14175',
			// Every read the browser makes goes to B — the RPC proxy and the
			// WebSockets both.
			ENGINE_API_URL: ENGINE_B,
			PUBLIC_ENGINE_WS_URL: ENGINE_B_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// Two namespaces. Alpha names the two pools it may dispatch onto, which is
	// also the authoring gate a disallowed pool would hit; beta is deliberately
	// left empty, because "unavailable, not zero" needs a namespace with
	// nothing in it to be falsifiable at all.
	await rpc(ENGINE_A, 'NamespaceService/CreateNamespace', {
		namespace: {
			name: ALPHA,
			description: 'live composition',
			defaultPool: 'main',
			allowedPools: ['main', 'gpu']
		}
	});
	await rpc(ENGINE_A, 'NamespaceService/CreateNamespace', {
		namespace: { name: BETA, description: 'empty namespace' }
	});
	note(`namespaces created on engine A: ${ALPHA} (pools main,gpu), ${BETA} (empty)`);

	// Both pools must have live Orkers before a submit: the engine refuses to
	// start a run whose pool has nobody serving it.
	await waitFor('both pools report live Orkers', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			ENGINE_B,
			'NamespaceService/ListPools',
			{ namespace: ALPHA }
		);
		const live = new Map((answer.pools ?? []).map((p) => [p.name, p.liveOrkerCount ?? 0]));
		return (live.get('main') ?? 0) > 0 && (live.get('gpu') ?? 0) > 0;
	});
	note('engine B reports live Orkers on both pools: main, gpu');

	await rpc(ENGINE_A, 'OrkflowService/ApplyOrkflow', {
		yaml: chattyOrkflow('0.1.0', 'first revision'),
		namespace: ALPHA
	});
	await rpc(ENGINE_A, 'OrkflowService/ApplyOrkflow', {
		yaml: failingOrkflow(),
		namespace: ALPHA
	});
	await rpc(ENGINE_A, 'OrkflowService/ApplyOrkflow', {
		yaml: retryingOrkflow(),
		namespace: ALPHA
	});
	note(`engine A applied ${ORKFLOW}@0.1.0, ${FAILING}@0.1.0 and ${RETRYING}@0.1.0 in ${ALPHA}`);
});

test.afterAll(async () => {
	// The empty namespace holds nothing, so hand it back to the shared
	// registry. Best effort and never fatal: this is tidiness, not a claim,
	// and a proof that has already measured everything must not fail in its
	// own cleanup.
	try {
		await rpc(ENGINE_A, 'NamespaceService/DeleteNamespace', { name: BETA });
	} catch {
		// The engines may already be gone, or the registry may refuse. Either
		// way the run is over and nothing below depends on it.
	}
	await killAll();
});

// The live half: the pane follows while the task runs, keeps the
// engine's order, and every byte of it arrived over a socket served by the
// replica that started nothing.
test('a task log follows live in the browser, in order, from the replica that started nothing', async ({
	page
}) => {
	const sockets: string[] = [];
	page.on('websocket', (ws) => sockets.push(ws.url()));
	// A browser exception during a proof belongs in the record, not in a
	// developer's console: without this a page that threw looks exactly like a
	// page that was slow.
	page.on('pageerror', (err) => note(`BROWSER ERROR: ${err.message}`));
	page.on('console', (msg) => {
		if (msg.type() === 'error') note(`BROWSER CONSOLE ERROR: ${msg.text()}`);
	});

	const submitted = await rpc<{ run: { id: string } }>(ENGINE_A, 'RunService/SubmitRun', {
		orkflowName: ORKFLOW,
		namespace: ALPHA
	});
	run1 = submitted.run.id;
	note(`engine A started run ${run1} (${ORKFLOW}@0.1.0); engine B was not asked`);

	await page.goto(`${UI_ORIGIN}/runs/${run1}?ns=${ALPHA}`);
	await expect(page.getByTestId('run-id')).toHaveText(run1);

	// The pane is collapsed until it is asked for: thirty tasks must not mean
	// thirty sockets.
	await expect(page.getByTestId('task-log-talk')).toHaveCount(0);
	await page.getByTestId('task-log-toggle-talk').click();

	// FOLLOWS WHILE THE TASK RUNS. This is the assertion a post-mortem read
	// cannot pass: lines are on screen while the run's own status pill still
	// says RUNNING, so nothing here came from an artifact published at the end.
	//
	// The threshold is the task's opening burst, which is also what makes the
	// pane taller than its box — a scroll gesture below needs something to
	// scroll.
	await expect
		.poll(async () => (await lineNumbers(page, 'talk', 'talk')).length, { timeout: 120_000 })
		.toBeGreaterThanOrEqual(TALK_BURST);
	const runStatusWhenLive = (await page.getByTestId('run-status').innerText()).trim();
	const taskStatusWhenLive = (await page.getByTestId('task-talk-status').innerText()).trim();
	const linesWhenLive = await lineNumbers(page, 'talk', 'talk');
	note(
		`browser saw ${linesWhenLive.length} lines of task talk at B while the run read ` +
			`${runStatusWhenLive} and the task read ${taskStatusWhenLive}`
	);
	expect(runStatusWhenLive).toBe('RUNNING');
	expect(taskStatusWhenLive).toBe('RUNNING');
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('LIVE');
	await shot(page, 'screenshot-log-following.png');

	// ORDER. The engine's sequence is the order; the pane only ever appends.
	expect(linesWhenLive).toEqual([...linesWhenLive].sort((a, b) => a - b));
	expect(linesWhenLive[0]).toBe(1);

	// SCROLLBACK AND PAUSE. Scrolling up stops the follow, and output arriving
	// afterwards does not yank the reader back down.
	//
	// The park is MID-PANE, not at the top, and that is deliberate: a reader
	// parked at scrollTop=0 cannot be moved to 0 again, so a pane that resets
	// its buffer fires no scroll event and the defect below hides. Half way up
	// is where a real reader sits, and it is the position that makes an
	// unwanted reset observable.
	const box = page.getByTestId('task-log-talk');
	await box.evaluate((el) => el.scrollTo({ top: Math.floor((el.scrollHeight - el.clientHeight) / 2) }));
	await expect(page.getByTestId('task-log-follow-talk')).toHaveText('PAUSED');
	const parkedAt = await box.evaluate((el) => el.scrollTop);
	expect(parkedAt).toBeGreaterThan(0);
	const before = (await lineNumbers(page, 'talk', 'talk')).length;
	await expect
		.poll(async () => (await lineNumbers(page, 'talk', 'talk')).length, { timeout: 60_000 })
		.toBeGreaterThan(before);
	expect(await box.evaluate((el) => el.scrollTop)).toBe(parkedAt);
	note(`follow paused at scrollTop=${parkedAt}; ${before} lines grew while the view stayed put`);
	await shot(page, 'screenshot-log-paused.png');

	// AND THE PAUSE SURVIVES THE TASK ENDING — the moment that would break it,
	// and the reason this assertion is here rather than after a convenient
	// click. The last line of a task arrives with its terminal
	// status event, so a pane whose subscription is invalidated by that event
	// tears its socket down, opens a second one at from_seq=0, empties its
	// buffer, and reads the empty box as "the reader is at the tail": the
	// follow silently resumes and throws the reader to the bottom at exactly
	// the instant the output they were reading is completed. Measured on this
	// tree before the fix: PAUSED scrollTop=500 -> FOLLOWING scrollTop=976.
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('ENDED', { timeout: 180_000 });
	await expect(page.getByTestId('task-talk-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	expect((await page.getByTestId('task-log-follow-talk').innerText()).trim()).toBe('PAUSED');
	expect(await box.evaluate((el) => el.scrollTop)).toBe(parkedAt);
	note(
		`task talk reached SUCCEEDED and its stream ENDED with the reader still parked at ` +
			`scrollTop=${parkedAt}: the terminal status did not resume the follow`
	);

	// RESUME. Jump-to-bottom follows again, and lands at the bottom.
	await page.getByTestId('task-log-jump-talk').click();
	await expect(page.getByTestId('task-log-follow-talk')).toHaveText('FOLLOWING');
	await expect
		.poll(async () =>
			box.evaluate((el) => el.scrollHeight - el.scrollTop - el.clientHeight <= 8)
		)
		.toBe(true);

	// What the stream delivered is the whole output: dense 1..N, no repeats,
	// no gaps.
	const all = await lineNumbers(page, 'talk', 'talk');
	expect(all).toEqual(Array.from({ length: TALK_TOTAL }, (_, i) => i + 1));
	note(`task talk ended with ${all.length} lines, dense 1..${TALK_TOTAL}, no repeats`);
	await shot(page, 'screenshot-log-ended.png');

	// Every socket this page opened was served by B, the replica that started
	// nothing — including the log socket the lines above came down.
	expect(sockets.length).toBeGreaterThan(0);
	for (const url of sockets) expect(url).toContain(`:${B_PORT}`);

	// EXACTLY ONE log socket, for a whole attempt that ran from PENDING to
	// SUCCEEDED with the pane open throughout. A second one is not a harmless
	// reconnect: a new subscription replays the attempt from its first chunk,
	// so every line is downloaded and re-rendered again (up to the whole
	// ORK_ORKER_LOG_MAX_BYTES an attempt may hold) — the exact opposite of the
	// resume `from_seq` exists for, and the same defect the paused reader
	// above feels as a yank to the bottom.
	const logSockets = sockets.filter((u) => u.includes('/tasks/talk/logs'));
	expect(logSockets).toHaveLength(1);
	expect(new URL(logSockets[0]).searchParams.get('from_seq')).toBe('0');
	note(
		`task talk was served by exactly ${logSockets.length} log socket for the whole attempt: ` +
			'no status change re-subscribed the pane'
	);
	note(`sockets opened by the browser, all on engine B:\n    ${sockets.join('\n    ')}`);

	// Two pools, one run: the engine recorded where each task actually went,
	// and B is the replica saying so.
	const answer = await getRunFrom(ENGINE_B, run1);
	const pools = new Map((answer.run?.taskRuns ?? []).map((t) => [t.taskId, t.pool ?? '']));
	expect(pools.get('talk')).toBe('gpu');
	note(`engine B reports run ${run1}: task talk ran on pool ${pools.get('talk')}`);
});

// The half a slow task cannot prove: a chatty task must not
// freeze the tab, and its scrollback must be bounded — visibly, never in
// silence.
test('a chatty task keeps a bounded scrollback and leaves the tab responsive', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs/${run1}?ns=${ALPHA}`);

	// The probe is a tight loop of main-thread round trips started the instant
	// the pane opens, and it does NOT stop when the burst is over: five
	// thousand lines are delivered and rendered inside its first few
	// iterations, so what it records is what the page could do WHILE that
	// happened. A pane that appended one DOM node per delivered line would be
	// inside one long task for that whole window and could answer nothing.
	const openedAt = Date.now();
	await page.getByTestId('task-log-toggle-chatter').click();
	const latencies: number[] = [];
	for (let probe = 0; probe < 25; probe += 1) {
		const started = Date.now();
		await page.evaluate(() => document.querySelectorAll('[data-testid="task-log-line"]').length);
		latencies.push(Date.now() - started);
	}
	await expect(page.getByTestId('task-log-state-chatter')).toHaveText('ENDED', {
		timeout: 180_000
	});
	const renderedIn = Date.now() - openedAt;

	// Bounded: the pane holds the tail, says how much it dropped, and points at
	// the command that still has all of it.
	const rendered = await page
		.getByTestId('task-log-chatter')
		.getByTestId('task-log-line')
		.count();
	expect(rendered).toBeLessThanOrEqual(SCROLLBACK);
	const dropped = page.getByTestId('task-log-dropped-chatter');
	await expect(dropped).toBeVisible();
	await expect(dropped).toContainText('orkctl run logs');

	// The tail is the END of the output, not the beginning: a cap that kept the
	// first 2000 lines would leave a reader watching a task's opening titles.
	const numbers = await lineNumbers(page, 'chatter', 'chatter');
	expect(numbers[numbers.length - 1]).toBe(CHATTER_LINES);
	expect(numbers).toEqual([...numbers].sort((a, b) => a - b));

	const worst = Math.max(...latencies);
	const ordered = [...latencies].sort((a, b) => a - b);
	const median = ordered[Math.floor(ordered.length / 2)];
	note(
		`chatty task: ${CHATTER_LINES} lines written, ${rendered} rendered (cap ${SCROLLBACK}), ` +
			`tail is line ${numbers[numbers.length - 1]}, whole burst on screen ${renderedIn}ms after ` +
			`the pane opened, worst main-thread round trip ${worst}ms and median ${median}ms ` +
			`over ${latencies.length} probes`
	);
	// THE BOUND IS THE CLAIM, so it is set against the claim rather than
	// against the implementation that failed it.
	//
	// "The tab is not frozen" means the main thread comes back between frames,
	// not merely that it comes back eventually. A 5000ms bound would pass a
	// regression that blocked the main thread for four seconds, and four
	// seconds is a frozen tab by any reader's definition.
	//
	// 750ms is still five times the worst this tree has
	// measured on a machine running two engines, two Orkers, a browser and the
	// whole 5000-line burst at once (124ms and 138ms in two recorded runs).
	// The median is bounded separately,
	// because a single unlucky probe on a busy machine is not the property:
	// a page that answered 24 probes in 5ms and one in 700ms is responsive,
	// and one whose typical answer is a quarter of a second is not.
	expect(worst).toBeLessThan(750);
	expect(median).toBeLessThan(250);
	await shot(page, 'screenshot-log-chatty.png');
});

// An orkflow edited and promoted, in the browser, against the
// replica that started nothing — and then executed by the one that starts runs.
test('an orkflow is edited and promoted in the browser, and the next run executes the promoted version', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=edit`);
	await expect(page.getByTestId('editor-form')).toBeVisible();
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');

	await page.getByTestId('editor-yaml').fill(chattyOrkflow('0.2.0', 'second revision'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await expect(page.getByTestId('version-row-0.2.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	note(`browser (engine B) applied ${ORKFLOW}@0.2.0; head is now 0.2.0`);
	await shot(page, 'screenshot-edit-applied.png');

	// Promote the OLDER version. A promotion that only ever moved forward would
	// be indistinguishable from "the newest wins", and this is the assertion
	// that tells them apart: current and head part company.
	await page.getByTestId('promote-0.1.0').click();
	await expect(page.getByTestId('promote-confirm')).toContainText('0.1.0');
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	await shot(page, 'screenshot-promoted.png');

	const listed = await rpc<{ currentVersion: string; headVersion: string }>(
		ENGINE_B,
		'OrkflowService/ListOrkflowVersions',
		{ name: ORKFLOW, namespace: ALPHA }
	);
	expect(listed.currentVersion).toBe('0.1.0');
	expect(listed.headVersion).toBe('0.2.0');
	note(`engine B: ${ORKFLOW} current=0.1.0 head=0.2.0 (promoted in the browser)`);

	// Engine A, which never saw the edit, starts a run that names no version —
	// and gets the promoted one, from the pointer in the shared bucket.
	const submitted = await rpc<{ run: { id: string; orkflowVersion: string } }>(
		ENGINE_A,
		'RunService/SubmitRun',
		{ orkflowName: ORKFLOW, namespace: ALPHA }
	);
	run2 = submitted.run.id;
	expect(submitted.run.orkflowVersion).toBe('0.1.0');
	note(`engine A started run ${run2} with no version and pinned 0.1.0, the promoted one`);
});

// The property a reconnect is for: a dropped socket resumes with
// from_seq. Nothing is replayed twice and nothing is missed — over a socket
// Playwright drops mid-follow, which is the failure a wifi handover is.
test('a dropped log socket resumes from the last sequence, repeating and losing nothing', async ({
	page
}) => {
	const socketUrls: string[] = [];
	let dropped = false;

	await page.routeWebSocket(/\/tasks\/talk\/logs/, (ws) => {
		socketUrls.push(ws.url());
		const server = ws.connectToServer();
		server.onMessage((message) => {
			ws.send(message);
			if (!dropped) {
				dropped = true;
				// The page's socket dies with everything the engine has already
				// sent still on screen. The client's only way back is from_seq.
				void ws.close({ code: 1011, reason: 'injected drop' });
			}
		});
	});

	await page.goto(`${UI_ORIGIN}/runs/${run2}?ns=${ALPHA}`);
	await expect(page.getByTestId('run-id')).toHaveText(run2);
	await page.getByTestId('task-log-toggle-talk').click();

	await expect(page.getByTestId('task-log-state-talk')).toHaveText('ENDED', { timeout: 180_000 });

	const all = await lineNumbers(page, 'talk', 'talk');
	expect(all).toEqual(Array.from({ length: TALK_TOTAL }, (_, i) => i + 1));

	// The drop really happened, and the second socket really resumed: its
	// from_seq is the last sequence the first one delivered, not 0.
	//
	// TWO sockets, exactly: the one that was dropped and the one that resumed.
	// A third would be a re-subscription — and this test would not notice it
	// in its line numbers, because a replay from from_seq=0 into an emptied
	// buffer produces a dense 1..N too. The count is the only thing that can
	// tell a resume from a restart, so the count is asserted.
	expect(dropped).toBe(true);
	expect(socketUrls).toHaveLength(2);
	expect(new URL(socketUrls[0]).searchParams.get('from_seq')).toBe('0');
	const resumed = Number(new URL(socketUrls[1]).searchParams.get('from_seq'));
	expect(resumed).toBeGreaterThan(0);
	note(
		`socket dropped mid-follow on run ${run2}; reconnected with from_seq=${resumed} and the ` +
			`pane still holds a dense 1..${TALK_TOTAL} with no repeats`
	);
	note(`    sockets: ${socketUrls.join('\n             ')}`);
	await shot(page, 'screenshot-log-resumed.png');
});

// The dashboard, scoped to the selected namespace, with pool
// health and recent failures — and the honesty clause, which needs a namespace
// where the numbers genuinely cannot be answered.
test('the dashboard scopes to a namespace, shows pool health and failures, and says unavailable rather than zero', async ({
	page
}) => {
	// A failure to report. Started on A, read on B, like everything else.
	const failed = await rpc<{ run: { id: string } }>(ENGINE_A, 'RunService/SubmitRun', {
		orkflowName: FAILING,
		namespace: ALPHA
	});
	failedRun = failed.run.id;
	note(`engine A started run ${failed.run.id} (${FAILING}), which is meant to fail`);
	await waitFor(
		'the failing run reaches a terminal state',
		async () => (await getRunFrom(ENGINE_B, failed.run.id)).run?.status === STATUS_FAILED,
		180_000
	);

	// ---- alpha: a namespace with real numbers ------------------------------
	await page.goto(`${UI_ORIGIN}/?ns=${ALPHA}`);
	await expect(page.getByTestId('dashboard-heading')).toBeVisible();
	await expect(page.getByTestId('dashboard-namespace')).toContainText(ALPHA);

	// Pool health: both pools, both with an Orker. This is the same answer
	// ListPools gives, from a replica that started nothing. The `gpu` pool is
	// the falsifiable half — it exists only because an Orker was started with
	// ORK_ORKER_POOL=gpu and this namespace allows it.
	await expect(page.getByTestId('pool-main')).toBeVisible();
	await expect(page.getByTestId('pool-gpu')).toBeVisible();
	await expect(page.getByTestId('pool-default-main')).toBeVisible();
	const gpuOrkers = (await page.getByTestId('pool-orkers-gpu').innerText()).trim();
	expect(Number(gpuOrkers.split(' ')[0])).toBeGreaterThanOrEqual(1);
	note(`dashboard pool health for ${ALPHA}: gpu has ${gpuOrkers}`);

	// Recent failures: the orkflow that failed, named, with its tally.
	await expect
		.poll(async () => page.getByTestId(`failure-${FAILING}`).count(), { timeout: 60_000 })
		.toBe(1);
	await expect(page.getByTestId(`failure-${FAILING}`)).toContainText('1');

	// Counts that ARE answerable are answered. A real zero is a zero; the
	// unavailable state below is reserved for numbers that do not exist.
	await expect(page.getByTestId('stat-succeeded')).toHaveAttribute('data-unavailable', 'false');
	await expect(page.getByTestId('stat-failed')).toHaveAttribute('data-unavailable', 'false');
	await expect(page.getByTestId('stat-success-rate')).toHaveAttribute('data-unavailable', 'false');
	const alphaNumbers = {
		succeeded: await page.getByTestId('stat-succeeded').innerText(),
		failed: await page.getByTestId('stat-failed').innerText(),
		running: await page.getByTestId('stat-running').innerText(),
		rate: await page.getByTestId('stat-success-rate').innerText(),
		p50: await page.getByTestId('stat-p50').innerText(),
		p95: await page.getByTestId('stat-p95').innerText()
	};
	note(`dashboard for ${ALPHA}, served by engine B: ${JSON.stringify(alphaNumbers)}`);
	expect(Number(alphaNumbers.failed)).toBeGreaterThanOrEqual(1);

	// ONE WINDOW, ONE POPULATION. The note under the percentiles states them as
	// a SHARE of the set they are drawn from: the window's runs that have
	// FINISHED, which is the same set the success rate beside them is a rate
	// over — not a bare count of runs that CLOSED in the window, which is a
	// different set again.
	//
	// The denominator EXCLUDES `running`, and that is what is asserted below.
	// `running` is the one number on this row that is not about the window: it
	// is a fact about now (proto/orkstack/v1/stats.proto), so a namespace with
	// one long-lived run started before the window would inflate the total and
	// the samples would be a share of a set they are not drawn from. `orkctl
	// stats` prints the same denominator (cmd/orkctl/stats.go), and one number
	// must not have two.
	const coverage = (await page.getByTestId('stat-p50-note').innerText()).trim();
	const share = coverage.match(/^(\d+) of the window's (\d+) finished runs/);
	if (!share) throw new Error(`the p50 note reads ${JSON.stringify(coverage)}`);
	const finishedRuns =
		Number(alphaNumbers.succeeded) +
		Number(alphaNumbers.failed) +
		Number(await page.getByTestId('stat-canceled').innerText());
	expect(Number(share[2])).toBe(finishedRuns);
	// The samples are drawn from those finished runs, so they cannot outnumber
	// them: a bigger number would be two populations parting company again.
	expect(Number(share[1])).toBeLessThanOrEqual(finishedRuns);
	note(`dashboard for ${ALPHA}: percentiles cover ${coverage}, over the same runs the ` +
		`rate ${alphaNumbers.rate} is a rate over (${alphaNumbers.running} running excluded: ` +
		'that number is about now, not about the window)');
	await shot(page, 'screenshot-dashboard.png');

	// ---- beta: a namespace where the numbers cannot be answered ------------
	// The switcher is the way in, so the scoping proven here is the one a user
	// gets.
	await selectNamespace(page, BETA);
	await page.waitForURL(new RegExp(`ns=${BETA}`));
	await expect(page.getByTestId('dashboard-namespace')).toContainText(BETA);

	// Nothing of alpha's leaks in.
	await expect(page.getByTestId(`failure-${FAILING}`)).toHaveCount(0);
	await expect(page.getByTestId('failures-empty')).toBeVisible();
	// Nothing failed, so there is nothing to fingerprint: the disclosure is
	// not offered at all rather than opening onto an empty table.
	await expect(page.getByTestId('failure-fingerprints')).toHaveCount(0);

	// THE HONESTY CLAUSE. A success rate over no finished runs is unanswerable,
	// and 0% is the most alarming number a dashboard can show. Percentiles over
	// no samples are the same. Both say so, in words, and neither reads 0.
	const rate = page.getByTestId('stat-success-rate');
	await expect(rate).toHaveAttribute('data-unavailable', 'true');
	await expect(rate).toHaveText('unavailable');
	await expect(page.getByTestId('stat-success-rate-note')).toContainText('no run finished');
	for (const id of ['stat-p50', 'stat-p95']) {
		await expect(page.getByTestId(id)).toHaveAttribute('data-unavailable', 'true');
		await expect(page.getByTestId(id)).toHaveText('unavailable');
	}
	// A count of nothing IS zero, and is shown as zero: the two states are
	// different answers and the dashboard tells them apart.
	await expect(page.getByTestId('stat-succeeded')).toHaveText('0');
	await expect(page.getByTestId('stat-succeeded')).toHaveAttribute('data-unavailable', 'false');
	// Stability is offered even here — the absence of stability data is itself
	// shown — and nothing of alpha's retries leaks into it.
	await openStability(page);
	await expect(page.getByTestId('stability-empty')).toBeVisible({ timeout: 60_000 });
	await expect(page.locator('[data-testid^="stability-row-"]').filter({ hasText: 'flaky' })).toHaveCount(0);
	note(`stability for ${BETA}: ${(await page.getByTestId('stability-empty').innerText()).trim()}`);
	note(
		`dashboard for ${BETA} (empty): success rate and percentiles render "unavailable", ` +
			'while the run counts render a real 0'
	);
	await shot(page, 'screenshot-dashboard-unavailable.png');
});

// The fingerprints: the failed run grouped by what failed and why, linked to
// the runs list it came from, and honest about evidence that is gone. Read, as
// everything here is, through the replica that started nothing.
test('the recent-failures section expands into fingerprints that name the task and code, deep-link to the filtered runs list, and say unavailable for reclaimed evidence', async ({
	page
}) => {
	expect(failedRun, 'the dashboard test left a failed run behind').not.toBe('');
	await page.goto(`${UI_ORIGIN}/?ns=${ALPHA}`);
	await expect
		.poll(async () => page.getByTestId(`failure-${FAILING}`).count(), { timeout: 60_000 })
		.toBe(1);

	await page.getByTestId('failure-fingerprints-toggle').click();
	// ALPHA holds only this run's orkflows, so the failing one is the row; it
	// is located by its name rather than assumed to sort first.
	const row = page.locator('[data-testid^="fingerprint-row-"]').filter({ hasText: FAILING });
	await expect(row).toHaveCount(1, { timeout: 60_000 });
	for (const text of [FAILING, 'boom', 'CONTAINER_EXIT_NONZERO', 'ACTIVITY_FAILED']) {
		await expect(row).toContainText(text);
	}
	const rowId = (await row.getAttribute('data-testid')) ?? '';
	const index = rowId.replace('fingerprint-row-', '');
	note(`fingerprint row ${rowId}: ${(await row.innerText()).replace(/\s+/g, ' ')}`);

	// The image the final attempt ran, as a pinned digest.
	await page.getByTestId('fingerprint-dim-image').check();
	const image = page.getByTestId(`fingerprint-cell-${index}-image`);
	await expect(image).toContainText('@sha256:', { timeout: 30_000 });
	await expect(image).toHaveAttribute('data-unavailable', 'false');
	note(`fingerprint image for ${FAILING}: ${await image.innerText()}`);

	// The CLI that reproduces these rows.
	await page.getByTestId('fingerprint-cli-trigger').click();
	const command = (await page.getByTestId('fingerprint-cli-command').innerText()).trim();
	expect(command).toContain('stats failures');
	expect(command).toContain('--group-by orkflow,task,code,category,image');
	note(`fingerprint CLI equivalent: ${command}`);
	await page.keyboard.press('Escape');
	await shot(page, 'screenshot-dashboard-fingerprints.png');

	// The row is a link to the runs it counts: the namespace, the orkflow,
	// status failed, and the exact window the breakdown answered.
	await row.click();
	await page.waitForURL(/\/runs\?/);
	const target = new URL(page.url());
	expect(target.searchParams.get('status')).toBe('failed');
	expect(target.searchParams.get('orkflow')).toBe(FAILING);
	expect(target.searchParams.get('ns')).toBe(ALPHA);
	expect(target.searchParams.get('since')).toBeTruthy();
	expect(target.searchParams.get('until')).toBeTruthy();
	await expect(page.getByTestId(`run-row-${failedRun}`)).toBeVisible({ timeout: 30_000 });
	note(`fingerprint row navigated to ${target.pathname}${target.search}`);

	// Reclaim the run's attempt records the way the cleanup worker does when
	// retention ends. The archived run stays; the evidence of which image it
	// ran does not.
	const reclaimed = execFileSync('go', ['run', './test/tools/reclaim-run', ALPHA, failedRun], {
		cwd: repoRoot,
		env: {
			...process.env,
			ORK_ARTIFACT_BACKEND: 's3',
			ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
			ORK_S3_REGION: 'us-east-1',
			ORK_S3_BUCKET: 'orkstack-artifacts',
			ORK_S3_ACCESS_KEY: 'orkstack',
			ORK_S3_SECRET_KEY: 'orkstack-secret',
			ORK_S3_PATH_STYLE: 'true'
		}
	}).toString().trim();
	note(reclaimed);

	await page.goto(`${UI_ORIGIN}/?ns=${ALPHA}`);
	// The disclosure remembers it was open; the dimension chips do not.
	await expect(page.getByTestId('fingerprint-dim-image')).toBeVisible({ timeout: 60_000 });
	await page.getByTestId('fingerprint-dim-image').check();
	const after = page.locator('[data-testid^="fingerprint-row-"]').filter({ hasText: FAILING });
	// The engine caches an answer for its stats TTL, and the dashboard refetches
	// on every new measurement, so the reclaim shows within a few refreshes.
	await expect
		.poll(async () => (await after.locator('[data-unavailable="true"]').count()), { timeout: 60_000 })
		.toBe(1);
	const unavailableCell = after.locator('[data-unavailable="true"]');
	await expect(unavailableCell).toHaveText('unavailable');
	await expect(unavailableCell).toHaveAttribute('data-testid', /-image$/);
	const footer = page.getByTestId('failure-fingerprints-footer');
	await expect(footer).toContainText('attempt records for 1 run were reclaimed or never written');
	note(`after the reclaim: ${(await after.innerText()).replace(/\s+/g, ' ')} / ${(await footer.innerText()).replace(/\s+/g, ' ')}`);
	await shot(page, 'screenshot-dashboard-fingerprints-unavailable.png');
});

// The retries the success rate hides: a run that SUCCEEDED on its second
// attempt, named on the dashboard with its rate and cause, linked to the runs
// list it came from, and — once its attempt records are reclaimed — said to be
// unavailable, never 0%. Read, as everything here is, through the replica that
// started nothing.
test("the dashboard's stability section names the task that retried, its rate and cause, links to the filtered runs list, and says unavailable rather than zero", async ({
	page
}) => {
	// Runs to wait for, archive writes and a cache TTL: more than the
	// default budget, never a sleep.
	test.setTimeout(420_000);
	const submitted = await rpc<{ run: { id: string } }>(ENGINE_A, 'RunService/SubmitRun', {
		orkflowName: RETRYING,
		namespace: ALPHA
	});
	const retriedRun = submitted.run.id;
	note(`engine A started run ${retriedRun} (${RETRYING}), whose flaky task fails attempt 1`);
	await waitFor(
		'the retrying run succeeds',
		async () => (await getRunFrom(ENGINE_B, retriedRun)).run?.status === 'STATUS_SUCCEEDED',
		180_000
	);
	const attempts = await rpc<{ attempts?: { taskId: string; attempt: number; orkerId?: string }[] }>(
		ENGINE_B,
		'RunService/ListTaskAttempts',
		{ runId: retriedRun, namespace: ALPHA }
	);
	const firstAttempt = (attempts.attempts ?? []).find((a) => a.taskId === 'flaky' && a.attempt === 1);
	const orkerId = firstAttempt?.orkerId ?? '';
	expect(orkerId, 'attempt 1 of flaky names the Orker that ran it').not.toBe('');
	note(`attempt 1 of flaky in ${retriedRun} ran on ${orkerId}`);

	await page.goto(`${UI_ORIGIN}/?ns=${ALPHA}`);
	await openStability(page);
	const row = page.getByTestId('stability-row-0');
	// The archive is written when the run ends and the engine caches an answer
	// for its stats TTL; the dashboard refetches on every new measurement.
	await expect(row).toContainText(RETRYING, { timeout: 90_000 });
	for (const text of ['flaky', '100.0%', 'ACTIVITY_FAILED', 'CONTAINER_EXIT_NONZERO']) {
		await expect(row).toContainText(text);
	}
	await expect(page.getByTestId('stability-row-0-rate')).toHaveAttribute('data-unavailable', 'false');
	note(`stability row 0: ${(await row.innerText()).replace(/\s+/g, ' ')}`);
	const footerBefore = (await page.getByTestId('stability-footer').innerText()).replace(/\s+/g, ' ');
	note(`stability footer before the reclaim: ${footerBefore}`);
	const missingBefore = Number(footerBefore.match(/missing for (\d+)/)?.[1] ?? 'NaN');

	// Grouped by Orker too: the row names the Orker whose attempt FAILED.
	await page.getByTestId('stability-dim-orker').check();
	const orkerRow = page.locator('[data-testid^="stability-row-"]').filter({ hasText: RETRYING }).filter({ hasText: '100.0%' });
	await expect(orkerRow).toContainText(orkerId, { timeout: 30_000 });
	note(`stability grouped by task,orker: ${(await orkerRow.first().innerText()).replace(/\s+/g, ' ')}`);

	await page.getByTestId('stability-cli-trigger').click();
	const command = (await page.getByTestId('stability-cli-command').innerText()).trim();
	expect(command).toContain('stats flakes');
	expect(command).toContain('--group-by task,orker');
	note(`stability CLI equivalent: ${command}`);
	await page.keyboard.press('Escape');
	await shot(page, 'screenshot-dashboard-stability.png');

	// The row is a link to the runs it counts: namespace, orkflow and the
	// answered window — and NO status, because a flaky task's runs mostly
	// succeeded.
	await page.getByTestId('stability-dim-orker').uncheck();
	await expect(page.getByTestId('stability-row-0')).toContainText(RETRYING, { timeout: 30_000 });
	await page.getByTestId('stability-row-0-task').click();
	await page.waitForURL(/\/runs\?/);
	const target = new URL(page.url());
	expect(target.searchParams.get('orkflow')).toBe(RETRYING);
	expect(target.searchParams.get('ns')).toBe(ALPHA);
	expect(target.searchParams.get('since')).toBeTruthy();
	expect(target.searchParams.get('until')).toBeTruthy();
	expect(target.searchParams.get('status')).toBeNull();
	await expect(page.getByTestId(`run-row-${retriedRun}`)).toBeVisible({ timeout: 30_000 });
	note(`stability row navigated to ${target.pathname}${target.search}`);

	// Reclaim the run's attempt records as the cleanup worker does when
	// retention ends. The archived run stays; the evidence that it retried
	// does not — and the dashboard must say so rather than call it clean.
	const reclaimed = execFileSync('go', ['run', './test/tools/reclaim-run', ALPHA, retriedRun], {
		cwd: repoRoot,
		env: {
			...process.env,
			ORK_ARTIFACT_BACKEND: 's3',
			ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
			ORK_S3_REGION: 'us-east-1',
			ORK_S3_BUCKET: 'orkstack-artifacts',
			ORK_S3_ACCESS_KEY: 'orkstack',
			ORK_S3_SECRET_KEY: 'orkstack-secret',
			ORK_S3_PATH_STYLE: 'true'
		}
	}).toString().trim();
	note(reclaimed);

	await page.goto(`${UI_ORIGIN}/?ns=${ALPHA}`);
	// The disclosure remembers it was open.
	await openStability(page);
	const after = page.locator('[data-testid^="stability-row-"]').filter({ hasText: RETRYING }).filter({ hasText: 'flaky' });
	await expect
		.poll(async () => (await after.locator('[data-unavailable="true"]').count()), { timeout: 90_000 })
		.toBe(1);
	const rate = after.locator('[data-unavailable="true"]');
	await expect(rate).toHaveText('unavailable');
	await expect(rate).toHaveAttribute('data-testid', /-rate$/);
	const footer = page.getByTestId('stability-footer');
	await expect(footer).toContainText(
		`${missingBefore + 1} runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean`
	);
	note(`after the reclaim: ${(await after.innerText()).replace(/\s+/g, ' ')} / ${(await footer.innerText()).replace(/\s+/g, ' ')}`);
	await shot(page, 'screenshot-dashboard-stability-unavailable.png');
});
