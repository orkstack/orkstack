import { expect, test, type Page } from './test';
import { selectTask } from './uiHelpers';
import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	noteTo,
	orkerEnv,
	ownBucket,
	proofOut,
	removeBucket,
	repoRoot,
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
const BUCKET = ownBucket('run-page-capacity');

// "Waiting for capacity" on the run detail page, in a browser, against the
// real stack. Two pools, each with one Orker, each saturated by a running
// task of an earlier run:
//
//   cap    declares a capacity of 1 cpu with a per-task cap of 1 cpu: one
//          running task fills it.
//   slots  declares no capacity and runs one task at a time
//          (max_concurrent 1).
//
//   C1  a task of a second run waiting on `cap` reads "Waiting for capacity"
//       with what stands reserved, and its title says why;
//   C2  the positive control, in the same answer: a task of the same run
//       waiting on `slots`, an Orker that is busy but not at a capacity,
//       still reads plain "Waiting for capacity" — the caption is derived
//       from the inventory, not from "nothing has started";
//   C3  `orkctl run inspect` prints the same two lines for the same run;
//   C4  once the earlier run is cancelled and its containers are removed,
//       both waiting tasks run, and the run succeeds.
//
// Topology: one engine, the production UI build, two Orkers started before
// anything is submitted. Ports and a Temporal namespace of this spec's own
// (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18571';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;

const TEMPORAL_NAMESPACE = temporalNamespace('run-capacity');
const STAMP = Date.now().toString(36);
const HOLD = `capacity-hold-${STAMP}`;
const WAIT = `capacity-wait-${STAMP}`;
const CAP_ORKER = 'orker-e2e-cap';
const SLOTS_ORKER = 'orker-e2e-slots';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page-capacity')
	: path.join(uiDir, 'test-results', 'run-page-capacity');
const TRANSCRIPT = 'run-page-capacity-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

let orkctl = '';

function startOrker(id: string, pool: string, extra: Record<string, string>) {
	spawnLogged(outPath(`${id}.log`), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv(`run-page-capacity-${pool}`),
		ORK_ORKER_ID: id,
		ORK_ORKER_POOL: pool,
		ORK_ORKER_HEARTBEAT_INTERVAL: '1s',
		...extra
	});
	note(`orker ${id} started on pool ${pool} with ${JSON.stringify(extra)}`);
}

// A task that runs until it is told to stop, or for ten minutes at most, so a
// container whose Orker was killed under it cannot outlive the spec for long.
// The shell handles SIGTERM, so a cancel ends it within a second rather than
// after the grace.
const holding = `["sh", "-c", "trap 'exit 0' TERM; i=0; while [ $i -lt 600 ]; do sleep 1; i=$((i+1)); done"]`;

function holdOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${HOLD}
version: "0.1.0"
description: Saturates pool cap by capacity and pool slots by its one slot.

tasks:
  - id: hold-cap
    uses: shell
    pool: cap
    timeout_seconds: 600
    cancellation_grace_period_seconds: 5
    resources:
      cpu: "1"
    with:
      op: exec
      command: ${holding}
  - id: hold-slots
    uses: shell
    pool: slots
    timeout_seconds: 600
    cancellation_grace_period_seconds: 5
    with:
      op: exec
      command: ${holding}
`;
}

function waitOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${WAIT}
version: "0.1.0"
description: One task on each saturated pool.

tasks:
  - id: wait-cap
    uses: shell
    pool: cap
    timeout_seconds: 120
    resources:
      cpu: "1"
    with:
      op: exec
      command: ["echo", "ran on cap"]
  - id: wait-slots
    uses: shell
    pool: slots
    timeout_seconds: 120
    with:
      op: exec
      command: ["echo", "ran on slots"]
`;
}

interface InspectAnswer {
	attempts?: { taskId: string; state?: string; capacityWait?: unknown }[];
}

/** startedTasks is the task ids InspectRun reports an Orker has started. */
async function startedTasks(runId: string): Promise<string[]> {
	const answer = await rpc<InspectAnswer>('RunService/InspectRun', { runId });
	return (answer.attempts ?? [])
		.filter((a) => a.state === 'LIVE_ATTEMPT_STATE_STARTED')
		.map((a) => a.taskId)
		.sort();
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	appendFileSync(
		outPath(TRANSCRIPT),
		[
			'# "Waiting for capacity" on the run detail page: a task waiting on a pool',
			'# whose Orker is at its declared capacity, beside a task waiting on a pool',
			'# whose Orker is busy but declares none, and `orkctl run inspect` for the',
			'# same run.',
			'#',
			'# Written by ui/e2e/run-page-capacity.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${HOLD}, ${WAIT} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18571)) throw new Error('port 18571 already in use (stray process?)');

	ensureActivityImages();
	orkctl = bin('orkctl');

	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18571',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);

	startOrker(CAP_ORKER, 'cap', { ORK_ORKER_CAPACITY_CPU: '1', ORK_ORKER_MAX_CPU: '1', ORK_ORKER_DEFAULT_CPU: '1' });
	startOrker(SLOTS_ORKER, 'slots', { ORK_ORKER_MAX_CONCURRENT: '1' });
	await waitFor(
		'both orkers in the inventory, the cap orker with its capacity',
		async () => {
			const answer = await rpc<{ orkers?: { orkerId: string; capacity?: { cpu?: number } }[] }>(
				'ActivityService/ListOrkers',
				{}
			);
			const ids = new Map((answer.orkers ?? []).map((o) => [o.orkerId, o]));
			return ids.get(CAP_ORKER)?.capacity?.cpu === 1 && ids.has(SLOTS_ORKER);
		},
		120_000
	);
	note('both orkers answer in the inventory; the cap orker reports a capacity of 1 cpu');

	await rpc('OrkflowService/ApplyOrkflow', { yaml: holdOrkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: waitOrkflow() });
	note(`applied ${HOLD} and ${WAIT}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a task waiting on a pool at its capacity says so; one waiting on a busy pool without one does not', async ({
	page
}) => {
	// Two Orker boots' worth of waiting, the capacity read, and the run that
	// follows once it frees: more than the default per-test budget.
	test.setTimeout(600_000);
	// Saturate both pools.
	const hold = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: HOLD })).run.id;
	note(`submitted ${hold}; waiting until both of its tasks are running`);
	await waitFor(
		'both hold tasks started',
		async () => (await startedTasks(hold)).join(',') === 'hold-cap,hold-slots',
		120_000
	);
	note(`${hold}: hold-cap and hold-slots are running`);

	const waiting = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: WAIT })).run.id;
	note(`submitted ${waiting} with both pools saturated`);
	await page.goto(`${UI_ORIGIN}/runs/${waiting}?ns=default`);

	// C1 — the pool at its capacity. The live facts are the selected task's,
	// in the Item card: one task at a time.
	await selectTask(page, 'wait-cap');
	const cap = page.getByTestId('task-wait-cap-live');
	await expect(cap).toHaveAttribute('data-live', 'capacity', { timeout: 60_000 });
	await expect(cap).toContainText('Waiting for capacity · every Orker of pool cap is full (reserved 1/1 cpu)');
	await cap.hover();
	const capTip = page.getByRole('tooltip');
	await expect(capTip).toHaveText(/^No orker of pool cap has room for another task/);
	note(`C1 task-wait-cap-live data-live=capacity text="${await cap.innerText()}"`);
	note(`C1 tooltip="${await capTip.innerText()}"`);

	// C2 — the control, from the same answer: busy, but not by a capacity.
	await selectTask(page, 'wait-slots');
	const slots = page.getByTestId('task-wait-slots-live');
	await expect(slots).toHaveAttribute('data-live', 'waiting');
	// (The UI now words both "Waiting for capacity"; only a pool at its
	// declared capacity says which Orkers are full and what stands reserved.)
	await expect(slots).toContainText('Waiting for capacity');
	await expect(slots).not.toContainText('every Orker of pool');
	note(`C2 task-wait-slots-live data-live=waiting text="${await slots.innerText()}"`);
	await shot(page, 'capacity-waiting.png');
	await page.emulateMedia({ colorScheme: 'dark' });
	await shot(page, 'capacity-waiting-dark.png');
	await page.emulateMedia({ colorScheme: 'light' });

	// C3 — the CLI's own answer for the same run.
	const args = ['--server', ENGINE, '--namespace', 'default', 'run', 'inspect', waiting];
	const out = execFileSync(orkctl, args, { cwd: repoRoot, encoding: 'utf8' });
	note(`$ orkctl ${args.join(' ')}\n${out}`);
	expect(out).toContain('wait-cap: waiting for capacity for');
	expect(out).toContain('every orker of pool cap is full (reserved 1/1 cpu)');
	expect(out).toContain('wait-slots: waiting for an orker on pool slots for');

	// C4 — the capacity frees and the waiting tasks run.
	await rpc('RunService/CancelRun', { runId: hold, reason: 'run-page-capacity e2e: free both pools' });
	note(`cancelled ${hold}`);
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 180_000 });
	await expect(page.getByTestId('task-wait-cap-live')).toHaveCount(0);
	note(`C4 ${waiting} SUCCEEDED once ${hold}'s containers were removed`);
	await shot(page, 'capacity-freed.png');
});
