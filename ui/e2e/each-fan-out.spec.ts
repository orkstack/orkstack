import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
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
	rpc as callEngine,
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('each-fan-out');

// Fan-out (`each:`) on the run detail page, in a browser, against the real
// stack (docs/DSL.md "Fan-out").
//
// What is measured is the client surface of one claim: a fan-out is ONE card
// whose items are each a durable task of their own — a cell that moves from
// that item's own event, a panel that reads that item's own attempts, log and
// artifacts, and a deep link that names the item.
//
// The fan-out at the item ceiling (E4, a render budget) is
// each-fan-out-ceiling.spec.ts: a budget runs alone (ui/playwright.config.ts,
// `projects`), and these tests need not.
//
// Topology: one engine, one Orker, the production UI build. Isolation per
// AGENTS.md §11.1 — a Temporal namespace of this spec's own and stamped
// orkflow names, so a parallel agent cannot steal these activities and two
// runs of this spec cannot collide.

// Ports of this spec's own, so the whole e2e directory can run in one go.
const ENGINE = 'http://127.0.0.1:18597';
const ENGINE_WS = 'ws://127.0.0.1:18597';
const UI_ORIGIN = 'http://127.0.0.1:14597';

const TEMPORAL_NAMESPACE = temporalNamespace('each-fan-out');
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const FAN_OUT = `each-fan-out-${STAMP}`;
const FAILING = `each-fan-out-fails-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'each-fan-out')
	: path.join(uiDir, 'test-results', 'each-fan-out');
const TRANSCRIPT = 'each-fan-out-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// `list` takes a few seconds, so the page is open before the fan-out
// expands; `convert` fans out over the run's six input parts, two at a time,
// each item sleeping long enough for the page to watch its cell move; item 2
// exits 1 on its FIRST attempt only, so exactly one item retries; `merge`
// reads every item's part.txt, staged under one directory per item, in item
// order, and `op: eval` makes what it read its values.
//
// The parts are the run's INPUT, so this spec exercises an input-sourced
// expansion; the shipped example computes its list in a json task and is run
// end to end by cmd/orkctl/examples_integration_test.go. The
// upstream-sourced expansion is also proven by the engine suite
// (internal/engine/each_integration_test.go).
const PARTS = ['a', 'b', 'c', 'd', 'e', 'f'].map((name) => ({ name }));

function fanOutOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${FAN_OUT}
version: "0.1.0"
description: Six parts fanned out two at a time, one retried, merged.

inputs:
  parts:
    type: array

tasks:
  - id: list
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: [sh, -c, "echo listing six parts; sleep 3"]

  - id: convert
    uses: shell
    needs: [list]
    timeout_seconds: 120
    each:
      over: '{{ .inputs.parts }}'
      as: part
      max_items: 20
      max_parallel: 2
    retry: { maximum_attempts: 2 }
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "converting item {{ .each.index }} of {{ .each.count }}:" {{ sh .each.part.name }}
        sleep 3
        printf '%s' {{ sh .each.part.name }} > /orkstack/output/artifacts/part.txt
        if [ {{ .each.index }} -eq 2 ] && grep -q '"attempt": 1,' /orkstack/input/task.json; then
          echo "item 2 fails its first attempt on purpose" >&2
          exit 1
        fi

  - id: merge
    uses: shell
    needs: [convert]
    timeout_seconds: 120
    with:
      op: eval
      command: [sh, -c, "cat /orkstack/input/artifacts/convert/*/part.txt"]
`;
}

// One at a time, item 3 always fails: items 4 and 5 are never dispatched.
function failingOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${FAILING}
version: "0.1.0"
description: Item 3 always fails; the items after it never start.

inputs:
  parts:
    type: array

tasks:
  - id: convert
    uses: shell
    timeout_seconds: 120
    each:
      over: '{{ .inputs.parts }}'
      max_items: 10
      max_parallel: 1
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "item {{ .each.index }}"
        if [ {{ .each.index }} -eq 3 ]; then exit 1; fi
`;
}

let fanOutRun = '';

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# Fan-out on the run detail page: a live item grid, an item panel',
			'# with its own attempts, log and CLI, an item deep link, a fan-in',
			'# that read every item, and a failed item that names itself.',
			'#',
			'# Written by ui/e2e/each-fan-out.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${FAN_OUT}, ${FAILING} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	// Prerequisites, stated as failures rather than skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18597, 14597]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18597',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('each-fan-out')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14597',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// Submit refuses a pool with no live Orker — and, for a fan-out, one
	// whose Orker predates fan-out — so the Orker must be polling first.
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: fanOutOrkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: failingOrkflow() });
	note(`applied ${FAN_OUT} and ${FAILING}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test.describe.configure({ mode: 'serial' });

test('a fan-out card fills its item grid live', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: FAN_OUT,
		input: { data: { parts: PARTS } }
	});
	fanOutRun = submitted.run.id;
	note(`submitted run ${fanOutRun}`);

	// Open the page BEFORE the fan-out expands, so every cell it shows turning
	// SUCCEEDED was moved by an item event, not read from a finished snapshot.
	await page.goto(`${UI_ORIGIN}/runs/${fanOutRun}?ns=default`);
	await expect(page.getByTestId('task-convert-each-summary')).toBeVisible();

	// A cell that is RUNNING while the run is still RUNNING: the grid is live.
	await expect(page.getByTestId('task-convert-item-0')).toHaveAttribute('data-status', /RUNNING|SUCCEEDED/, {
		timeout: 120_000
	});
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING');
	await shot(page, 'screenshot-each-live.png');

	for (let index = 0; index < 6; index++) {
		await expect(page.getByTestId(`task-convert-item-${index}`)).toHaveAttribute(
			'aria-label',
			new RegExp(`^item ${index}, SUCCEEDED, attempt \\d$`),
			{ timeout: 180_000 }
		);
	}
	await expect(page.getByTestId('task-convert-each-summary')).toContainText('6 of 6 settled');
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	// One card for the fan-out, and its retry badge counts items.
	await expect(page.getByTestId('task-convert-retries')).toHaveText('1 item retried');
	note(`summary: ${await page.getByTestId('task-convert-each-summary').textContent()}`);
	await shot(page, 'screenshot-each-settled.png');
});

test('an item that retried shows its own attempts and logs', async ({ page }) => {
	expect(fanOutRun).not.toBe('');
	await page.goto(`${UI_ORIGIN}/runs/${fanOutRun}?ns=default`);
	await page.getByTestId('task-convert-item-2').click();
	const panel = page.getByTestId('task-convert-item-panel');
	await expect(panel).toBeVisible();
	await expect(page.getByTestId('task-convert-item-panel-label')).toHaveText('convert[2]');

	// Two attempts, the first FAILED — item 2's own timeline.
	await expect(page.getByTestId('task-convert-attempt-1-outcome')).toHaveText(/FAILED/, { timeout: 30_000 });
	await expect(page.getByTestId('task-convert-attempt-2-outcome')).toHaveText(/SUCCEEDED/);

	// The log pane streams item 2's own output.
	await page.getByTestId('task-log-toggle-convert').click();
	await expect(page.getByTestId('task-log-convert')).toContainText('converting item 2 of 6: c', { timeout: 30_000 });
	await expect(page.getByTestId('task-log-convert')).not.toContainText('converting item 1 of 6');

	// The CLI equivalent names --item 2.
	await page.getByTestId('task-convert-item-panel-cli-logs-trigger').click();
	await expect(page.getByTestId('task-convert-item-panel-cli-logs-command')).toContainText("'--item' '2'");
	note(`item 2 panel: ${(await panel.textContent())?.replace(/\s+/g, ' ').slice(0, 400)}`);
	await shot(page, 'screenshot-each-item-panel.png');
});

test('an item deep link restores the selection', async ({ page }) => {
	expect(fanOutRun).not.toBe('');
	await page.goto(`${UI_ORIGIN}/runs/${fanOutRun}?ns=default&task=convert&item=2&attempt=1`);
	await expect(page.getByTestId('task-convert-item-panel')).toBeVisible();
	await expect(page.getByTestId('task-convert-item-panel-label')).toHaveText('convert[2]');
	await expect(page.getByTestId('task-convert-item-2')).toHaveAttribute('aria-pressed', 'true');
	// The linked attempt is the FIRST, failed one: only that try printed the
	// failure line, so the pane reading it is the pane reading attempt 1. (The
	// pane's "attempt N" chip is drawn for a try after the first or a reader's
	// own pick, never for a link to attempt 1 — TaskLogPane, unchanged here.)
	await expect(page).toHaveURL(/item=2.*attempt=1|attempt=1.*item=2/);
	await expect(page.getByTestId('task-log-convert')).toContainText('item 2 fails its first attempt on purpose', {
		timeout: 30_000
	});
	await page.reload();
	await expect(page.getByTestId('task-convert-item-panel-label')).toHaveText('convert[2]');
	await shot(page, 'screenshot-each-deep-link.png');
});

test('the merge task read every item', async ({ page }) => {
	expect(fanOutRun).not.toBe('');
	await page.goto(`${UI_ORIGIN}/runs/${fanOutRun}?ns=default`);
	await expect(page.getByTestId('task-values-merge')).toContainText('"value": "abcdef"');
	note(`merge values: ${await page.getByTestId('task-values-merge').textContent()}`);
	await shot(page, 'screenshot-each-merge.png');
});

// F-ui-perf-2: a burst of a thousand item events — what a cached 1000-item
// fan-out replays on open — is applied in frame-sized batches, the grid grows
// to every cell, and the inspector renders a window of rows and stays
// interactive. The burst is played through a mocked run socket so the scale
// does not need a thousand containers.
test('a burst of 1000 item events renders, and the open inspector stays interactive', async ({ page }) => {
	expect(fanOutRun).not.toBe('');
	const BURST = 1000;
	await page.routeWebSocket(
		(url) => url.pathname === `/ws/runs/${fanOutRun}`,
		(ws) => {
			for (let i = 0; i < BURST; i++) {
				ws.send(
					JSON.stringify({
						runId: fanOutRun,
						sequence: String(i + 1),
						type: 'TASK_LOG',
						status: 'SUCCEEDED',
						taskId: 'convert',
						item: i,
						message: `item ${i} reused a cached result`
					})
				);
			}
		}
	);
	const started = Date.now();
	await page.goto(`${UI_ORIGIN}/runs/${fanOutRun}?ns=default&events=1`);
	await expect(page.getByTestId(`task-convert-item-${BURST - 1}`)).toHaveAttribute('data-status', 'SUCCEEDED', {
		timeout: 60_000
	});
	await expect(page.getByTestId('event-count')).toHaveText(`${BURST} events`);
	const rendered = Date.now() - started;
	// The inspector draws a window of the newest rows, not all of them.
	await expect(page.getByTestId('event-entry')).toHaveCount(200);
	await expect(page.getByTestId('event-show-earlier')).toContainText('500 of 800 more');
	// Interactive: a filter answers, and Show earlier draws more.
	await page.getByTestId('event-filter-run').click();
	await expect(page.getByTestId('event-filter-empty')).toBeVisible();
	await page.getByTestId('event-filter-all').click();
	await page.getByTestId('event-show-earlier').click();
	await expect(page.getByTestId('event-entry')).toHaveCount(700);
	note(`burst of ${BURST} item events: last cell SUCCEEDED and inspector at ${BURST} events ${rendered} ms after navigation`);
	await shot(page, 'screenshot-each-burst-1000.png');
});

test('a failed item fails the run and names itself', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: FAILING,
		input: { data: { parts: ['a', 'b', 'c', 'd', 'e', 'f'] } }
	});
	const run = submitted.run.id;
	note(`submitted failing run ${run}`);
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 180_000 });
	await expect(page.getByTestId('run-error')).toContainText('item 3 of 6 failed');
	// The run's failure and the task's lead with the item's reason and keep
	// the item sentence, with no word of the execution service's wrapper.
	await expect(page.getByTestId('run-error-reason')).toHaveText('CONTAINER_EXIT_NONZERO');
	await expect(page.getByTestId('run-error-message')).toHaveText('item 3 of 6 failed: task "convert": the container exited 1');
	await expect(page.getByTestId('task-convert-error-code')).toHaveText('CONTAINER_EXIT_NONZERO');
	await expect(page.getByTestId('task-convert-error-message')).toHaveText('item 3 of 6 failed: task "convert": the container exited 1');
	for (const id of ['run-error', 'task-convert-error']) {
		for (const word of ['activity error', 'scheduledEventID', 'identity:', '(type:']) {
			await expect(page.getByTestId(id)).not.toContainText(word);
		}
	}

	// Cell 3 is red: tinted with FAILED's own classes.
	const cell = page.getByTestId('task-convert-item-3');
	await expect(cell).toHaveAttribute('aria-label', /^item 3, FAILED, attempt 1$/);
	await expect(cell).toHaveClass(/bg-failed-bg/);
	// The cells after it never started, and say so.
	for (const index of [4, 5]) {
		await expect(page.getByTestId(`task-convert-item-${index}`)).toHaveAttribute(
			'aria-label',
			new RegExp(`^item ${index}, CANCELED \\(never started\\), attempt 0$`)
		);
	}
	note(`run error: ${(await page.getByTestId('run-error').first().textContent())?.trim()}`);
	await shot(page, 'screenshot-each-failed.png');
});
