import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
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
const BUCKET = ownBucket('each-fan-out-ceiling');

// A fan-out (`each:`) at the item ceiling on the run detail page, in a
// browser, against the real stack (docs/DSL.md "Fan-out"): spec E4, the run
// page and the fan-out's item list interactive within 5 s.
//
// It is a file of its own, beside each-fan-out.spec.ts (the fan-out's
// functional tests), for one reason: a render budget is a measurement of a
// quiet host, so this file runs ALONE (ui/playwright.config.ts, `projects`),
// and only the test that asserts the budget should pay for running alone.
//
// Topology: one engine, one Orker, the production UI build. Isolation per
// AGENTS.md §11.1 — ports, a bucket, a Temporal namespace and an Orker work
// root of this file's own, and a stamped Orkflow name.

// Ports of this spec's own, so the whole e2e directory can run in one go.
const ENGINE = 'http://127.0.0.1:18598';
const ENGINE_WS = 'ws://127.0.0.1:18598';
const UI_ORIGIN = 'http://127.0.0.1:14598';

const TEMPORAL_NAMESPACE = temporalNamespace('each-fan-out-ceiling');

const STAMP = Date.now().toString(36);
const GATED = `each-fan-out-gated-${STAMP}`;

// The gated size: the built-in item maximum (internal/dsl MaxEachItems),
// which the fan-out soak measured (ORK_SOAK_ITEMS selects a fallback size, as
// it does for the soak). The engine below raises ORK_EACH_MAX_ITEMS to it,
// with continuation on by default.
const GATED_ITEMS = Number(process.env.ORK_SOAK_ITEMS ?? 10000);
// E4: the run page and the fan-out's item list are interactive within 5 s.
const GATED_RENDER_BUDGET_MS = 5_000;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'each-fan-out-ceiling')
	: path.join(uiDir, 'test-results', 'each-fan-out-ceiling');
const TRANSCRIPT = 'each-fan-out-ceiling-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// GATED_ITEMS items over a list a json task computes, two at a time: the
// grid holds every cell from the moment the fan-out expands, however few
// items have run.
function gatedOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${GATED}
version: "0.1.0"
description: A fan-out at the gated item size.

tasks:
  - id: list
    uses: json
    network: none
    with: {op: query, nullInput: true, filter: "[range(0; ${GATED_ITEMS})]"}

  - id: spread
    uses: shell
    needs: [list]
    network: none
    timeout_seconds: 120
    each:
      over: '{{ .tasks.list.output.result }}'
      max_items: ${GATED_ITEMS}
      max_parallel: 2
    with: {op: exec, command: [echo, "{{ .each.item }}"]}
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# A fan-out at the gated item size on the run detail page: the run',
			'# page and the item list interactive within 5 s, live and finished.',
			'#',
			'# Written by ui/e2e/each-fan-out-ceiling.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${GATED} (${GATED_ITEMS} items, ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	// Prerequisites, stated as failures rather than skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18598, 14598]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18598',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN,
		ORK_EACH_MAX_ITEMS: String(GATED_ITEMS)
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('each-fan-out-ceiling')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14598',
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

	await rpc('OrkflowService/ApplyOrkflow', { yaml: gatedOrkflow() });
	note(`applied ${GATED}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test.describe.configure({ mode: 'serial' });

// E4: at the gated item size, the run page and the fan-out's item list each
// become interactive within 5 s — live, with every cell drawn and two items
// at a time running, and again once the run has finished (canceled here: a
// real container per item would take hours at this size on one Orker).
test('the run page and the item list at the gated size are interactive within 5 s', async ({ page }) => {
	test.setTimeout(10 * 60_000);
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: GATED });
	const run = submitted.run.id;
	note(`submitted ${GATED} (${GATED_ITEMS} items) as run ${run}`);
	await waitFor(`run ${run}'s fan-out expanded`, async () => {
		const answer = await rpc<{ run?: { taskRuns?: { taskId: string; each?: { expanded?: boolean } }[] } }>(
			'RunService/GetRun',
			{ runId: run }
		);
		return (answer.run?.taskRuns ?? []).some((row) => row.taskId === 'spread' && row.each?.expanded);
	});

	const last = GATED_ITEMS - 1;
	const measure = async (phase: string) => {
		const started = Date.now();
		await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
		await expect(page.getByTestId('run-status')).toBeVisible({ timeout: 30_000 });
		const pageReady = Date.now() - started;
		const cell = page.getByTestId(`task-spread-item-${last}`);
		await expect(cell).toBeVisible({ timeout: 30_000 });
		await cell.click();
		await expect(cell).toHaveAttribute('aria-pressed', 'true', { timeout: 30_000 });
		await expect(page.getByTestId('task-spread-item-panel-label')).toHaveText(`spread[${last}]`, {
			timeout: 30_000
		});
		const listReady = Date.now() - started;
		await expect(page.getByTestId('task-spread-item-grid').locator('button[data-item]')).toHaveCount(GATED_ITEMS);
		note(
			`${phase}: run page interactive ${pageReady} ms, item list of ${GATED_ITEMS} interactive ` +
				`(item ${last} selected) ${listReady} ms after navigation`
		);
		await shot(page, `screenshot-each-gated-${phase}.png`);
		expect(pageReady, `${phase} run page`).toBeLessThanOrEqual(GATED_RENDER_BUDGET_MS);
		expect(listReady, `${phase} item list`).toBeLessThanOrEqual(GATED_RENDER_BUDGET_MS);
	};

	await measure('live');
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING');

	await rpc('RunService/CancelRun', { runId: run, reason: 'each-fan-out e2e: gated-size render measured' });
	await waitFor(`run ${run} finished`, async () => {
		const answer = await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId: run });
		return answer.run?.status === 'STATUS_CANCELED';
	});
	await measure('finished');
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED');

	// The task map, outside the timed span (UI revamp, stage 2): its section
	// of the fan-out drew every item's cell after the two tasks' own.
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default&view=map`);
	const map = page.getByTestId('task-map');
	await expect(map).toHaveAttribute('data-painted', 'true', { timeout: 30_000 });
	await expect(map).toHaveAttribute('data-cells', String(2 + GATED_ITEMS));
	await expect(page.getByTestId('task-map-section-spread')).toContainText(
		`${new Intl.NumberFormat('en-US').format(GATED_ITEMS)} items`
	);
	note(`task map: ${await map.getAttribute('data-cells')} cells painted, the spread section ${GATED_ITEMS} items`);
	await shot(page, 'screenshot-each-gated-map.png');
});
