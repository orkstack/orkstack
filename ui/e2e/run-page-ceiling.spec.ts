import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
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

// The run detail page at the task ceiling (spec E7, the run-page case of
// T7): the deployment's default engine.orkflow_max_tasks is the largest size
// at which, among the task soak's other thresholds, the run page of a flat
// stage of that many tasks becomes interactive within 5 s, live and finished.
//
// It is a file of its own, beside run-page.spec.ts, for one reason: it runs
// without Playwright's trace. A trace snapshots the whole DOM at every
// action, which on a page of thousands of task cards is seconds of the
// test's own work counted against the page, and a trace setting can only be
// changed for a whole file.
//
// Topology: one engine, one Orker, the production UI build, a Temporal
// namespace of its own, and a stamped Orkflow name.

test.use({ trace: 'off' });
test.describe.configure({ mode: 'serial' });

const ENGINE = 'http://127.0.0.1:18471';
const ENGINE_WS = 'ws://127.0.0.1:18471';
const UI_ORIGIN = 'http://127.0.0.1:14471';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-page-ceiling';

const STAMP = Date.now().toString(36);
const GATED_ORKFLOW = `run-page-gated-${STAMP}`;

// The gated task size: the deployment default engine.orkflow_max_tasks,
// which the task soak measured (ORK_SOAK_TASKS selects a fallback size, as
// it does for the soak). The engine below runs at its defaults, so the
// ceiling it applies is this size.
const GATED_TASKS = Number(process.env.ORK_SOAK_TASKS ?? 4000);
// E7: the run page is interactive within 5 s, live and finished.
const GATED_RENDER_BUDGET_MS = 5_000;
// T2 (e)'s output-maximal rows: every task's inline output at the 4 KiB
// values bound.
const OUTPUT_MAXIMAL_BYTES = 4096;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page-ceiling')
	: path.join(uiDir, 'test-results', 'run-page-ceiling');
const TRANSCRIPT = 'run-page-ceiling-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// gatedOrkflow is the task soak's flat-short shape at the gated size: every
// task needs nothing and runs the shortest container there is.
function gatedOrkflow(): string {
	const lines = ['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${GATED_ORKFLOW}`, 'version: "0.1.0"', 'description: A flat stage at the gated task size.', 'tasks:'];
	for (let i = 0; i < GATED_TASKS; i++) {
		lines.push(`  - id: t${i}`, '    uses: shell', '    network: none', '    with: {op: exec, command: [echo, ok]}');
	}
	return lines.join('\n') + '\n';
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The run detail page at the task ceiling: interactive within 5 s,',
			'# live and finished, and the output-maximal rows quoted.',
			'#',
			'# Written by ui/e2e/run-page-ceiling.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${GATED_ORKFLOW} (${GATED_TASKS} tasks, ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);
	// Prerequisites, stated as failures rather than skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18471, 14471]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18471',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ...orkerEnv('run-page-ceiling') });
	await waitForEngine(ENGINE);
	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14471', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: gatedOrkflow() });
	note(`applied ${GATED_ORKFLOW} (${GATED_TASKS} tasks)`);
});

test.afterAll(async () => {
	await killStarted();
});

// measureRunPage opens a run's page and times it to interactive: its header,
// every one of its task cards drawn, the last of them in view, and the page
// answering a click (the last card's Inspect opens). It returns the
// milliseconds from navigation.
async function measureRunPage(page: Page, runId: string, tasks: number, phase: string): Promise<number> {
	const started = Date.now();
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-status')).toBeVisible({ timeout: 60_000 });
	const header = Date.now() - started;
	// The last card drawn is every card drawn (the list renders in order);
	// they are counted once, after, rather than polled: a locator polling a
	// count over thousands of cards is the test's own load on the page.
	const last = page.getByTestId(`task-t${tasks - 1}`);
	await last.scrollIntoViewIfNeeded({ timeout: 60_000 });
	await expect(last).toBeVisible();
	const cards = Date.now() - started;
	await page.getByTestId(`task-inspect-toggle-t${tasks - 1}`).click();
	await expect(page.getByTestId(`task-t${tasks - 1}-inspect`)).toBeVisible({ timeout: 60_000 });
	const elapsed = Date.now() - started;
	expect(await page.getByTestId('task-list').locator('[data-testid^="task-deep-link-"]').count()).toBe(tasks);
	note(
		`${phase}: run page of ${runId} (${tasks} tasks) interactive ${elapsed} ms after navigation ` +
			`(header ${header} ms, every card ${cards} ms)`
	);
	// The viewport, not the full page: a page of thousands of cards is taller
	// than a screenshot may be.
	await page.screenshot({ path: outPath(`screenshot-run-page-gated-${phase}.png`) });
	return elapsed;
}

// E7: the run page of the gated-size flat-short run is interactive within
// 5 s, live — every card drawn while its tasks start — and again once the
// run has finished (canceled here: a real container per task would keep one
// Orker busy for an hour at this size). The output-maximal run of T2 (e) at
// the same size, its rows archived by test/tools/archive-sized-run, is
// opened too, and its time quoted, not gated.
test('the run page at the gated task size is interactive within 5 s, live and finished', async ({ page }) => {
	test.setTimeout(30 * 60_000);
	const run = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: GATED_ORKFLOW })).run.id;
	note(`submitted ${GATED_ORKFLOW} (${GATED_TASKS} tasks) as run ${run}`);
	await waitFor(
		`run ${run} is running tasks`,
		async () => {
			const answer = await rpc<{ run?: { taskRuns?: { status?: string }[] } }>('RunService/GetRun', { runId: run });
			const rows = answer.run?.taskRuns ?? [];
			return (
				rows.length === GATED_TASKS &&
				rows.some((row) => row.status === 'STATUS_RUNNING' || row.status === 'STATUS_SUCCEEDED')
			);
		},
		300_000
	);

	const live = await measureRunPage(page, run, GATED_TASKS, 'live');
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING');

	await rpc('RunService/CancelRun', { runId: run, reason: 'run-page-ceiling e2e: gated-size render measured' });
	// Finished means its last snapshot: the run reads CANCELED as soon as the
	// cancel is decided, and its thousands of rows reach their own ends over
	// the minutes the run takes to record them.
	const terminal = ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED', 'STATUS_SKIPPED'];
	await waitFor(
		`run ${run} finished, every task row with it`,
		async () => {
			const answer = await rpc<{ run?: { status?: string; taskRuns?: { status?: string }[] } }>('RunService/GetRun', {
				runId: run
			});
			const rows = answer.run?.taskRuns ?? [];
			return (
				answer.run?.status === 'STATUS_CANCELED' &&
				rows.length === GATED_TASKS &&
				rows.every((row) => terminal.includes(row.status ?? ''))
			);
		},
		900_000
	);
	const finished = await measureRunPage(page, run, GATED_TASKS, 'finished');
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED');

	// The output-maximal rows, quoted only.
	const definition = await rpc<{ orkflow?: { definitionDigest?: string } }>('OrkflowService/GetOrkflow', {
		name: GATED_ORKFLOW
	});
	const maximal = `ork-${Date.now()}-output-maximal`;
	const archived = execFileSync(
		'go',
		[
			'run',
			'./test/tools/archive-sized-run',
			'default',
			GATED_ORKFLOW,
			'0.1.0',
			definition.orkflow?.definitionDigest ?? '',
			maximal,
			String(GATED_TASKS),
			String(OUTPUT_MAXIMAL_BYTES)
		],
		{ cwd: repoRoot, env: { ...process.env, ...s3Env } }
	).toString();
	note(archived.trim());
	const outputMaximal = await measureRunPage(page, maximal, GATED_TASKS, 'output-maximal');
	writeFileSync(
		outPath('run-page-gated.json'),
		JSON.stringify(
			{
				tasks: GATED_TASKS,
				budget_ms: GATED_RENDER_BUDGET_MS,
				live_ms: live,
				finished_ms: finished,
				output_maximal_ms: outputMaximal,
				output_maximal_inline_bytes: OUTPUT_MAXIMAL_BYTES
			},
			null,
			2
		) + '\n'
	);
	note(
		`E7 run page at ${GATED_TASKS} tasks: live ${live} ms, finished ${finished} ms (budget ${GATED_RENDER_BUDGET_MS} ms); ` +
			`output-maximal ${outputMaximal} ms (quoted)`
	);
	expect(live, 'live run page').toBeLessThanOrEqual(GATED_RENDER_BUDGET_MS);
	expect(finished, 'finished run page').toBeLessThanOrEqual(GATED_RENDER_BUDGET_MS);
});
