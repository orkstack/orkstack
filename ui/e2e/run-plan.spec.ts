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
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('run-plan');

// The submit dialog's Plan action, in a browser, against the real stack.
//
// A plan answers "which tasks of this Orkflow would run for this input, and
// why the others would not" WITHOUT starting anything. What is measured here:
//
//   P1. with a declared boolean set false in the typed editor, Plan shows the
//       gated task `skipped` with the interpreter's own condition sentence and
//       its dependant `skipped` because its upstream was;
//   P2. flipping the input clears the previous table (a plan describes ONE
//       draft), and planning again shows every task `runs`;
//   P3. the plan's CLI equivalent is `orkctl run plan <name> --namespace <ns>`
//       carrying the same input JSON;
//   P4. none of that started a run;
//   P5. submitting the P2 draft produces a run whose every task's status is
//       what the P2 plan said — the browser-visible half of the CLI's
//       agreement test.
//
// Topology: one engine, one Orker, the production UI build, on ports of this
// spec's own and a Temporal namespace of its own. Only the processes this
// spec started are ever stopped (killStarted, by handle).

const ENGINE = 'http://127.0.0.1:18417';
const ENGINE_WS = 'ws://127.0.0.1:18417';
const UI_ORIGIN = 'http://127.0.0.1:14417';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE
	? `${process.env.ORK_TEMPORAL_NAMESPACE}-run-plan`
	: 'ork-ui-run-plan';
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const FLOW = `run-plan-ui-${STAMP}`;
const NS = 'default';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-plan')
	: path.join(uiDir, 'test-results', 'run-plan');
const TRANSCRIPT = 'run-plan-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// build always runs; deploy is gated on a declared boolean; smoke needs deploy,
// so it is skipped exactly when deploy is — the upstream-skip sentence.
function planOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${FLOW}
version: "0.1.0"
description: The submit dialog's Plan action.
inputs:
  deploy:
    type: boolean
    default: true
tasks:
  - id: build
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo build"]
  - id: deploy
    uses: shell
    needs: [build]
    condition: '{{ .inputs.deploy }}'
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo deploy"]
  - id: smoke
    uses: shell
    needs: [deploy]
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo smoke"]
`;
}

async function runsOfFlow(): Promise<number> {
	const answer = await rpc<{ runs?: unknown[] }>('RunService/ListRuns', {
		filters: [{ field: 'orkflow_name', op: 'FILTER_OPERATOR_EQUALS', values: [FLOW] }],
		namespace: NS
	});
	return (answer.runs ?? []).length;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The submit dialog Plan action: a plan per draft, cleared when the',
			'# draft changes, a CLI equivalent, no run started, and a real run that',
			'# agrees with the plan task for task.',
			'#',
			'# Written by ui/e2e/run-plan.spec.ts. Every line below was produced by',
			'# the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${FLOW} (ork namespace: ${NS})`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18417, 14417]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18417',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('run-plan')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14417',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: planOrkflow() });
	note(`applied ${FLOW}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('Plan answers per draft, starts nothing, and a real run agrees with it', async ({ page }) => {
	const runsBefore = await runsOfFlow();
	note(`runs of ${FLOW} before planning: ${runsBefore}`);

	await page.goto(`${UI_ORIGIN}/orkflows/${FLOW}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('declared-input-deploy')).toBeChecked();

	// P1: the declared boolean off, in the typed editor, then Plan.
	await page.getByTestId('declared-input-deploy').uncheck();
	await page.getByTestId('run-submit-plan').click();
	await expect(page.getByTestId('run-plan-table')).toBeVisible();
	await expect(page.getByTestId('run-plan-row-build-outcome')).toHaveText('runs');
	await expect(page.getByTestId('run-plan-row-deploy-outcome')).toHaveText('skipped');
	await expect(page.getByTestId('run-plan-row-deploy-detail')).toHaveText(
		'condition "{{ .inputs.deploy }}" evaluated false'
	);
	await expect(page.getByTestId('run-plan-row-smoke-outcome')).toHaveText('skipped');
	await expect(page.getByTestId('run-plan-row-smoke-detail')).toHaveText(
		'upstream task "deploy" was skipped'
	);
	await expect(page.getByTestId('run-plan-summary')).toHaveText(
		'1 run, 2 skipped, 1 wave. Nothing was started.'
	);
	// Rows in the CLI's order: wave, then declaration.
	const rowOrder = await page
		.getByTestId('run-plan-table')
		.locator('tbody tr')
		.evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')));
	expect(rowOrder).toEqual(['run-plan-row-build', 'run-plan-row-deploy', 'run-plan-row-smoke']);
	note(`P1 summary: ${await page.getByTestId('run-plan-summary').textContent()}`);
	note(`P1 deploy: ${await page.getByTestId('run-plan-row-deploy-detail').textContent()}`);
	note(`P1 smoke: ${await page.getByTestId('run-plan-row-smoke-detail').textContent()}`);
	await shot(page, 'plan-skipped.png');

	// An assumed output that is not an object is refused in place, and blocks
	// Plan but not Submit: a submission never sends assumed outputs.
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-assumed-outputs').fill('{"build": 1}');
	await expect(page.getByTestId('run-submit-assumed-outputs-error')).toHaveText(
		'Assumed output for task "build" must be a JSON object, as a values.json is.'
	);
	await expect(page.getByTestId('run-submit-plan')).toBeDisabled();
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByTestId('run-submit-assumed-outputs').fill('{}');
	await expect(page.getByTestId('run-submit-assumed-outputs-error')).toHaveCount(0);

	// P2: flipping the input clears the table (it answered another draft);
	// planning again answers the new one.
	await page.getByTestId('run-submit-plan').click();
	await expect(page.getByTestId('run-plan-table')).toBeVisible();
	await page.getByTestId('declared-input-deploy').check();
	await expect(page.getByTestId('run-plan-table')).toHaveCount(0);
	note('P2 the P1 table disappeared when deploy was flipped');
	await page.getByTestId('run-submit-plan').click();
	for (const id of ['build', 'deploy', 'smoke']) {
		await expect(page.getByTestId(`run-plan-row-${id}-outcome`)).toHaveText('runs');
	}
	await expect(page.getByTestId('run-plan-summary')).toHaveText(
		'3 run, 0 skipped, 3 waves. Nothing was started.'
	);
	await expect(page.getByTestId('run-plan-row-smoke-wave')).toHaveText('3');
	note(`P2 summary: ${await page.getByTestId('run-plan-summary').textContent()}`);
	await shot(page, 'plan-runs.png');

	// P3: the plan's CLI equivalent.
	await page.getByTestId('run-submit-plan-cli-trigger').click();
	const command = page.getByTestId('run-submit-plan-cli-command');
	await expect(command).toContainText(`orkctl run plan '${FLOW}' --namespace '${NS}'`);
	await expect(command).toContainText(`'{"deploy":true}' > input.json`);
	// The Orkflow page's dialog pins the version the page shows, so the copied
	// command plans exactly that version rather than whatever `current` is.
	await expect(command).toContainText(`--version '0.1.0'`);
	await expect(command).not.toContainText('outputs.json');
	note(`P3 CLI: ${(await command.textContent())?.replace(/\s+/g, ' ').trim()}`);
	await page.keyboard.press('Escape');

	// P4: nothing above started a run.
	const runsAfterPlans = await runsOfFlow();
	expect(runsAfterPlans).toBe(runsBefore);
	note(`P4 runs of ${FLOW} after three plans: ${runsAfterPlans} (before: ${runsBefore})`);

	// P5: submit the P2 draft; every task ends where the P2 plan said. The
	// plan read here is the one the dialog is still showing — the page's own
	// answer, not a second request.
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	const planned: Record<string, string> = {};
	for (const id of ['build', 'deploy', 'smoke']) {
		planned[id] = ((await page.getByTestId(`run-plan-row-${id}-outcome`).textContent()) ?? '').trim();
	}
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	for (const [id, outcome] of Object.entries(planned)) {
		const want = outcome === 'runs' ? 'SUCCEEDED' : 'SKIPPED';
		await expect(page.getByTestId(`task-${id}-status`)).toHaveText(new RegExp(`^\\s*${want}\\s*$`));
		note(`P5 ${id}: planned ${outcome}, ran ${want}`);
	}
	await shot(page, 'plan-then-run.png');
});
