import { expect, test, type Page } from './test';
import { mkdirSync, readFileSync } from 'node:fs';
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
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('composed-orkflows');

// Composed Orkflows against a real engine, Orker, Temporal and MinIO
// (docs/DSL.md "Composition"): a composed task's card names its pinned child
// and links its child run the moment TASK_STARTED arrives, rolling the
// child's tasks up live from the child's own stream; the child's page names
// its parent and the parent lists its children; declared outputs are the run
// output and a downstream task reads them; a failed child fails the parent's
// task with its reason and a link; cancelling the parent cancels the child;
// the Orkflow page shows the child reference, its digest and the outputs; and
// apply shows composition issues on the fields they concern.
const ENGINE = 'http://127.0.0.1:18631';
const ENGINE_WS = 'ws://127.0.0.1:18631';
const UI = 'http://127.0.0.1:14631';
const TEMPORAL_NAMESPACE = temporalNamespace('composed-orkflows');
const STAMP = Date.now().toString(36);
const NS = `compose-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'composed-orkflows')
	: path.join(uiDir, 'test-results', 'composed-orkflows');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

const example = (file: string) => readFileSync(path.join(repoRoot, 'examples', file), 'utf8');

// A child whose one task fails, and a parent composing it.
const failingBlock = `apiVersion: orkstack/v1
kind: Orkflow
name: failing-block
version: "0.1.0"
outputs:
  digest: '{{ .tasks.attest.output.value }}'
tasks:
  - id: attest
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo attest failed >&2; exit 1"]
`;
const failingParent = `apiVersion: orkstack/v1
kind: Orkflow
name: composed-failing
version: "0.1.0"
tasks:
  - id: release
    uses: orkflow:failing-block@0.1.0
`;
// A child that sleeps until it is cancelled, and a parent composing it.
const slowBlock = `apiVersion: orkstack/v1
kind: Orkflow
name: slow-block
version: "0.1.0"
tasks:
  - id: wait
    uses: shell
    timeout_seconds: 600
    with:
      op: exec
      command: ["sh", "-c", "sleep 600"]
`;
const slowParent = `apiVersion: orkstack/v1
kind: Orkflow
name: composed-slow
version: "0.1.0"
tasks:
  - id: release
    uses: orkflow:slow-block@0.1.0
`;

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18631, 14631]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ORK_HTTP_ADDR: '127.0.0.1:18631', ORK_WS_ALLOWED_ORIGINS: UI
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('composed-orkflows') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14631', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'composed orkflows' } });
	// The child first: a composed task resolves its child at apply.
	for (const yaml of [example('release-block.ork.yaml'), example('composed-release.ork.yaml'), failingBlock, failingParent, slowBlock, slowParent]) {
		await rpc('OrkflowService/ApplyOrkflow', { yaml, namespace: NS });
	}
	note(`namespace ${NS}, orkflows release-block@0.1.0 and composed-release@0.1.0, engine ${ENGINE}, ui ${UI}`);
});

// Cleanup stops exactly the processes this spec started, by handle
// (killStarted), never by name or pattern.
test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

async function submit(name: string): Promise<string> {
	const resp = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: name, namespace: NS });
	return resp.run.id;
}

async function childRunId(page: Page, taskId: string): Promise<string> {
	const link = page.getByTestId(`task-${taskId}-child-link`);
	await expect(link).toBeVisible({ timeout: 120_000 });
	return ((await link.textContent()) ?? '').trim();
}

test('a composed task links to its child run and rolls up its tasks live', async ({ page }) => {
	// F-ui-perf-9: a composed card reads its child and opens its socket only
	// once it is on screen. A short viewport keeps the card below the fold
	// until it is scrolled to.
	await page.setViewportSize({ width: 1280, height: 360 });
	const childReads: string[] = [];
	const childSockets: string[] = [];
	page.on('request', (request) => {
		if (request.url().endsWith('/orkstack.v1.RunService/GetRun')) {
			childReads.push(JSON.parse(request.postData() ?? '{}').runId ?? '');
		}
	});
	page.on('websocket', (socket) => childSockets.push(socket.url()));
	const runId = await submit('composed-release');
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('task-release-child')).toBeAttached();
	await expect(page.getByTestId('task-release-child-orkflow')).toHaveText('release-block@0.1.0');
	const link = page.getByTestId('task-release-child-link');
	await expect(link).toBeAttached({ timeout: 120_000 });
	const childId = ((await link.textContent()) ?? '').trim();
	expect(childId).toMatch(/^ork-\d+-\d+$/);
	await expect(page.getByTestId('task-release-child')).not.toBeInViewport();
	expect(childReads.filter((id) => id === childId), 'no child read before the card is seen').toHaveLength(0);
	expect(childSockets.filter((url) => url.includes(`/ws/runs/${childId}?`)), 'no child socket before the card is seen').toHaveLength(0);
	await expect(page.getByTestId('task-release-child')).toHaveAttribute('data-following', 'idle');
	await page.getByTestId('task-release-child').scrollIntoViewIfNeeded();
	await expect(page.getByTestId('task-release-child')).toHaveAttribute('data-following', 'read', { timeout: 30_000 });
	expect(childReads.filter((id) => id === childId).length >= 1).toBe(true);
	expect(childSockets.filter((url) => url.includes(`/ws/runs/${childId}?`)).length <= 1).toBe(true);
	note(`composed card below the fold: 0 child reads and 0 child sockets until scrolled to; then ${childReads.filter((id) => id === childId).length} read(s)`);
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.getByTestId('task-release-child').scrollIntoViewIfNeeded();
	await expect(page.getByTestId('task-release-child-task-sign-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('task-release-child-task-publish-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('task-release-child-rollup')).toHaveText('2 tasks: 2 succeeded');
	await expect(page.getByTestId('task-release-child-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note(`run ${runId}: release started child ${childId}; the roll-up turned 2 tasks SUCCEEDED`);
	await shot(page, 'screenshot-child-rollup.png');
});

test('the child run names its parent and the parent lists its children', async ({ page }) => {
	const runId = await submit('composed-release');
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	const childId = await childRunId(page, 'release');
	await page.getByTestId('task-release-child-link').click();
	await page.waitForURL(new RegExp(`/runs/${childId}`));
	await expect(page.getByTestId('run-origin-kind')).toHaveText('parent-run');
	await expect(page.getByTestId('run-origin-link')).toHaveText(runId);
	await shot(page, 'screenshot-child-origin.png');
	await page.getByTestId('run-origin-link').click();
	await page.waitForURL(new RegExp(`/runs/${runId}`));
	// O1: Child runs is the runs list filtered to this run's children, and
	// its CLI equivalent is the same two filters.
	await page.getByTestId('run-children-cli-trigger').click();
	await expect(page.getByTestId('run-children-cli-command')).toContainText(
		`run list --filter origin.kind=PARENT_RUN --filter origin.id=${runId}`
	);
	await page.keyboard.press('Escape');
	await page.getByTestId('run-children-link').click();
	await page.waitForURL(/\/runs\?/);
	expect(new URL(page.url()).searchParams.getAll('filter')).toEqual([`origin.id=${runId}`, 'origin.kind=PARENT_RUN']);
	await expect(page.getByText(childId)).toBeVisible({ timeout: 30_000 });
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(1);
	note(`child ${childId} names parent ${runId}; the parent's Child runs list shows it`);
	await shot(page, 'screenshot-children-list.png');
});

test('declared outputs are the run output and a downstream task read them', async ({ page }) => {
	const runId = await submit('composed-release');
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('run-output-kind')).toHaveText('Declared outputs');
	await expect(page.getByTestId('run-output')).toContainText('"digest": "sha256:', { timeout: 60_000 });
	const output = (await page.getByTestId('run-output').textContent()) ?? '';
	const digest = /"digest": "(sha256:[0-9a-f]{64})"/.exec(output)?.[1] ?? '';
	expect(digest).not.toBe('');
	await page.getByTestId('task-log-toggle-announce').click();
	await expect(page.getByTestId('task-log-announce')).toContainText(`released ${digest} at https://registry.example/records/${digest}`, {
		timeout: 60_000
	});
	note(`run ${runId}: Run.output digest ${digest}; announce printed it`);
	await shot(page, 'screenshot-declared-outputs.png');
});

test('a failed child fails the parent task with its reason and a link', async ({ page }) => {
	const runId = await submit('composed-failing');
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 120_000 });
	await expect(page.getByTestId('task-release-error')).toContainText('CHILD_RUN_FAILED');
	await expect(page.getByTestId('task-release-error')).toContainText('(failing-block@0.1.0) failed');
	await expect(page.getByTestId('task-release-child-link')).toBeVisible();
	await page.getByTestId('task-release-child').scrollIntoViewIfNeeded();
	await expect(page.getByTestId('task-release-child-task-attest-status')).toHaveText('FAILED', { timeout: 60_000 });
	note(`run ${runId}: release FAILED CHILD_RUN_FAILED naming failing-block@0.1.0`);
	await shot(page, 'screenshot-child-failed.png');
});

test('cancelling the parent cancels the child', async ({ page }) => {
	const runId = await submit('composed-slow');
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	const childId = await childRunId(page, 'release');
	await page.getByTestId('task-release-child').scrollIntoViewIfNeeded();
	await expect(page.getByTestId('task-release-child-task-wait-status')).toHaveText('RUNNING', { timeout: 120_000 });
	await page.getByTestId('cancel-run').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	await page.getByTestId('cancel-run-accept').click();
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED', { timeout: 120_000 });
	await expect(page.getByTestId('task-release-status')).toHaveText('CANCELED');
	const child = await rpc<{ run: { status: string } }>('RunService/GetRun', { runId: childId, namespace: NS });
	expect(child.run.status).toBe('STATUS_CANCELED');
	note(`run ${runId} canceled; child ${childId} ended ${child.run.status}`);
	await shot(page, 'screenshot-cancel-parent.png');
});

test('the Orkflow page shows the child reference, its digest and the outputs', async ({ page }) => {
	await page.goto(`${UI}/orkflows/composed-release?ns=${NS}`);
	await expect(page.getByTestId('task-child-link-release')).toHaveText('orkflow release-block@0.1.0');
	await expect(page.getByTestId('task-child-digest-release')).toHaveText(/^sha256:[0-9a-f]{12}…[0-9a-f]{6}$/);
	await expect(page.getByTestId('dag-node-release-orkflow')).toHaveText('orkflow release-block@0.1.0');
	await expect(page.getByTestId('orkflow-output-digest')).toContainText('{{ .tasks.release.output.digest }}');
	await shot(page, 'screenshot-orkflow-page.png');
	await page.getByTestId('task-child-link-release').click();
	await page.waitForURL(/\/orkflows\/release-block/);
	await expect(page.getByTestId('orkflow-output-url')).toBeVisible();
	await page.goto(`${UI}/orkflows/failing-block?ns=${NS}`);
	await expect(page.getByTestId('orkflow-output-digest')).toBeVisible();
	await page.goto(`${UI}/orkflows/slow-block?ns=${NS}`);
	await expect(page.getByTestId('orkflow-outputs-none')).toBeVisible();
});

test('apply shows composition issues on the uses line', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill(`apiVersion: orkstack/v1
kind: Orkflow
name: composed-issues
version: "1.0.0"
tasks:
  - id: release
    uses: orkflow:release-block@9.9.9
`);
	await page.getByTestId('apply-validate').click();
	const unknown = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].uses"]');
	await expect(unknown).toHaveAttribute('data-severity', 'error');
	await expect(unknown).toContainText('has no version "9.9.9"');
	await expect(unknown).toContainText('(versions: 0.1.0 (current))');

	await page.getByTestId('apply-yaml').fill(`apiVersion: orkstack/v1
kind: Orkflow
name: composed-issues
version: "1.0.0"
tasks:
  - id: release
    uses: orkflow:release-block@0.1.0
    with:
      image: x
      extra: why
  - id: after
    uses: shell
    needs: [release]
    with:
      op: exec
      command: ["echo", "{{ .tasks.release.output.digets }}"]
`);
	await page.getByTestId('apply-validate').click();
	const undeclared = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].with.extra"]');
	await expect(undeclared).toHaveAttribute('data-severity', 'warning');
	await expect(undeclared).toContainText('is not an input of release-block@0.1.0');
	const output = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[1].with.command[1]"]');
	await expect(output).toHaveAttribute('data-severity', 'error');
	await expect(output).toContainText('declares no output "digets" (outputs: digest, url)');
	await shot(page, 'screenshot-apply-composition-issues.png');
});
