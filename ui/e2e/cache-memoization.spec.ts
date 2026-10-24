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
const BUCKET = ownBucket('cache-memoization');

// Task caching against a real engine, Orker, Temporal and MinIO (docs/DSL.md
// "Caching"): a second identical run shows its
// cached task linking to the run that produced it; a miss says why, and the
// attempt timeline shows the decision and its key components; Ignore cached
// results bypasses every cached task; Admin → Cache shows the hit rate and
// purges memos; a cached fan-out marks its cached items.
const ENGINE = 'http://127.0.0.1:18741';
const ENGINE_WS = 'ws://127.0.0.1:18741';
const UI = 'http://127.0.0.1:14741';
const TEMPORAL_NAMESPACE = temporalNamespace('cache-memoization');
const STAMP = Date.now().toString(36);
const NS = `cache-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'cache-memoization')
	: path.join(uiDir, 'test-results', 'cache-memoization');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

const example = (file: string) => readFileSync(path.join(repoRoot, 'examples', file), 'utf8');
// Every scenario gets its own stamped Orkflow, so one test's memos never feed
// another's.
const pipeline = (name: string) => example('cached-pipeline.ork.yaml').replace('name: cached-pipeline', `name: ${name}`);
// The fan-out over a run input, its convert cached (activity-shell writes its
// own values.json, so the array comes from the input, as each-fan-out.spec.ts
// does).
const cachedFanOut = (name: string) => `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "0.1.0"
inputs:
  parts:
    type: array
tasks:
  - id: convert
    uses: shell
    cache: { enabled: true }
    each:
      over: '{{ .inputs.parts }}'
      as: part
      max_items: 10
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        printf '%s\\n' {{ sh .each.part }} > /orkstack/output/artifacts/part.txt
`;

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18741, 14741]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ORK_HTTP_ADDR: '127.0.0.1:18741', ORK_WS_ALLOWED_ORIGINS: UI
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('cache-memoization') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14741', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'task caching' } });
	note(`namespace ${NS}, engine ${ENGINE}, ui ${UI}`);
});

// Cleanup stops exactly the processes this spec started, by PID: never a
// name-pattern sweep that could reach a process this spec does not own.
test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

async function apply(yaml: string): Promise<void> {
	await rpc('OrkflowService/ApplyOrkflow', { yaml, namespace: NS });
}

type RunView = { run: { status: string; taskRuns?: { taskId: string; cache?: { outcome?: string } }[] } };

/** submit starts one run through the API and waits until it SUCCEEDED. */
async function submitAndWait(name: string, input: Record<string, unknown> = {}, noCache = false): Promise<string> {
	const resp = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: name, namespace: NS, input: { data: input }, noCache
	});
	const runId = resp.run.id;
	await waitFor(`run ${runId} succeeds`, async () => {
		const got = await rpc<RunView>('RunService/GetRun', { runId, namespace: NS });
		if (got.run.status === 'STATUS_FAILED' || got.run.status === 'STATUS_CANCELED') throw new Error(`run ${runId} ${got.run.status}`);
		return got.run.status === 'STATUS_SUCCEEDED';
	}, 180_000);
	return runId;
}

test('a second run shows the cached task and links to the run that produced it', async ({ page }) => {
	const name = `cached-p1-${STAMP}`;
	await apply(pipeline(name));
	const first = await submitAndWait(name);
	const second = await submitAndWait(name);
	await page.goto(`${UI}/runs/${second}?ns=${NS}`);
	const badge = page.getByTestId('task-featurize-cache');
	await expect(badge).toHaveAttribute('data-outcome', 'hit', { timeout: 60_000 });
	await expect(page.getByTestId('task-summary-cached')).toContainText('· 1 cached');
	await expect(page.getByTestId('task-featurize-cache-source')).toHaveText(first);
	await page.getByTestId('task-inspect-toggle-featurize').click();
	await expect(page.getByTestId('task-featurize-cache-key')).toHaveText(/^[0-9a-f]{64}$/);
	await expect(page.getByTestId('task-featurize-cache-source-commit')).toContainText(first);
	await page.getByTestId('task-log-toggle-featurize').click();
	await expect(page.getByTestId('task-log-featurize')).toContainText('[ork] cache hit', { timeout: 60_000 });
	await expect(page.getByTestId('task-log-featurize')).not.toContainText('featurized 1000 rows');
	await shot(page, 'screenshot-p1-cached-task.png');
	await page.getByTestId('task-featurize-cache-source').click();
	await page.waitForURL(new RegExp(`/runs/${first}`));
	note(`P1: run ${second} featurize Cached from ${first}; the source link lands on run 1`);
});

test('a cache miss says why, and the attempt timeline shows it', async ({ page }) => {
	const name = `cached-p2-${STAMP}`;
	await apply(pipeline(name));
	const runId = await submitAndWait(name);
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	const badge = page.getByTestId('task-train-cache');
	await expect(badge).toHaveAttribute('data-outcome', 'miss', { timeout: 60_000 });
	await expect(badge).toHaveText('Cache miss');
	await expect(badge).toHaveAttribute('title', /No memo exists for this key/);
	await page.getByTestId('task-inspect-toggle-train').click();
	await expect(page.getByTestId('task-train-cache-reason')).toContainText('No memo exists');
	const row = page.getByTestId('task-train-attempt-1-cache');
	await expect(row).toContainText('cache miss: MEMO_ABSENT', { timeout: 60_000 });
	await page.getByTestId('task-train-attempt-1-cache-components').locator('summary').click();
	await expect(page.getByTestId('task-train-attempt-1-cache-components')).toContainText('config');
	await expect(page.getByTestId('task-train-attempt-1-cache-components')).toContainText('run_input');
	await shot(page, 'screenshot-p2-cache-miss.png');
	note(`P2: run ${runId} train Cache miss MEMO_ABSENT with its key components`);
});

test('Ignore cached results runs every cached task', async ({ page }) => {
	const name = `cached-p3-${STAMP}`;
	await apply(pipeline(name));
	await submitAndWait(name);
	await page.goto(`${UI}/orkflows/${name}?ns=${NS}`);
	await expect(page.getByTestId('task-cache-featurize')).toHaveText('24h · orkflow');
	await expect(page.getByTestId('task-cache-stamp')).toHaveText('—');
	await expect(page.getByTestId('dag-node-featurize-cache')).toBeVisible();
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-no-cache').check();
	await page.getByTestId('run-submit-cli-trigger').click();
	await expect(page.getByTestId('run-submit-cli-command')).toContainText('--no-cache');
	await shot(page, 'screenshot-p3-no-cache.png');
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	for (const task of ['featurize', 'train']) {
		await expect(page.getByTestId(`task-${task}-cache`)).toHaveAttribute('data-outcome', 'bypassed', { timeout: 180_000 });
		await expect(page.getByTestId(`task-${task}-cache`)).toHaveText('Cache bypassed');
	}
	await shot(page, 'screenshot-p3-bypassed.png');
	note(`P3: Ignore cached results — featurize and train read Cache bypassed; the CLI equivalent says --no-cache`);
});

test('the Cache admin page shows the hit rate and purges memos', async ({ page }) => {
	const name = `cached-p4-${STAMP}`;
	await apply(pipeline(name));
	await submitAndWait(name);
	await submitAndWait(name);
	await page.goto(`${UI}/orkflows/${name}?ns=${NS}`);
	await page.getByTestId('orkflow-cache-link').click();
	await page.waitForURL(/\/admin\/cache/);
	const row = page.getByTestId(`cache-usage-${name}-featurize`);
	await waitFor('the archive counts both runs', async () => {
		await page.reload();
		return (await row.count()) > 0 && (await page.getByTestId(`cache-usage-${name}-featurize-hits`).textContent()) === '1';
	}, 120_000);
	await expect(page.getByTestId(`cache-usage-${name}-featurize-misses`)).toHaveText('1');
	await expect(page.getByTestId(`cache-usage-${name}-featurize-rate`)).toHaveText('50.0%');
	await expect(page.getByTestId('cache-scan-note')).toContainText('scanned 2 of 2 finished runs');
	await expect(page.getByTestId(`cache-memos-orkflow-${name}-featurize`)).toBeVisible();
	await shot(page, 'screenshot-p4-cache-admin.png');
	await page.getByTestId('cache-purge').click();
	await expect(page.getByTestId('cache-purge-dialog')).toBeVisible();
	await page.getByTestId('cache-purge-orkflow').check();
	await page.getByTestId('cache-purge-cli-trigger').click();
	await expect(page.getByTestId('cache-purge-cli-command')).toContainText(`cache purge --orkflow '${name}'`);
	await page.getByTestId('cache-purge-confirm').click();
	await expect(page.getByTestId('cache-purge-result').first()).toContainText('Purged', { timeout: 30_000 });
	await page.getByTestId('cache-purge-dialog-close').click();
	await expect(page.getByTestId(`cache-memos-orkflow-${name}-featurize`)).toHaveCount(0, { timeout: 30_000 });
	await shot(page, 'screenshot-p4-purged.png');
	const next = await submitAndWait(name);
	const got = await rpc<RunView>('RunService/GetRun', { runId: next, namespace: NS });
	const featurize = got.run.taskRuns?.find((t) => t.taskId === 'featurize');
	expect(featurize?.cache?.outcome).toBe('CACHE_OUTCOME_MISS');
	note(`P4: ${name} featurize 1 hit / 1 miss (50.0%); purge emptied its memos; run ${next} featurize missed`);
});

test('a cached fan-out marks its cached items', async ({ page }) => {
	const name = `cached-p5-${STAMP}`;
	await apply(cachedFanOut(name));
	const parts = { parts: ['a', 'b', 'c', 'd'] };
	await submitAndWait(name, parts);
	const second = await submitAndWait(name, parts);
	await page.goto(`${UI}/runs/${second}?ns=${NS}`);
	for (let i = 0; i < 4; i++) {
		await expect(page.getByTestId(`task-convert-item-${i}`)).toHaveAttribute('data-cached', 'true', { timeout: 60_000 });
	}
	await expect(page.getByTestId('task-convert-each-summary')).toContainText('· 4 cached');
	await expect(page.getByTestId('task-summary-cached')).toContainText('· 4 cached');
	await page.getByTestId('task-convert-item-2').click();
	await expect(page.getByTestId('task-convert-item-panel-cache')).toHaveAttribute('data-outcome', 'hit');
	await shot(page, 'screenshot-p5-cached-items.png');
	note(`P5: run ${second} — every convert item cell data-cached="true", the summary says 4 cached`);
});
