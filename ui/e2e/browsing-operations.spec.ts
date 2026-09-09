import { expect, test } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv, clusterEnv, ensureActivityImages, killAll, orkerEnv, proofOut,
	rpc as callEngine, s3Env, spawnLogged, tcpOpen, uiDir, repoRoot, waitFor, waitForEngine
} from './harness';

// A real browser path for URL-addressable list filters and dashboard windows.
// It gives pagination enough data to issue an opaque cursor, then follows it
// instead of reconstructing a cursor in the page.
const ENGINE = 'http://127.0.0.1:18090';
const UI = 'http://127.0.0.1:14184';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-browsing';
const NS = 'browsing-ops';
const FLOW = `browsing-${Date.now().toString(36)}`;
const DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'browsing-operations') : path.join(uiDir, 'test-results', 'browsing-operations');
const out = proofOut(DIR);
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);

function workflow() { return `name: ${FLOW}
version: "1.0.0"
description: browsing fixture
tasks:
  - id: emit
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "printf '{}' > /orkstack/output/values.json"]
`; }

test.beforeAll(async () => {
	mkdirSync(DIR, { recursive: true });
	writeFileSync(out('session.txt'), `browsing operations\nnamespace=${NS}\nflow=${FLOW}\n`);
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18090, 14184]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], { ...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18090', ORK_WS_ALLOWED_ORIGINS: UI });
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('browsing-operations') });
	await waitForEngine(ENGINE);
	spawnLogged(out('ui.log'), 'node', ['build'], { HOST: '127.0.0.1', PORT: '14184', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: 'ws://127.0.0.1:18090' }, uiDir);
	await waitFor('UI', async () => (await fetch(`${UI}/runs?ns=${NS}`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'browsing operations' } });
	await waitFor('a live pool', async () => (await rpc<{ pools: { liveOrkerCount: number }[] }>('NamespaceService/ListPools', { namespace: NS })).pools.some((pool) => pool.liveOrkerCount > 0));
	await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: workflow() });
	for (let i = 0; i < 11; i++) await rpc('RunService/SubmitRun', { namespace: NS, orkflowName: FLOW, idempotencyKey: `${FLOW}-${i}` });
	await waitFor('all fixture runs to reach the list index', async () => (await rpc<{ runs: unknown[] }>('RunService/ListRuns', { namespace: NS, orkflowName: FLOW, pageSize: 25 })).runs.length === 11);
});

test.afterAll(async () => { await killAll(); });

test('runs retain filter/time URL coordinates, reject invalid time ranges, and follow opaque pages', async ({ page }) => {
	await page.goto(`${UI}/runs?ns=${NS}&orkflow=${FLOW}&size=10`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.getByTestId('run-filters-apply').click();
	await expect(page).toHaveURL(new RegExp(`orkflow=${FLOW}`));
	const firstPageIDs = await page.locator('[data-testid^="run-row-"]').evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')));
	expect(firstPageIDs).toHaveLength(10);
	await expect(page.getByTestId('runs-next')).toHaveAttribute('aria-disabled', 'false');
	await page.getByTestId('runs-next').click();
	await expect(page).toHaveURL(/cursor=/);
	const secondPageIDs = await page.locator('[data-testid^="run-row-"]').evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')));
	expect(secondPageIDs).toHaveLength(1);
	expect(secondPageIDs.some((id) => firstPageIDs.includes(id))).toBe(false);
	await expect(page.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'false');
	await page.getByTestId('runs-previous').click();
	await expect(page).not.toHaveURL(/cursor=/);
	await page.screenshot({ path: out('screenshot-runs-filtered.png'), fullPage: true });
	await page.goto(`${UI}/runs?ns=${NS}&status=bogus`);
	await expect(page.getByTestId('engine-error')).toContainText('Unknown run status');
	await page.goto(`${UI}/runs?ns=${NS}&since=2026-01-02T00:00&until=2026-01-01T00:00`);
	await expect(page.getByTestId('engine-error')).toContainText('after');
	await page.screenshot({ path: out('screenshot-runs-invalid-range.png'), fullPage: true });
});

test('dashboard presets and custom since are visible, addressable, and export a matching CLI', async ({ page }) => {
	await page.goto(`${UI}/?ns=${NS}&window=15m`);
	await expect(page.getByTestId('dashboard-window-15m')).toHaveAttribute('aria-current', 'page');
	await page.getByTestId('dashboard-window-6h').click();
	await expect(page).toHaveURL(/window=6h/);
	await page.locator('input[name="since"]').fill(new Date(Date.now() - 5 * 60_000).toISOString().slice(0, 16));
	await page.getByRole('button', { name: 'Custom' }).click();
	await expect(page).toHaveURL(/window=custom.*since=/);
	await page.getByTestId('dashboard-cli-trigger').click();
	await expect(page.getByTestId('dashboard-cli-command')).toContainText('orkctl --namespace');
	await page.screenshot({ path: out('screenshot-dashboard-window.png'), fullPage: true });
});
