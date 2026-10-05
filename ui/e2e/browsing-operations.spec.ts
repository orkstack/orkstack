import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv, clusterEnv, ensureActivityImages, killAll, orkerEnv, proofOut,
	rpc as callEngine, s3Env, spawnLogged, tcpOpen, uiDir, repoRoot, waitFor, waitForEngine
} from './harness';
import { selectOption } from './uiHelpers';

// A real browser path for URL-addressable list filters and dashboard windows.
// It gives pagination enough data to issue an opaque cursor, then follows it
// instead of reconstructing a cursor in the page.
//
// 21 runs at `size=10` is three pages on purpose: two pages only prove that a
// cursor can be followed once, and the defect this spec pins down — the URL
// growing by one escaped engine cursor per click, because the whole stack of
// earlier cursors rode along in a `history=` parameter — is only visible from
// the second Next onwards.
const ENGINE = 'http://127.0.0.1:18090';
const UI = 'http://127.0.0.1:14184';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-browsing';
const NS = 'browsing-ops';
const FLOW = `browsing-${Date.now().toString(36)}`;
const RUNS = 21;
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
	for (let i = 0; i < RUNS; i++) await rpc('RunService/SubmitRun', { namespace: NS, orkflowName: FLOW, idempotencyKey: `${FLOW}-${i}` });
	await waitFor('all fixture runs to reach the list index', async () => (await rpc<{ runs: unknown[] }>('RunService/ListRuns', { namespace: NS, orkflowName: FLOW, pageSize: 25 })).runs.length === RUNS);
});

test.afterAll(async () => { await killAll(); });

/** The run rows on screen, by testid, in the order they render. */
function rowIDs(page: Page) {
	// Rows only: each row's Origin cell carries run-row-origin-{id}, which the
	// same prefix would otherwise count as a second row.
	return page.locator('tr[data-testid^="run-row-"]').evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')));
}

/**
 * A list address says WHERE the reader is and nothing about how they got
 * there: the filter coordinates, one cursor, one page number. No key twice,
 * no `history=`, and a length that does not grow with depth.
 */
function expectListAddress(url: string): URL {
	const parsed = new URL(url);
	expect([...parsed.searchParams.keys()].sort()).toEqual(['cursor', 'ns', 'orkflow', 'page', 'size']);
	expect(parsed.searchParams.getAll('size')).toHaveLength(1);
	expect(parsed.searchParams.has('history')).toBe(false);
	// One engine cursor is 160-240 characters before escaping; 400 is generous
	// for exactly one and far under a stack of them.
	expect(url.length).toBeLessThan(400);
	return parsed;
}

test('runs retain filter/time URL coordinates, reject invalid time ranges, and follow opaque pages without a growing URL', async ({ page, browser }) => {
	await page.goto(`${UI}/runs?ns=${NS}&orkflow=${FLOW}&size=10`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.getByTestId('run-filters-apply').click();
	await expect(page).toHaveURL(new RegExp(`orkflow=${FLOW}`));
	const firstPageIDs = await rowIDs(page);
	expect(firstPageIDs).toHaveLength(10);
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 1 · 10 runs');
	await expect(page.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'true');
	await expect(page.getByTestId('runs-next')).toHaveAttribute('aria-disabled', 'false');

	// Next records the cursor it is leaving as it is followed, so the page has
	// to be hydrated for the path back to exist: before hydration the anchor is
	// an ordinary link and the browser simply goes. The built UI this spec
	// drives hydrates immediately — Apply filters above already proves it — and
	// waiting is a second of insurance rather than a fix.
	await page.waitForLoadState('networkidle');
	await page.getByTestId('runs-next').click();
	await expect(page).toHaveURL(/cursor=/);
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 2 · 10 runs');
	const pageTwoUrl = page.url();
	expectListAddress(pageTwoUrl);
	const secondPageIDs = await rowIDs(page);
	expect(secondPageIDs).toHaveLength(10);
	expect(secondPageIDs.some((id) => firstPageIDs.includes(id))).toBe(false);
	await expect(page.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'false');

	// The decisive one: a second cursor followed leaves the address the same
	// SIZE, because the cursor is replaced rather than pushed onto a stack the
	// URL carries. A page-number digit is the only honest growth.
	await page.getByTestId('runs-next').click();
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 3 · 1 run · last page');
	const pageThreeUrl = page.url();
	expectListAddress(pageThreeUrl);
	expect(pageThreeUrl.length).toBeLessThanOrEqual(pageTwoUrl.length + 16);
	expect(await rowIDs(page)).toHaveLength(1);
	await expect(page.getByTestId('runs-next')).toHaveAttribute('aria-disabled', 'true');
	await page.screenshot({ path: out('screenshot-runs-last-page.png'), fullPage: true });

	// Back down the way we came, to a first page that names neither a cursor
	// nor a page number, because that is what page 1 is.
	await page.getByTestId('runs-previous').click();
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 2 · 10 runs');
	await page.getByTestId('runs-previous').click();
	await expect(page).not.toHaveURL(/cursor=/);
	await expect(page).not.toHaveURL(/page=/);
	await expect(page.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'true');
	expect(await rowIDs(page)).toEqual(firstPageIDs);

	// The path back lives in this tab's sessionStorage, so a reload — an
	// ordinary gesture on a list — keeps Previous.
	await page.getByTestId('runs-next').click();
	await expect(page).toHaveURL(/cursor=/);
	await page.reload();
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 2 · 10 runs');
	await expect(page.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'false');
	await page.getByTestId('runs-previous').click();
	await expect(page).not.toHaveURL(/cursor=/);

	// A pasted link opens the page it was copied from, and says plainly that
	// it cannot name the page before it: this browser never walked here, so
	// there is no earlier cursor to offer — only the first page.
	const colleague = await browser.newContext();
	const pasted = await colleague.newPage();
	await pasted.goto(pageTwoUrl);
	await expect(pasted.getByTestId('runs-page-status')).toHaveText('Page 2 · 10 runs');
	await expect(pasted.getByTestId('runs-previous')).toHaveAttribute('aria-disabled', 'true');
	await expect(pasted.getByTestId('runs-first')).toBeVisible();
	await pasted.screenshot({ path: out('screenshot-runs-pasted-page.png'), fullPage: true });
	await pasted.getByTestId('runs-first').click();
	await expect(pasted).not.toHaveURL(/cursor=/);
	await expect(pasted.getByTestId('runs-page-status')).toHaveText('Page 1 · 10 runs');
	await colleague.close();

	await page.screenshot({ path: out('screenshot-runs-filtered.png'), fullPage: true });
	await page.goto(`${UI}/runs?ns=${NS}&status=bogus`);
	await expect(page.getByTestId('engine-error')).toContainText('Unknown run status');
	await page.goto(`${UI}/runs?ns=${NS}&since=2026-01-02T00:00&until=2026-01-01T00:00`);
	await expect(page.getByTestId('engine-error')).toContainText('after');
	await page.screenshot({ path: out('screenshot-runs-invalid-range.png'), fullPage: true });
});

test('the orkflows page size applies on change and writes one size to the URL', async ({ page }) => {
	// The four bold-or-not links this replaces concatenated `&size=N` onto a
	// query that already carried `size=25`: the address grew a second `size`
	// key, `searchParams.get` answered with the first, and the click did
	// nothing. The assertion is about the URL — the fixture has one orkflow.
	await page.goto(`${UI}/orkflows?ns=${NS}`);
	await expect(page.getByTestId('orkflow-list')).toBeVisible();
	// The picker is a button and a listbox, so it needs the page hydrated
	// before the panel can open at all.
	await page.waitForLoadState('networkidle');
	await selectOption(page, 'orkflow-page-size', '50');
	await expect(page).toHaveURL(/size=50/);
	await expect(page.getByTestId('orkflow-page-size')).toHaveText('50');
	expect(new URL(page.url()).searchParams.getAll('size')).toEqual(['50']);
	await selectOption(page, 'orkflow-page-size', '100');
	await expect(page).toHaveURL(/size=100/);
	await expect(page.getByTestId('orkflow-page-size')).toHaveText('100');
	expect(new URL(page.url()).searchParams.getAll('size')).toEqual(['100']);
	await page.screenshot({ path: out('screenshot-orkflows-page-size.png'), fullPage: true });
});

test('dashboard presets and custom since are visible, addressable, and export a matching CLI', async ({ page }) => {
	await page.goto(`${UI}/?ns=${NS}&window=15m`);
	await expect(page.getByTestId('dashboard-window-15m')).toHaveAttribute('aria-current', 'page');
	await page.getByTestId('dashboard-window-6h').click();
	await expect(page).toHaveURL(/window=6h/);
	// The custom range is the fourth segment of the same group: it is chosen
	// the way the presets are and marked the way they are. A local
	// `YYYY-MM-DDTHH:mm` is what the input holds and what sinceInputValue
	// produces, and the segment states the same instant back.
	const since = new Date(Date.now() - 5 * 60_000 - new Date().getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
	// The segment is a popover trigger, so it answers a click only once the
	// page is hydrated.
	await page.waitForLoadState('networkidle');
	await page.getByTestId('dashboard-window-custom').click();
	await expect(page.getByTestId('dashboard-window-custom-popover')).toBeVisible();
	await page.getByTestId('dashboard-window-custom-since').fill(since);
	await page.getByTestId('dashboard-window-custom-apply').click();
	await expect(page).toHaveURL(/window=custom.*since=/);
	await expect(page.getByTestId('dashboard-window-custom')).toHaveAttribute('aria-current', 'page');
	// The applied instant, in the same format the header line uses — not the
	// escaped ISO string in the address bar, which was the only trace of it.
	await expect(page.getByTestId('dashboard-window-custom')).toContainText('Since');
	for (const preset of ['15m', '6h', '24h']) await expect(page.getByTestId(`dashboard-window-${preset}`)).not.toHaveAttribute('aria-current', 'page');
	// A reload states the same window, in the control and in the draft the
	// picker reopens with — the field used to come back empty beside a window
	// that was still in force.
	await page.reload();
	await expect(page.getByTestId('dashboard-window-custom')).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId('dashboard-window-custom')).toContainText('Since');
	await page.waitForLoadState('networkidle');
	await page.getByTestId('dashboard-window-custom').click();
	await expect(page.getByTestId('dashboard-window-custom-since')).toHaveValue(since);
	await page.keyboard.press('Escape');
	await page.getByTestId('dashboard-cli-trigger').click();
	await expect(page.getByTestId('dashboard-cli-command')).toContainText('orkctl --namespace');
	await page.screenshot({ path: out('screenshot-dashboard-window.png'), fullPage: true });
});
