import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv, clusterEnv, ensureActivityImages, killStarted, orkerEnv, proofOut,
	rpc as callEngine, s3Env, spawnLogged, tcpOpen, uiDir, repoRoot, waitFor, waitForEngine
} from './harness';
import { OP, addListFilter, selectOption } from './uiHelpers';

// A real browser path for URL-addressable list filters and dashboard windows.
// It gives pagination enough data to issue an opaque cursor, then follows it
// instead of reconstructing a cursor in the page.
//
// The Runs list is filtered through the structured list filter
// (docs/UI_CLI_PARITY.md "Filtering lists"): repeated `filter` parameters in
// the CLI grammar, edited through ListFilter, applied with the page size by
// Apply filters. R1–R6 below are its cases on a real engine, Orker, Temporal
// and MinIO.
//
// 21 runs at `size=10` is three pages on purpose: two pages only prove that a
// cursor can be followed once, and the defect this spec pins down — the URL
// growing by one escaped engine cursor per click, because the whole stack of
// earlier cursors rode along in a `history=` parameter — is only visible from
// the second Next onwards.
const ENGINE = 'http://127.0.0.1:18090';
const UI = 'http://127.0.0.1:14184';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-browsing';
// One namespace per execution: R1-R3 count every run of a status, of a
// contains match or of a window across the whole namespace, and the shared
// development archive keeps every earlier execution's finished runs there, so
// a fixed name would hand each rerun the FAILED runs of the ones before it.
const NS = `browsing-ops-${Date.now().toString(36)}`;
const FLOW = `browsing-${Date.now().toString(36)}`;
// A second Orkflow whose every run fails, so the status filters have more
// than one status to choose between.
const FAILING = `failing-${Date.now().toString(36)}`;
const RUNS = 21;
const FAILED_RUNS = 3;
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

function failingWorkflow() { return `name: ${FAILING}
version: "1.0.0"
description: browsing fixture that fails
tasks:
  - id: boom
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "echo boom >&2; exit 1"]
`; }

/** One EQUALS filter as the engine's JSON codec reads it. */
function equals(field: string, value: string) {
	return { field, op: 'FILTER_OPERATOR_EQUALS', values: [value] };
}

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
	await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: failingWorkflow() });
	for (let i = 0; i < RUNS; i++) await rpc('RunService/SubmitRun', { namespace: NS, orkflowName: FLOW, idempotencyKey: `${FLOW}-${i}` });
	for (let i = 0; i < FAILED_RUNS; i++) await rpc('RunService/SubmitRun', { namespace: NS, orkflowName: FAILING, idempotencyKey: `${FAILING}-${i}` });
	await waitFor('all fixture runs to reach the list index', async () => (await rpc<{ runs: unknown[] }>('RunService/ListRuns', { namespace: NS, filters: [equals('orkflow_name', FLOW), equals('status', 'SUCCEEDED')], pageSize: 25 })).runs.length === RUNS);
	await waitFor('every failing run to be listed FAILED', async () => (await rpc<{ runs: unknown[] }>('RunService/ListRuns', { namespace: NS, filters: [equals('status', 'FAILED')], pageSize: 25 })).runs.length === FAILED_RUNS);
});

test.afterAll(async () => { await killStarted(); });

/** The run rows on screen, by testid, in the order they render. */
function rowIDs(page: Page) {
	// Rows only: each row's Origin cell carries run-row-origin-{id}, which the
	// same prefix would otherwise count as a second row.
	return page.locator('tr[data-testid^="run-row-"]').evaluateAll((rows) => rows.map((row) => row.getAttribute('data-testid')));
}

/** The `filter` parameters of the page's address, decoded, in order. */
function urlFilters(page: Page): string[] {
	return new URL(page.url()).searchParams.getAll('filter');
}

/** The status pills of the rows on screen, in order. */
async function rowStatuses(page: Page) {
	return (await page.locator('tr[data-testid^="run-row-"] td:nth-child(4)').allTextContents()).map((text) => text.trim());
}

/** The CLI equivalent of the applied filters, read from its popover. */
async function cliCommand(page: Page): Promise<string> {
	await page.getByTestId('run-filters-cli-trigger').click();
	const text = await page.getByTestId('run-filters-cli-command').innerText();
	await page.keyboard.press('Escape');
	return text.trim();
}

/**
 * A list address says WHERE the reader is and nothing about how they got
 * there: the filters, one cursor, one page number. No scalar key twice, no
 * `history=`, and a length that does not grow with depth.
 */
function expectListAddress(url: string): URL {
	const parsed = new URL(url);
	expect([...new Set(parsed.searchParams.keys())].sort()).toEqual(['cursor', 'filter', 'ns', 'page', 'size']);
	expect(parsed.searchParams.getAll('filter')).toEqual([`orkflow_name=${FLOW}`]);
	expect(parsed.searchParams.getAll('size')).toHaveLength(1);
	expect(parsed.searchParams.has('history')).toBe(false);
	// One engine cursor is 160-240 characters before escaping; 400 is generous
	// for exactly one and far under a stack of them.
	expect(url.length).toBeLessThan(400);
	return parsed;
}

test('R5: runs keep their filters in the URL, page under them, and a pasted later page offers First page', async ({ page, browser }) => {
	await page.goto(`${UI}/runs?ns=${NS}&filter=orkflow_name%3D${FLOW}&size=10`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	// The draft is the URL's: one orkflow_name row, already filled.
	await expect(page.getByTestId('run-filter-field-0')).toHaveText('Orkflow (orkflow_name)');
	await expect(page.getByTestId('run-filter-value-0')).toHaveValue(FLOW);
	await expect(page.getByTestId('runs-heading')).toContainText(`for ${FLOW}`);
	await page.getByTestId('run-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual([`orkflow_name=${FLOW}`]);
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

	// What a field may take is the engine's to refuse, and its words are
	// shown verbatim: a status the list cannot select, and an inverted
	// created_at window.
	await page.goto(`${UI}/runs?ns=${NS}&filter=status%3Dbogus`);
	await expect(page.getByTestId('engine-error')).toContainText('"bogus" is not a status this list can select; status is one of PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED, SKIPPED');
	await page.goto(`${UI}/runs?ns=${NS}&filter=created_at%3E%3D2026-01-02T00%3A00%3A00Z&filter=created_at%3C2026-01-01T00%3A00%3A00Z`);
	await expect(page.getByTestId('engine-error')).toContainText('created_at >= 2026-01-02T00:00:00Z and < 2026-01-01T00:00:00Z select no instant');
	await page.screenshot({ path: out('screenshot-runs-invalid-range.png'), fullPage: true });
});

test('R1: status in and not equals through the enum controls', async ({ page }) => {
	await page.goto(`${UI}/runs?ns=${NS}&size=50`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.waitForLoadState('networkidle');
	// in: a checkbox per allowed status, never AWAITING (a task status).
	const row = await addListFilter(page, 'run', { field: 'status', op: OP.in });
	await expect(page.getByTestId(`run-filter-value-${row}-AWAITING`)).toHaveCount(0);
	// Ticked out of order on purpose: the values go out in the declared order.
	await page.getByTestId(`run-filter-value-${row}-CANCELED`).check();
	await page.getByTestId(`run-filter-value-${row}-FAILED`).check();
	await page.getByTestId('run-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['status=FAILED,CANCELED']);
	await expect.poll(() => rowStatuses(page)).toEqual(Array(FAILED_RUNS).fill('FAILED'));
	expect(await cliCommand(page)).toBe(`orkctl -n '${NS}' run list --filter 'status=FAILED,CANCELED'`);
	await page.screenshot({ path: out('screenshot-runs-status-in.png'), fullPage: true });

	// not equals: one value from the enum Select. The draft was refilled from
	// the URL, so the row is replaced rather than added to.
	await page.getByTestId(`run-filter-remove-${row}`).click();
	const ne = await addListFilter(page, 'run', { field: 'status', op: OP.notEquals, select: 'SUCCEEDED' });
	await page.getByTestId('run-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['status!=SUCCEEDED']);
	await expect.poll(() => rowStatuses(page)).toEqual(Array(FAILED_RUNS).fill('FAILED'));
	await expect(page.getByTestId(`run-filter-value-${ne}`)).toHaveText('SUCCEEDED');
	await page.screenshot({ path: out('screenshot-runs-status-not-equals.png'), fullPage: true });
});

test('R2: orkflow_name contains narrows, and an equals names the Orkflow in the header', async ({ page }) => {
	await page.goto(`${UI}/runs?ns=${NS}&size=50`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'run', { field: 'orkflow_name', op: OP.contains, value: 'failing-' });
	await page.getByTestId('run-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['orkflow_name~failing-']);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(FAILED_RUNS);
	// A contains is not one Orkflow: the header names none and Submit offers
	// the picker.
	await expect(page.getByTestId('runs-heading')).toHaveText('Runs');
	await expect(page.getByTestId('runs-submit-run')).toHaveText('Browse orkflows');

	await page.goto(`${UI}/runs?ns=${NS}&filter=orkflow_name%3D${FAILING}`);
	await expect(page.getByTestId('runs-heading')).toContainText(`for ${FAILING}`);
	await expect(page.getByTestId('runs-submit-run')).toHaveText('Submit a run');
	await expect(page.getByTestId('runs-submit-run')).toHaveAttribute('href', `/orkflows/${FAILING}?ns=${NS}`);
	await page.screenshot({ path: out('screenshot-runs-orkflow-equals.png'), fullPage: true });
});

test('R3: a created_at window through datetime-local, and its CLI equivalent in UTC', async ({ page }) => {
	await page.goto(`${UI}/runs?ns=${NS}&size=50`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.waitForLoadState('networkidle');
	// The local minute an hour ago and an hour ahead: every fixture run is
	// inside, so the window keeps all of them.
	const local = (offsetMs: number) => new Date(Date.now() + offsetMs - new Date().getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
	const since = local(-60 * 60_000);
	const until = local(60 * 60_000);
	const from = await addListFilter(page, 'run', { field: 'created_at', op: OP.atOrAfter, value: since });
	const to = await addListFilter(page, 'run', { field: 'created_at', op: OP.before, value: until });
	const sinceUtc = new Date(since).toISOString().replace('.000Z', 'Z');
	const untilUtc = new Date(until).toISOString().replace('.000Z', 'Z');
	await expect(page.getByTestId(`run-filter-utc-${from}`)).toHaveText(`${sinceUtc} (UTC)`);
	await expect(page.getByTestId(`run-filter-utc-${to}`)).toHaveText(`${untilUtc} (UTC)`);
	await page.getByTestId('run-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual([`created_at<${untilUtc}`, `created_at>=${sinceUtc}`]);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(RUNS + FAILED_RUNS);
	expect(await cliCommand(page)).toBe(`orkctl -n '${NS}' run list --filter 'created_at<${untilUtc}' --filter 'created_at>=${sinceUtc}'`);
	await page.screenshot({ path: out('screenshot-runs-created-window.png'), fullPage: true });

	// A window that closed before the runs existed keeps none.
	await page.goto(`${UI}/runs?ns=${NS}&filter=created_at%3C2000-01-01T00%3A00%3A00Z`);
	await expect(page.getByTestId('runs-empty')).toContainText('No runs match these filters');
});

test('R4: page size is a field of the filter form and applies with Apply filters', async ({ page }) => {
	await page.goto(`${UI}/runs?ns=${NS}&filter=orkflow_name%3D${FLOW}`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	await page.waitForLoadState('networkidle');
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(RUNS);
	const before = page.url();
	await selectOption(page, 'run-filter-size', '10');
	await expect(page.getByTestId('run-filter-size')).toHaveText('10');
	expect(page.url()).toBe(before);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(RUNS);
	await page.getByTestId('run-filters-apply').click();
	await expect(page).toHaveURL(/size=10/);
	const applied = new URL(page.url());
	expect(applied.searchParams.getAll('size')).toEqual(['10']);
	expect(applied.searchParams.get('cursor')).toBeNull();
	expect(applied.searchParams.getAll('filter')).toEqual([`orkflow_name=${FLOW}`]);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(10);
	await expect(page.getByTestId('runs-page-status')).toHaveText('Page 1 · 10 runs');
	await page.screenshot({ path: out('screenshot-runs-page-size.png'), fullPage: true });
});

test('R6: the dashboard and an Orkflow page link into the filtered runs list', async ({ page }) => {
	// The dashboard's failures table links each Orkflow to its runs. (The
	// flake and fingerprint rows' links, which add the report's window, are
	// followed in logs-dashboard.spec.ts, which seeds a retried task.)
	await page.goto(`${UI}/?ns=${NS}&window=24h`);
	await expect.poll(async () => page.getByTestId(`failure-${FAILING}`).count(), { timeout: 60_000 }).toBe(1);
	await page.getByTestId(`failure-${FAILING}`).getByRole('link', { name: FAILING }).click();
	await page.waitForURL(/\/runs\?/);
	expect(urlFilters(page)).toEqual([`orkflow_name=${FAILING}`]);
	expect(new URL(page.url()).searchParams.get('ns')).toBe(NS);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(FAILED_RUNS);
	await expect(page.getByTestId('runs-heading')).toContainText(`for ${FAILING}`);

	// An Orkflow's View runs is the same filter.
	await page.goto(`${UI}/orkflows/${FLOW}?ns=${NS}`);
	await page.getByRole('link', { name: 'View runs' }).click();
	await page.waitForURL(/\/runs\?/);
	expect(urlFilters(page)).toEqual([`orkflow_name=${FLOW}`]);
	await expect(page.locator('tr[data-testid^="run-row-"]')).toHaveCount(RUNS);
	await page.screenshot({ path: out('screenshot-runs-from-orkflow.png'), fullPage: true });
});

test('the orkflows page size applies with Apply filters and writes one size to the URL', async ({ page }) => {
	// Page size is a field of the Orkflows filter form (ListFilter) and
	// applies with Apply filters, never on change: one form never applies
	// some fields on change and others on submit. The four bold-or-not links
	// this once replaced concatenated `&size=N` onto a query that already
	// carried `size=25`, so the URL is also held to exactly one size key.
	await page.goto(`${UI}/orkflows?ns=${NS}`);
	await expect(page.getByTestId('orkflow-list')).toBeVisible();
	// The picker is a button and a listbox, so it needs the page hydrated
	// before the panel can open at all.
	await page.waitForLoadState('networkidle');
	const before = page.url();
	await selectOption(page, 'orkflow-page-size', '50');
	await expect(page.getByTestId('orkflow-page-size')).toHaveText('50');
	expect(page.url()).toBe(before);
	await page.getByTestId('orkflow-filters-apply').click();
	await expect(page).toHaveURL(/size=50/);
	expect(new URL(page.url()).searchParams.getAll('size')).toEqual(['50']);
	expect(new URL(page.url()).searchParams.get('cursor')).toBeNull();
	await selectOption(page, 'orkflow-page-size', '100');
	await expect(page).toHaveURL(/size=50/);
	await page.getByTestId('orkflow-filters-apply').click();
	await expect(page).toHaveURL(/size=100/);
	await expect(page.getByTestId('orkflow-page-size')).toHaveText('100');
	expect(new URL(page.url()).searchParams.getAll('size')).toEqual(['100']);
	expect(new URL(page.url()).searchParams.get('cursor')).toBeNull();
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
