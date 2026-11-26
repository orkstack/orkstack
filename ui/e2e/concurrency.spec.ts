import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
import { mkdirSync } from 'node:fs';
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
	tcpOpen,
	storeEnv,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { OP, addListFilter } from './uiHelpers';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('concurrency');

// A concurrency limit in a browser, against the real stack (E88 P9a):
//
//   C1  the Orkflow page names its limit, in `orkctl orkflow get`'s words;
//   C2  a run past the limit shows the Queued card: why it waits, the
//       limit, its place, the run it waits behind as a link, and when it
//       times out;
//   C3  the Runs list's Queued tile counts it and filters to it, and the
//       dashboard's Running count says how many of the running are queued;
//   C4  on the queued run's open page, when the run holding the key ends,
//       the queued run starts and its card goes, live;
//   C5  Operate → Concurrency lists the key with who holds and waits, its
//       filter narrows it, and the key's own page, which the Queued card
//       links to, names both runs in order; a key nothing uses says so;
//   C6  a schedule's fire that a skip limit keeps from starting a run reads
//       skipped, with why, on the Schedules tab, and never preparing.
//
// Ports and a Temporal namespace of this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18867';
const TEMPORAL_NAMESPACE = temporalNamespace('concurrency');
const STAMP = Date.now().toString(36);
const ORKFLOW = `concurrency-${STAMP}`;
const SKIPPER = `concurrency-skip-${STAMP}`;
const SKIP_KEY = `skip-${STAMP}/a+b`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'concurrency')
	: path.join(uiDir, 'test-results', 'concurrency');
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, 'concurrency-session.txt'));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// One run at a time on the Orkflow's own key. hold keeps the slot far longer
// than the test reads the queued run; the test then ends the holder itself,
// by a cancel, so the moment the slot frees is the test's own.
function orkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: One run at a time.

concurrency:
  limit: 1

tasks:
  - id: hold
    uses: shell
    timeout_seconds: 600
    cancellation_grace_period_seconds: 5
    with:
      op: exec
      command: ["sleep", "500"]
`;
}

// A skip limit on a key of its own, with a slash and a plus in it, held by a
// run that only sleeps.
function skipper(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${SKIPPER}
version: "0.1.0"
concurrency:
  key: "${SKIP_KEY}"
  limit: 1
  on_limit: skip
tasks:
  - id: hold
    sleep:
      seconds: 400
`;
}

async function axe(page: Page, label: string): Promise<void> {
	const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze();
	const found = result.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
	note(`axe ${label}: ${found.length} violation(s)${found.length ? ` ${found.join('; ')}` : ''}`);
	expect(found).toEqual([]);
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18867)) throw new Error('port 18867 already in use (stray process?)');
	ensureActivityImages();
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18867',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv('concurrency'),
		ORK_ORKER_ID: 'orker-e2e-concurrency'
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: skipper() });
	note(`applied ${ORKFLOW} and ${SKIPPER}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

// The runs a test started, canceled whatever it ends with and waited for, so
// no 500 s holder outlives the Orker. The queued one is asked first, which
// usually ends it before the holder's slot frees; if it starts all the same,
// its own cancel ends it.
const started: string[] = [];
// A schedule a test made, and the Orkflow whose every run is then that test's.
let scheduled: { name: string; orkflow: string } | undefined;
test.afterEach(async () => {
	const schedule = scheduled;
	scheduled = undefined;
	if (schedule) {
		await rpc('ScheduleService/DeleteSchedule', { namespace: 'default', name: schedule.name }).catch(() => undefined);
		// A fire's run is found by its Orkflow: the test may have failed before it
		// read the id. One not listed yet holds no container, only a timer in this
		// spec's own Temporal namespace.
		const { runs: fired } = await rpc<{ runs?: { id: string }[] }>('RunService/ListRuns', {
			namespace: 'default',
			filters: [{ field: 'orkflow_name', op: 'FILTER_OPERATOR_EQUALS', values: [schedule.orkflow] }]
		}).catch(() => ({ runs: [] }));
		started.push(...(fired ?? []).map((r) => r.id).filter((id) => !started.includes(id)));
	}
	const runs = started.splice(0);
	for (const runId of [...runs].reverse()) {
		await rpc('RunService/CancelRun', { runId, namespace: 'default', reason: 'concurrency e2e: cleanup' }).catch(() => undefined);
	}
	for (const runId of runs) {
		await waitFor(
			`run ${runId} to end`,
			async () => /SUCCEEDED|FAILED|CANCELED/.test((await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId, namespace: 'default' })).run?.status ?? ''),
			60_000
		);
	}
});

test('a run past its limit waits in line, says why, and starts when the holder ends', async ({ page }) => {
	test.setTimeout(300_000);

	// C1 — the Orkflow page names the limit.
	await page.goto(`${ENGINE}/orkflows/${ORKFLOW}?ns=default`);
	await expect(page.getByTestId('orkflow-concurrency')).toContainText(
		"limit 1 on this Orkflow's own key; the rest wait in line up to the deployment's default"
	);
	await shot(page, 'orkflow-concurrency.png');

	// The holder runs first; the Orker may still be booting, which the wait
	// for its task's Running bounds.
	const first = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, allowEmptyPool: true })).run.id;
	started.push(first);
	await page.goto(`${ENGINE}/runs/${first}?ns=default`);
	await expect(page.getByTestId('task-hold-status')).toHaveText('Running', { timeout: 120_000 });
	await expect(page.getByTestId('run-queued')).toHaveCount(0);
	note(`holder ${first} running`);

	// C2 — a second run queues behind it.
	const second = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, allowEmptyPool: true })).run.id;
	started.push(second);
	note(`submitted ${second}`);
	await page.goto(`${ENGINE}/runs/${second}?ns=default`);
	const card = page.getByTestId('run-queued');
	await expect(card).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('run-queued-key')).toHaveText(/\S/);
	// A run that waits on arrival needs no sentence beside its heading and key.
	await expect(page.getByTestId('run-queued-headline')).toHaveCount(0);
	await expect(page.getByTestId('run-queued-limit')).toHaveText('1 run at once');
	await expect(page.getByTestId('run-queued-place')).toHaveText(/^(\d+(st|nd|rd|th) in line|Unknown)$/);
	await expect(page.getByTestId('run-queued-times-out')).toHaveText(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
	await expect(page.getByTestId('task-hold-status')).toHaveText('Pending');
	// The run it waits behind is named and opens.
	await expect(page.getByTestId('run-queued-holder')).toHaveCount(1, { timeout: 30_000 });
	await expect(page.getByTestId('run-queued-holder')).toHaveAttribute('href', `/runs/${first}?ns=default`);
	await expect(page.getByTestId('run-queued-holder')).toHaveAccessibleName(first);
	note(`C2 card: ${await card.innerText()}`);
	await shot(page, 'queued-light.png');
	await page.emulateMedia({ colorScheme: 'dark' });
	await shot(page, 'queued-dark.png');
	await page.emulateMedia({ colorScheme: 'light' });
	await page.setViewportSize({ width: 390, height: 844 });
	await expect(card).toBeVisible();
	await shot(page, 'queued-phone.png');
	await page.setViewportSize({ width: 1280, height: 800 });

	// C3 — the list's Queued tile counts and filters it; the dashboard says it.
	await page.goto(`${ENGINE}/runs?ns=default`);
	const tile = page.getByTestId('runs-tile-queued');
	await expect(tile).toBeVisible({ timeout: 30_000 });
	await expect(tile).toContainText('Queued');
	await expect(tile.locator('.num')).toHaveText('1');
	await tile.click();
	await expect(tile).toHaveAttribute('aria-pressed', 'true');
	await expect(page).toHaveURL(/queued/);
	await expect(page.getByRole('row').filter({ hasText: second.slice(-6) })).toHaveCount(1, { timeout: 30_000 });
	await expect(page.getByRole('row').filter({ hasText: first.slice(-6) })).toHaveCount(0);
	await shot(page, 'runs-queued.png');
	// The dashboard counts once, when it loads.
	await expect(async () => {
		await page.goto(`${ENGINE}/?ns=default`);
		await expect(page.getByTestId('stat-running-note')).toHaveText('1 queued', { timeout: 5_000 });
	}).toPass({ timeout: 30_000 });
	await shot(page, 'dashboard-queued.png');

	// C5 — the key, from the queued run's card to its own page and the list.
	await page.goto(`${ENGINE}/runs/${second}?ns=default`);
	await expect(card).toBeVisible({ timeout: 30_000 });
	const key = (await page.getByTestId('run-queued-key').innerText()).replace(/\s+/g, '');
	await page.getByTestId('run-queued-key').getByRole('link').click();
	await expect(page).toHaveURL(/\/admin\/concurrency\?/);
	await expect(page.getByTestId('concurrency-key')).toHaveText(key);
	await expect(page.getByTestId('breadcrumb-current')).toHaveText(key);
	await expect(page.getByTestId('admin-tab-concurrency')).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId('concurrency-state')).toHaveText('OK');
	await expect(page.getByTestId('concurrency-holders-count')).toHaveText('1');
	await expect(page.getByTestId('concurrency-queued-count')).toHaveText('1');
	await expect(page.getByTestId('concurrency-epoch')).toHaveText(/^\d+$/);
	const holder = page.getByTestId('concurrency-holder');
	await expect(holder).toHaveCount(1);
	await expect(holder.getByRole('link', { name: first, exact: true })).toHaveAttribute('href', `/runs/${first}?ns=default`);
	await expect(holder.locator('[data-label="Level"]')).toHaveText('run');
	await expect(holder.locator('[data-label="On limit"]')).toHaveText('queue');
	await expect(page.getByTestId('concurrency-holder-stalled')).toHaveCount(0);
	const waiting = page.getByTestId('concurrency-waiting');
	await expect(waiting).toHaveCount(1);
	await expect(waiting.locator('[data-label="Place"]')).toHaveText('1');
	await expect(waiting.getByRole('link', { name: second, exact: true })).toBeVisible();
	await expect(waiting.locator('[data-label="Times out"]')).toHaveText(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
	await page.getByTestId('page-cli-trigger').click();
	await expect(page.getByText(`orkctl -n default concurrency get ${key}`)).toBeVisible();
	await page.keyboard.press('Escape');
	await axe(page, 'one key');
	await shot(page, 'key-light.png');
	await page.emulateMedia({ colorScheme: 'dark' });
	await axe(page, 'one key, dark');
	await shot(page, 'key-dark.png');
	await page.emulateMedia({ colorScheme: 'light' });
	await page.setViewportSize({ width: 390, height: 844 });
	await expect(holder).toBeVisible();
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
	await shot(page, 'key-phone.png');
	await page.setViewportSize({ width: 1280, height: 800 });

	await page.getByTestId('admin-tab-concurrency').click();
	await expect(page.getByTestId('concurrency-scope')).toBeVisible();
	// The index of the keys in use lags a moment behind a first hold.
	const row = page.locator(`[data-testid="concurrency-row"][data-key="${key}"]`);
	await expect(async () => {
		await page.getByTestId('concurrency-refresh').click();
		await expect(row).toHaveCount(1, { timeout: 2_000 });
	}).toPass({ timeout: 60_000 });
	await expect(row.locator('[data-label="Holding"]')).toHaveText('1');
	await expect(row.locator('[data-label="Waiting"]')).toHaveText('1');
	await expect(row.getByTestId('concurrency-row-state')).toHaveText('OK');
	await axe(page, 'the keys');
	await shot(page, 'keys-light.png');
	await page.setViewportSize({ width: 390, height: 844 });
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
	await shot(page, 'keys-phone.png');
	await page.setViewportSize({ width: 1280, height: 800 });
	await addListFilter(page, 'concurrency', { field: 'key', op: OP.prefix, value: 'no-such-key' });
	await page.getByTestId('concurrency-filters-apply').click();
	await expect(page.getByTestId('concurrency-empty')).toHaveText('No key matches these filters.');
	await expect(row).toHaveCount(0);
	await page.goto(`${ENGINE}/admin/concurrency?ns=default&key=${encodeURIComponent(key)}`);
	await expect(page.getByTestId('concurrency-holder')).toHaveCount(1);
	await page.goto(`${ENGINE}/admin/concurrency?ns=default&key=unused-${STAMP}`);
	await expect(page.getByTestId('concurrency-unused')).toContainText('No run holds or waits for this key now.');
	await expect(page.getByTestId('concurrency-error')).toHaveCount(0);
	note(`C5 key ${key}: ${first} holds, ${second} waits`);

	// C4 — on the queued run's open page, the holder ends; the queued run
	// takes its slot and its card goes without a reload. The card still
	// showing right before the cancel is the positive control.
	await page.goto(`${ENGINE}/runs/${second}?ns=default`);
	await expect(card).toBeVisible({ timeout: 30_000 });
	await rpc('RunService/CancelRun', { runId: first, namespace: 'default', reason: 'concurrency e2e: free the slot' });
	await expect(page.getByTestId('task-hold-status')).toHaveText('Running', { timeout: 120_000 });
	await expect(page.getByTestId('run-queued')).toHaveCount(0, { timeout: 30_000 });
	note(`C4 ${second} started once ${first} ended, its card gone live`);
});

interface Fire {
	runId?: string;
	skipCode?: string;
	skipMessage?: string;
}

test('a fire a skip limit keeps from starting a run reads skipped, with why', async ({ page }) => {
	test.setTimeout(300_000);
	scheduled = { name: 'nightly', orkflow: SKIPPER };
	// Paused, so only this test fires it; overlapping fires are allowed, so
	// what keeps the second from starting is the limit.
	await rpc('ScheduleService/CreateSchedule', {
		namespace: 'default',
		name: 'nightly',
		orkflowName: SKIPPER,
		spec: { cron: ['0 2 * * *'], timezone: 'UTC', overlap: 'OVERLAP_POLICY_ALLOW_ALL', pauseOnFailure: true },
		paused: true
	});
	const fires = async () =>
		(await rpc<{ schedule?: { recentFires?: Fire[] } }>('ScheduleService/GetSchedule', { namespace: 'default', name: 'nightly' })).schedule?.recentFires ?? [];
	const firstFireAt = Date.now();
	await rpc('ScheduleService/FireSchedule', { namespace: 'default', name: 'nightly' });
	await waitFor('the first fire to start its run', async () => !!(await fires())[0]?.runId);
	const holder = (await fires())[0].runId!;
	started.push(holder);
	await waitFor('that run to hold the key', async () =>
		((await rpc<{ limit?: { holders?: { runId: string }[] } }>('ConcurrencyService/GetConcurrencyLimit', { namespace: 'default', key: SKIP_KEY })).limit?.holders ?? []).some(
			(h) => h.runId === holder
		)
	);
	// The cluster takes one manual fire a second.
	await waitFor('the first fire\'s second to pass', async () => Date.now() - firstFireAt > 1_500);
	await rpc('ScheduleService/FireSchedule', { namespace: 'default', name: 'nightly' });
	await waitFor('the second fire to be skipped', async () => !!(await fires())[1]?.skipCode);
	const skipped = (await fires())[1];
	expect(skipped.skipCode).toBe('FIRE_SKIPPED_CONCURRENCY');
	expect(skipped.runId ?? '').toBe('');
	expect(skipped.skipMessage).toMatch(/\S/);

	await page.goto(`${ENGINE}/orkflows/${SKIPPER}?ns=default&tab=schedules`);
	await expect(async () => {
		await page.reload();
		await expect(page.getByTestId('schedule-row-last-run-nightly')).toHaveText('skipped', { timeout: 2_000 });
	}).toPass({ timeout: 60_000 });
	await page.getByTestId('schedule-expand-nightly').click();
	const recent = page.getByTestId('schedule-recent-nightly');
	await expect(recent.locator('tbody tr')).toHaveCount(2);
	// Oldest first, as the CLI prints them.
	await expect(recent.locator('tbody tr').nth(0).getByRole('link', { name: holder })).toBeVisible();
	await expect(recent.locator('tbody tr').nth(0).getByTestId('schedule-fire-note')).toHaveCount(0);
	await expect(recent.locator('tbody tr').nth(1).getByTestId('schedule-fire-skipped')).toHaveText('skipped');
	await expect(recent.locator('tbody tr').nth(1).getByTestId('schedule-fire-note')).toHaveText(`FIRE_SKIPPED_CONCURRENCY: ${skipped.skipMessage}`);
	await expect(recent).not.toContainText('preparing');
	await axe(page, 'a skipped fire');
	await shot(page, 'schedule-skipped.png');

	// Its key holds a slash and a plus, and still has a page of its own.
	await page.goto(`${ENGINE}/admin/concurrency?ns=default&key=${encodeURIComponent(SKIP_KEY)}`);
	await expect(page.getByTestId('concurrency-key')).toHaveText(SKIP_KEY);
	await expect(page.getByTestId('concurrency-holder').getByRole('link', { name: holder, exact: true })).toBeVisible();
	await expect(page.getByTestId('concurrency-holder').locator('[data-label="On limit"]')).toHaveText('skip');
	await expect(page.getByTestId('concurrency-line-empty')).toBeVisible();
	note(`C6 ${skipped.skipCode}: ${skipped.skipMessage}`);
});
