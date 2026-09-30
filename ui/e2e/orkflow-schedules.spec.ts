import { expect, test, type Page } from '@playwright/test';
import type { ChildProcess } from 'node:child_process';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import {
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
	uiDir,
	waitFor,
	waitForEngine
} from './harness';
import { selectOption } from './uiHelpers';

// Orkflow schedules, against a real engine, Orker, Temporal and MinIO: the
// Schedules tab creates a schedule the cluster keeps (next fire times and
// all), refuses a cron expression with the cluster's own words, pauses and
// resumes, fires now and links the run back to the tab, records a refused
// fire, deletes, and the runs list filters to one schedule's runs.
//
// This spec shares its host with other stacks, so it only ever signals the
// processes it started (killStarted), never a name pattern.
const ENGINE = 'http://127.0.0.1:18094';
const ENGINE_WS = 'ws://127.0.0.1:18094';
const UI = 'http://127.0.0.1:14192';
const TEMPORAL_NAMESPACE = `${process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-schedules'}-ui-schedules`;
const STAMP = Date.now().toString(36);
const NS = `sched-${STAMP}`;
const NAME = 'hello';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'orkflow-schedules')
	: path.join(uiDir, 'test-results', 'orkflow-schedules');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

let orker: ChildProcess | undefined;
const cluster = clusterEnv(TEMPORAL_NAMESPACE);

function startOrker(): void {
	orker = spawnLogged(out(`orker-${Date.now()}.log`), './bin/orker', [], {
		...cluster,
		...s3Env,
		...orkerEnv('orkflow-schedules')
	});
}

async function stopOrker(): Promise<void> {
	const child = orker;
	if (!child) return;
	if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	await waitFor('the Orker this spec started to exit', async () => child.exitCode !== null || child.signalCode !== null, 30_000);
	orker = undefined;
}

async function liveOrkers(): Promise<number> {
	const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
	return (answer.pools ?? []).reduce((sum, pool) => sum + (pool.liveOrkerCount ?? 0), 0);
}

function workflow(): string {
	return `name: ${NAME}
version: "0.1.0"
description: Orkflow schedules coverage.
tasks:
  - id: work
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        printf '{}' > /orkstack/output/values.json
`;
}

/** createSchedule creates a paused schedule over the API, for the scenarios that are not about the form. */
async function createSchedule(name: string, paused = true): Promise<string> {
	const created = await rpc<{ schedule: { id: string } }>('ScheduleService/CreateSchedule', {
		namespace: NS,
		name,
		orkflowName: NAME,
		spec: { cron: ['0 2 * * *'], timezone: 'Europe/Lisbon' },
		paused
	});
	note(`created schedule ${name} (${created.schedule.id}) over the API`);
	return created.schedule.id;
}

async function openTab(page: Page): Promise<void> {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}&tab=schedules`);
	await expect(page.getByTestId('schedules-scope')).toBeVisible();
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18094, 14192]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18094',
		ORK_ADMIN_ADDR: '127.0.0.1:0',
		ORK_WS_ALLOWED_ORIGINS: UI
	});
	startOrker();
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14192', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => (await liveOrkers()) > 0);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'orkflow schedules' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow(), namespace: NS });
	note(`temporal namespace ${TEMPORAL_NAMESPACE}, namespace ${NS}, orkflow ${NAME}@0.1.0, engine ${ENGINE}, ui ${UI}`);
});

test.afterAll(async () => {
	await killStarted();
});

test('the Schedules tab creates a schedule and shows its next fire times', async ({ page }) => {
	await openTab(page);
	await expect(page.getByTestId('schedules-scope')).toContainText(
		`Each schedule starts ${NAME} in ${NS} at the times it names. Temporal keeps the schedule; OrkStack stores nothing else about it.`
	);
	await expect(page.getByTestId('schedules-empty')).toBeVisible();
	await page.getByTestId('schedule-name').fill('nightly');
	await page.getByTestId('schedule-cron').fill('0 2 * * *');
	await page.getByTestId('schedule-timezone').fill('Europe/Lisbon');
	await selectOption(page, 'schedule-overlap', 'skip');
	await page.getByTestId('schedule-create-paused').check();
	await page.getByTestId('schedule-create').click();

	const row = page.getByTestId('schedule-row-nightly');
	await expect(row).toBeVisible();
	await expect(page.getByTestId('schedule-row-state-nightly')).toHaveText('paused');
	await expect(page.getByTestId('schedule-row-next-nightly')).toContainText(/T01:00:00Z|T00:00:00Z/);
	await page.getByTestId('schedule-expand-nightly').click();
	const fires = page.getByTestId('schedule-next-fires-nightly').locator('li');
	await expect(fires).toHaveCount(10);
	// 02:00 in Lisbon is 01:00Z in summer time and 02:00Z in winter.
	for (const text of await fires.allTextContents()) {
		expect(text.trim()).toMatch(/^\d{4}-\d{2}-\d{2}T0[12]:00:00Z$/);
	}
	await page.getByTestId('schedule-cli-nightly-trigger').click();
	await expect(page.getByTestId('schedule-cli-nightly-command')).toContainText(
		`orkctl schedule create nightly --orkflow ${NAME} --cron '0 2 * * *' --timezone Europe/Lisbon --overlap skip --paused`
	);
	note(`created nightly from the tab: paused, ${await fires.count()} next fire times, first ${(await fires.first().textContent())?.trim()}`);
	await shot(page, 'screenshot-schedules-paused-next-fires.png');
	await page.getByTestId('schedule-cli-nightly-trigger').click();
});

test("an invalid cron expression shows the cluster's refusal and creates nothing", async ({ page }) => {
	await openTab(page);
	await page.getByTestId('schedule-name').fill('broken');
	await page.getByTestId('schedule-cron').fill('0 2 * *');
	await page.getByTestId('schedule-create').click();
	const error = page.getByTestId('schedule-error');
	await expect(error).toBeVisible();
	await expect(error).toContainText(/cron|invalid/i);
	await expect(page.getByTestId('schedule-row-broken')).toHaveCount(0);
	note(`invalid cron refused: ${(await error.textContent())?.trim()}`);
	await shot(page, 'screenshot-schedules-invalid-cron.png');
});

test('pause with a reason and resume', async ({ page }) => {
	await createSchedule('weekly', false);
	await openTab(page);
	await expect(page.getByTestId('schedule-row-state-weekly')).toHaveText('active');
	await page.getByTestId('schedule-pause-weekly').click();
	await page.getByTestId('schedule-pause-reason').fill('Q3 close');
	await page.getByTestId('schedule-pause-accept').click();
	const state = page.getByTestId('schedule-row-state-weekly');
	await expect(state).toHaveText('paused');
	await expect(state).toHaveAttribute('title', 'Q3 close');
	await expect(page.getByTestId('schedule-row-note-weekly')).toHaveText('Q3 close');
	await shot(page, 'screenshot-schedules-paused-with-note.png');
	await page.getByTestId('schedule-resume-weekly').click();
	await expect(page.getByTestId('schedule-row-state-weekly')).toHaveText('active');
	note('weekly paused with "Q3 close", then resumed to active');
});

test('fire now starts a run that links back to the tab', async ({ page }) => {
	const id = await createSchedule('adhoc');
	await openTab(page);
	await page.getByTestId('schedule-fire-adhoc').click();
	await expect(page.getByTestId('schedule-fire-confirm')).toContainText('repeats its external side effects');
	await page.getByTestId('schedule-fire-accept').click();
	const lastRun = page.getByTestId('schedule-row-last-run-adhoc');
	await expect(async () => {
		await page.reload();
		await expect(lastRun.locator('a')).toBeVisible({ timeout: 2_000 });
	}).toPass({ timeout: 90_000 });
	const runId = ((await lastRun.locator('a').textContent()) ?? '').trim();
	expect(runId).toMatch(/^ork-/);
	await shot(page, 'screenshot-schedules-row-run-link.png');
	await lastRun.locator('a').click();
	await expect(page.getByTestId('run-id')).toHaveText(runId);
	await expect(page.getByTestId('run-origin-kind')).toHaveText('schedule');
	await expect(page.getByTestId('run-origin-description')).toHaveText('(adhoc)');
	const link = page.getByTestId('run-origin-link');
	await expect(link).toHaveText(id);
	await expect(link).toHaveAttribute('href', `/orkflows/${NAME}?tab=schedules&ns=${NS}`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await shot(page, 'screenshot-run-origin-schedule.png');
	await link.click();
	await expect(page).toHaveURL(/tab=schedules/);
	await expect(page.getByTestId('schedule-row-adhoc')).toBeVisible();
	note(`fired adhoc (${id}) -> ${runId}; run page origin schedule links to the Schedules tab`);
});

test('a refused fire reads refused with its reason', async ({ page }) => {
	await createSchedule('starved');
	await stopOrker();
	note('stopped the Orker this spec started; waiting for the pool to report no live Orker');
	await waitFor('the default pool to report no live Orker', async () => (await liveOrkers()) === 0, 150_000);
	try {
		await openTab(page);
		await page.getByTestId('schedule-fire-starved').click();
		await page.getByTestId('schedule-fire-accept').click();
		await expect(async () => {
			await page.reload();
			await expect(page.getByTestId('schedule-row-last-run-starved')).toHaveText('refused', { timeout: 2_000 });
		}).toPass({ timeout: 90_000 });
		await page.getByTestId('schedule-expand-starved').click();
		const recent = page.getByTestId('schedule-recent-starved');
		await expect(recent).toContainText('refused');
		await expect(recent).toContainText('no live Orker');
		note(`starved fire refused: ${(await recent.textContent())?.replace(/\s+/g, ' ').trim()}`);
		await shot(page, 'screenshot-schedules-refused-fire.png');
	} finally {
		startOrker();
		await waitFor('the default pool to report a live Orker again', async () => (await liveOrkers()) > 0);
		note('restarted the Orker');
	}
});

test('delete removes the row', async ({ page }) => {
	await openTab(page);
	const names = await page.locator('[data-testid^="schedule-row-state-"]').evaluateAll((nodes) =>
		nodes.map((node) => (node.getAttribute('data-testid') ?? '').replace('schedule-row-state-', ''))
	);
	for (const name of names) {
		await page.getByTestId(`schedule-delete-${name}`).click();
		await page.getByTestId('schedule-delete-accept').click();
		await expect(page.getByTestId(`schedule-row-${name}`)).toHaveCount(0);
	}
	await expect(page.getByTestId('schedules-empty')).toContainText('No schedules exist for');
	note(`deleted ${names.join(', ')}; the empty state reads no schedules exist`);
});

test('the runs list filters to one schedule', async ({ page }) => {
	const id = await createSchedule('listed');
	await rpc('ScheduleService/FireSchedule', { namespace: NS, name: 'listed' });
	let runId = '';
	await waitFor('the fire to name its run', async () => {
		const got = await rpc<{ schedule: { recentFires?: { runId?: string }[] } }>('ScheduleService/GetSchedule', {
			namespace: NS,
			name: 'listed'
		});
		runId = got.schedule.recentFires?.find((fire) => fire.runId)?.runId ?? '';
		return runId !== '';
	});
	const submitted = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME, namespace: NS })).run.id;

	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('run-origin-link')).toHaveText(id);
	await page.goto(`${UI}/runs?ns=${NS}`);
	await expect(page.getByTestId(`run-row-${runId}`)).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId(`run-row-${submitted}`)).toBeVisible({ timeout: 60_000 });
	await selectOption(page, 'run-filter-origin', 'schedule');
	await page.getByTestId('run-filter-origin-id').fill(id);
	await page.getByTestId('run-filters-apply').click();
	await expect(page).toHaveURL(new RegExp(`origin=schedule&originId=${id}`));
	await expect(page.getByTestId(`run-row-${runId}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-${submitted}`)).toHaveCount(0);
	await expect(page.getByTestId(`run-row-origin-${runId}`)).toHaveText(`schedule ${id}`);
	await expect(page.locator('[data-testid^="run-row-origin-"]')).toHaveCount(1);
	await shot(page, 'screenshot-runs-filtered-by-schedule.png');
	note(`list filter schedule:${id} -> [${runId}], not the submitted ${submitted}`);
	await rpc('ScheduleService/DeleteSchedule', { namespace: NS, name: 'listed' });
});
