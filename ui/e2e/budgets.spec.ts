import { expect, test, type Page } from '@playwright/test';
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
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// Namespace budgets in a browser, against a real engine, Orker, Temporal and
// MinIO (namespace-budgets spec, "Playwright"):
//
//   1. Admin → Namespaces creates a namespace with an enforced budget of one
//      cpu-second per hour, and the Budget column says so;
//   2. a one-task run reserves more than that;
//   3. the dashboard's Usage card asks the budget's own period, its cpu bar is
//      in its danger tint, it says `enforced`, the caveat and the coverage
//      line are there, and its CLI equivalent is the gate's own question;
//   4. the Submit a run dialog shows the server's BUDGET_EXCEEDED sentence
//      verbatim;
//   5. Admin → Usage shows every namespace side by side, the budget's share
//      only in its own window;
//   6. unchecking Enforce makes it soft: a submit is accepted and succeeds;
//   7. a namespace with no budget reads `unbounded` as a word, with no bar.
//
// Isolation (AGENTS.md §11.1): this spec's own Temporal namespace and its own
// OrkStack namespaces (stamped per run, so no earlier run's usage is in their
// window), on ports no other spec here uses. The engine measures on every
// request (ORK_USAGE_CACHE_TTL=0). Only the processes this spec started are
// stopped afterwards.
const ENGINE = 'http://127.0.0.1:18421';
const ENGINE_WS = 'ws://127.0.0.1:18421';
const UI_ORIGIN = 'http://127.0.0.1:14421';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE
	? `${process.env.ORK_TEMPORAL_NAMESPACE}-budgets-${STAMP}`
	: `ork-ui-budgets-${STAMP}`;
const NS = `budgets-${STAMP}`;
const FREE = `budgets-free-${STAMP}`;
const ORKFLOW = 'sleeper';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'budgets')
	: path.join(uiDir, 'test-results', 'budgets');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

const SLEEPER = `name: ${ORKFLOW}
version: "0.1.0"
description: Namespace budgets coverage.
tasks:
  - id: work
    uses: shell
    timeout_seconds: 120
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "sleep 2"]
`;

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18421, 14421]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18421',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN,
		ORK_USAGE_CACHE_TTL: '0'
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('budgets') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14421', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: FREE } });
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, budgeted namespace ${NS}, unbudgeted ${FREE}`);
});

test.afterAll(async () => {
	await killStarted();
});

async function awaitStatus(runId: string, status: string): Promise<void> {
	await waitFor(
		`run ${runId} reaches ${status}`,
		async () => {
			const got = await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId, namespace: NS });
			return got.run?.status === status;
		},
		180_000
	);
}

async function openSubmitDialog(page: Page): Promise<void> {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
}

test('1: Admin → Namespaces creates an enforced budget and the Budget column says so', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces`);
	await page.getByTestId('ns-create-name').fill(NS);
	await page.getByTestId('ns-create-budget-cpu-seconds').fill('1');
	await page.getByTestId('ns-create-budget-period-hours').fill('1');
	await page.getByTestId('ns-create-budget-enforce').check();
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${NS}.`);
	await expect(page.getByTestId(`namespace-budget-${NS}`)).toHaveText('1 cpu-s/1h0m0s enforced');
	await expect(page.getByTestId(`namespace-budget-${FREE}`)).toHaveText('unbounded');
	note(`Budget column of ${NS}: ${await page.getByTestId(`namespace-budget-${NS}`).innerText()}`);
	await page.getByTestId(`namespace-row-${NS}`).scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('screenshot-budget-column.png'), fullPage: true });
});

test('2–3: a run reserves past the budget, and the Usage card shows the danger bar and the caveat', async ({ page }) => {
	await rpc('OrkflowService/ApplyOrkflow', { yaml: SLEEPER, namespace: NS });
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, namespace: NS });
	await awaitStatus(submitted.run.id, 'STATUS_SUCCEEDED');
	await waitFor('the finished run is in the usage population', async () => {
		const usage = await rpc<{ runsInWindow?: string }>('StatsService/GetUsage', { namespace: NS, lookback: '3600s' });
		return Number(usage.runsInWindow ?? '0') >= 1;
	});
	note(`run ${submitted.run.id} SUCCEEDED and is counted`);

	await page.goto(`${UI_ORIGIN}/?ns=${NS}`);
	const card = page.getByTestId('usage');
	await expect(card).toBeVisible();
	await expect(page.getByTestId('usage-heading')).toHaveText("Usage — last 1h0m0s (the budget's period)");
	await expect(page.getByTestId('usage-bar-cpu')).toHaveAttribute('data-tone', 'danger');
	await expect(page.getByTestId('usage-budget-mode')).toHaveText('enforced — new runs are refused at the bound');
	await expect(page.getByTestId('usage-caveat')).toHaveText(
		'cpu-seconds and GiB-seconds are RESERVED cpu and memory × attempt time, not measured utilization.'
	);
	await expect(page.getByTestId('usage-footer')).toContainText('coverage begins ');
	await page.getByTestId('usage-cli-trigger').click();
	await expect(page.getByTestId('usage-cli-command')).toHaveText(`orkctl --namespace ${NS} stats usage --since 1h0m0s`);
	note(`usage card: bar ${await page.getByTestId('usage-bar-cpu-label').innerText()}; cpu-seconds ${await page.getByTestId('usage-cpu-seconds').innerText()}`);
	await card.scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('screenshot-usage-card-danger.png'), fullPage: true });
	await page.getByTestId('usage-cli-trigger').click();
});

test('4: a submit past the enforced budget shows the server’s sentence verbatim', async ({ page }) => {
	await openSubmitDialog(page);
	await page.getByTestId('run-submit-submit').click();
	const error = page.getByTestId('run-submit-error');
	await expect(error).toContainText(`namespace "${NS}" has reserved `);
	await expect(error).toContainText(' cpu-seconds over the last 1h0m0s, at or above its enforced budget of 1 (measured ');
	await expect(error).toContainText(`raise it with \`orkctl namespace update ${NS} --budget-cpu-seconds N\``);
	await expect(error).toContainText(`stop enforcing with \`orkctl namespace update ${NS} --budget-enforce=false\``);
	note(`refusal shown: ${await error.innerText()}`);
	await page.screenshot({ path: out('screenshot-submit-budget-refusal.png'), fullPage: true });
});

test('5: Admin → Usage lists every namespace; the budget’s share only in its own window', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/usage?window=24h`);
	await expect(page.getByTestId('usage-window-24h')).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId(`usage-ns-${NS}-budget`)).toHaveText('1 cpu-s per 1h0m0s (window differs)', { timeout: 120_000 });
	await expect(page.getByTestId(`usage-ns-${NS}-runs`)).toHaveText('1');
	await expect(page.getByTestId(`usage-ns-${FREE}-budget`)).toHaveText('unbounded', { timeout: 120_000 });
	await expect(page.getByTestId('usage-admin-caveat')).toContainText('RESERVED');
	await page.getByTestId('usage-admin-cli-trigger').click();
	await expect(page.getByTestId('usage-admin-cli-command')).toHaveText('orkctl stats usage --group-by namespace --since 24h');
	note(`admin usage row ${NS}: cpu-seconds ${await page.getByTestId(`usage-ns-${NS}-cpu-seconds`).innerText()}, budget ${await page.getByTestId(`usage-ns-${NS}-budget`).innerText()}`);
	await page.screenshot({ path: out('screenshot-admin-usage.png'), fullPage: true });
	await page.getByTestId('usage-admin-cli-trigger').click();
});

test('6: unchecking Enforce makes the budget soft, and a submit is accepted and succeeds', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=${NS}`);
	await page.getByTestId(`namespace-edit-${NS}`).click();
	await expect(page.getByTestId(`ns-edit-budget-enforce-${NS}`)).toBeChecked();
	await page.getByTestId(`ns-edit-budget-enforce-${NS}`).uncheck();
	await page.getByTestId(`ns-edit-cli-${NS}-trigger`).click();
	await expect(page.getByTestId(`ns-edit-cli-${NS}-command`)).toHaveText(
		`orkctl namespace update ${NS} --budget-cpu-seconds 1 --budget-gib-seconds 0 --budget-period 1h --budget-enforce=false`
	);
	await page.getByTestId(`ns-edit-cli-${NS}-trigger`).click();
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-budget-${NS}`)).toHaveText('1 cpu-s/1h0m0s soft');

	await openSubmitDialog(page);
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note(`soft budget: ${page.url()} admitted and SUCCEEDED`);

	await page.goto(`${UI_ORIGIN}/?ns=${NS}`);
	await expect(page.getByTestId('usage-budget-mode')).toHaveText('soft — reported, never refuses');
	await page.getByTestId('usage').scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('screenshot-usage-card-soft.png'), fullPage: true });
});

test('7: a namespace with no budget reads unbounded as a word, and no bar exists', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/?ns=${FREE}`);
	await expect(page.getByTestId('usage-heading')).toHaveText('Usage — this window');
	await expect(page.getByTestId('usage-unbounded')).toHaveText('unbounded');
	await expect(page.getByTestId('usage-bar-cpu')).toHaveCount(0);
	await expect(page.getByTestId('usage-bar-gib')).toHaveCount(0);
	await page.getByTestId('usage').scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('screenshot-usage-unbounded.png'), fullPage: true });
});
