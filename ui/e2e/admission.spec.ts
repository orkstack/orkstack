import { expect, test, type Page } from './test';
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
	waitForEngine,
	openNamespaceEdit
} from './harness';
import { adminNamespaceUrl } from './uiHelpers';

// Namespace admission control in a browser, against a real engine, Orker,
// Temporal and MinIO:
//
//   1. Admin → Namespaces shows a namespace as `accepting` and `unlimited`,
//      Hold… closes it with a reason, and the row then reads
//      `since … — "reason"`;
//   2. the Orkflows list and the dashboard of that namespace carry the hold
//      banner with the reason and the exact release command;
//   3. the Submit a run dialog carries it too, with the submit disabled — on
//      the strength of the server's own state, read fresh;
//   4. Release hold reopens it: the banner is gone and a submit succeeds and
//      reaches SUCCEEDED;
//   5. a max_running of 1 with one slow run in flight: the dialog's submit
//      is refused by the server, and its sentence is shown verbatim; after
//      the slow run is cancelled, a submit is accepted;
//   6. two tabs: a hold written from a second browser context after the
//      first opened the edit form makes the first's save a stale-etag
//      conflict, never a silent overwrite of the hold.
//
// Isolation (AGENTS.md §11.1): this spec's own Temporal namespace and its own
// OrkStack namespace, on ports no other spec here uses.
const ENGINE = 'http://127.0.0.1:18095';
const ENGINE_WS = 'ws://127.0.0.1:18095';
const UI_ORIGIN = 'http://127.0.0.1:14189';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-admission-${STAMP}`;
const NS = `admission-${STAMP}`;
const FAST = 'reconcile';
const SLOW = 'reconcile-slow';
const REASON = 'partner CI loop, INC-4412';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admission')
	: path.join(uiDir, 'test-results', 'admission');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function orkflow(name: string, command: string): string {
	return `name: ${name}
version: "0.1.0"
description: Namespace admission coverage.
tasks:
  - id: work
    uses: shell
    timeout_seconds: 300
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "${command}"]
`;
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18095, 14189]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

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
		ORK_HTTP_ADDR: '127.0.0.1:18095',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('admission') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14189', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow(FAST, 'echo reconciled'), namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow(SLOW, 'sleep 240'), namespace: NS });
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, orkstack namespace ${NS}`);
});

test.afterAll(async () => {
	await killStarted();
});

// Admin → Namespaces filtered to NS: the list is paged, and the filter is how
// a row is reached whichever page it sorts onto.
async function openAdmin(page: Page): Promise<void> {
	await page.goto(adminNamespaceUrl(UI_ORIGIN, NS, NS));
	await expect(page.getByTestId(`namespace-row-${NS}`)).toBeVisible();
}

async function openSubmitDialog(page: Page, name: string): Promise<void> {
	await page.goto(`${UI_ORIGIN}/orkflows/${name}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
}

async function runningCount(): Promise<number> {
	const answer = await rpc<{ runs?: unknown[] }>('RunService/ListRuns', {
		namespace: NS,
		filters: [{ field: 'status', op: 'FILTER_OPERATOR_EQUALS', values: ['RUNNING'] }]
	});
	return (answer.runs ?? []).length;
}

test('1–3: Hold… closes the namespace, and every place a submit starts says so', async ({ page }) => {
	await openAdmin(page);
	await expect(page.getByTestId(`namespace-hold-${NS}`)).toHaveText('accepting');
	await expect(page.getByTestId(`namespace-max-running-${NS}`)).toHaveText('unlimited');

	await page.getByTestId(`namespace-hold-action-${NS}`).click();
	await expect(page.getByTestId('namespace-hold-confirm')).toBeVisible();
	// The accept stays disabled while the reason is blank.
	await expect(page.getByTestId('namespace-hold-confirm-accept')).toBeDisabled();
	await page.getByTestId('namespace-hold-reason-input').fill(REASON);
	await expect(page.getByTestId('namespace-hold-confirm-accept')).toBeEnabled();
	await page.getByTestId('namespace-hold-cli-trigger').click();
	await expect(page.getByTestId('namespace-hold-cli-command')).toHaveText(
		`orkctl namespace update ${NS} --hold '${REASON}'`
	);
	await page.screenshot({ path: out('screenshot-hold-dialog.png'), fullPage: true });
	await page.getByTestId('namespace-hold-cli-trigger').click();
	await page.getByTestId('namespace-hold-confirm-accept').click();
	await expect(page.getByTestId('namespace-hold-confirm')).toHaveCount(0);

	const cell = page.getByTestId(`namespace-hold-${NS}`);
	await expect(cell).toContainText('since ');
	await expect(cell).toContainText(`— "${REASON}"`);
	await expect(page.getByTestId(`namespace-row-${NS}`)).toHaveAttribute('data-held', 'true');
	await expect(page.getByTestId(`namespace-release-hold-${NS}`)).toBeVisible();
	note(`admin row after Hold…: ${await cell.innerText()}`);
	await page.screenshot({ path: out('screenshot-admin-held-row.png'), fullPage: true });

	// 2. The Orkflows list and the dashboard of that namespace.
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}`);
	const banner = page.getByTestId('orkflows-hold-banner');
	await expect(banner).toBeVisible();
	await expect(banner).toContainText(`"${REASON}"`);
	await page.getByTestId('orkflows-hold-banner-cli-trigger').click();
	await expect(page.getByTestId('orkflows-hold-banner-cli-command')).toHaveText(
		`orkctl namespace update ${NS} --release-hold`
	);
	await page.screenshot({ path: out('screenshot-orkflows-hold-banner.png'), fullPage: true });
	await page.getByTestId('orkflows-hold-banner-cli-trigger').click();

	await page.goto(`${UI_ORIGIN}/?ns=${NS}`);
	await expect(page.getByTestId('dashboard-hold-banner')).toContainText(`"${REASON}"`);
	await page.screenshot({ path: out('screenshot-dashboard-hold-banner.png'), fullPage: true });

	// Another namespace carries no banner.
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await expect(page.getByTestId('orkflows-hold-banner')).toHaveCount(0);

	// 3. The Submit a run dialog: banner, submit disabled.
	await openSubmitDialog(page, FAST);
	await expect(page.getByTestId('submit-hold-banner')).toContainText(`"${REASON}"`);
	await expect(page.getByTestId('run-submit-submit')).toBeDisabled();
	await page.screenshot({ path: out('screenshot-submit-hold-banner.png'), fullPage: true });

	// The server is the gate: a submit forced past the disabled button is
	// refused with the same reason.
	const forced = await fetch(`${ENGINE}/orkstack.v1.RunService/SubmitRun`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ orkflowName: FAST, namespace: NS })
	});
	const forcedBody = await forced.text();
	note(`forced SubmitRun while held: ${forced.status} ${forcedBody}`);
	expect(forced.status).toBe(400);
	expect(forcedBody).toContain('failed_precondition');
	expect(forcedBody).toContain(REASON);
	expect(await runningCount()).toBe(0);
});

test('4: Release hold reopens the namespace, and a submit reaches SUCCEEDED', async ({ page }) => {
	await openAdmin(page);
	await page.getByTestId(`namespace-release-hold-${NS}`).click();
	await expect(page.getByTestId('namespace-release-hold-confirm')).toBeVisible();
	await expect(page.getByTestId('namespace-release-hold-reason')).toHaveText(`"${REASON}"`);
	await page.getByTestId('namespace-release-hold-cli-trigger').click();
	await expect(page.getByTestId('namespace-release-hold-cli-command')).toHaveText(
		`orkctl namespace update ${NS} --release-hold`
	);
	await page.getByTestId('namespace-release-hold-cli-trigger').click();
	await page.getByTestId('namespace-release-hold-confirm-accept').click();
	await expect(page.getByTestId(`namespace-hold-${NS}`)).toHaveText('accepting');

	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}`);
	await expect(page.getByTestId('orkflows-hold-banner')).toHaveCount(0);

	await openSubmitDialog(page, FAST);
	await expect(page.getByTestId('submit-hold-banner')).toHaveCount(0);
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note(`after release: ${page.url()} reached SUCCEEDED`);
});

test('5: at max_running the dialog shows the server’s cap sentence verbatim', async ({ page }) => {
	await openAdmin(page);
	await openNamespaceEdit(page, NS);
	await page.getByTestId(`ns-edit-max-running-${NS}`).fill('1');
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-max-running-${NS}`)).toHaveText('1');

	const slow = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: SLOW, namespace: NS });
	note(`slow run ${slow.run.id} submitted under max_running 1`);
	// Admission counts from the cluster's execution index; wait until it
	// lists the slow run, so the refusal below is about the cap and nothing
	// else.
	await waitFor('the execution index lists the slow run in flight', async () => (await runningCount()) === 1);

	await openSubmitDialog(page, FAST);
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByTestId('run-submit-submit').click();
	const sentence =
		`namespace "${NS}" already has 1 runs in flight, which is its max_running. ` +
		`Wait for one to finish, or raise the cap with \`orkctl namespace update ${NS} --max-running N\``;
	await expect(page.getByTestId('run-submit-error')).toContainText(sentence);
	note(`cap refusal shown: ${await page.getByTestId('run-submit-error').innerText()}`);
	await page.screenshot({ path: out('screenshot-submit-max-running-refusal.png'), fullPage: true });

	await rpc('RunService/CancelRun', { runId: slow.run.id, namespace: NS, reason: 'admission e2e' });
	await waitFor('the slow run leaves flight', async () => (await runningCount()) === 0, 180_000);

	await openSubmitDialog(page, FAST);
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note(`after the slow run was cancelled: ${page.url()} admitted and SUCCEEDED`);
});

test('6: a hold from a second tab turns the first tab’s save into a stale-etag conflict', async ({ page, browser }) => {
	await openAdmin(page);
	await openNamespaceEdit(page, NS);
	await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toBeVisible();
	await page.getByTestId(`ns-edit-description-${NS}`).fill('edited in the first tab');

	const second = await browser.newContext();
	try {
		const other = await second.newPage();
		await openAdmin(other);
		await other.getByTestId(`namespace-hold-action-${NS}`).click();
		await other.getByTestId('namespace-hold-reason-input').fill(REASON);
		await other.getByTestId('namespace-hold-confirm-accept').click();
		await expect(other.getByTestId(`namespace-hold-${NS}`)).toContainText(`"${REASON}"`);
	} finally {
		await second.close();
	}

	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-edit-conflict-${NS}`)).toBeVisible();
	await expect(page.getByTestId(`namespace-edit-conflict-${NS}`)).toContainText('changed since it was read');
	await page.screenshot({ path: out('screenshot-two-tabs-conflict.png'), fullPage: true });

	// The hold the second tab wrote is still there: nothing was overwritten.
	const stored = await rpc<{ namespace: { hold?: { reason: string } } }>('NamespaceService/GetNamespace', {
		name: NS
	});
	expect(stored.namespace.hold?.reason).toBe(REASON);
	note(`after the conflicting save, the stored hold is still "${stored.namespace.hold?.reason}"`);

	// Leave the namespace accepting.
	await rpc('NamespaceService/UpdateNamespace', {
		namespace: { ...stored.namespace, hold: undefined }
	});
});
