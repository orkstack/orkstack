import { expect, test, type Page } from './test';
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
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('awaited-tasks');

// Awaited tasks against a real engine, Orker, Temporal and MinIO: the submit
// dialog discloses each callback token once and the run page never shows it;
// an awaiting task shows its deadline and a remaining time that counts down,
// and resolves from the page (typed fields) — "Delivered" until the run
// applies it, then SUCCEEDED and the downstream task reads the value; a
// rejection fails the run with AWAIT_REJECTED and its reason; and a callback
// posted over plain HTTP with the token from the one-time panel resolves the
// await while the page updates live from the event stream.
const ENGINE = 'http://127.0.0.1:18424';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('awaited-tasks');
const STAMP = Date.now().toString(36);
const NS = `awaits-${STAMP}`;
const NAME = 'awaited-approval';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'awaited-tasks')
	: path.join(uiDir, 'test-results', 'awaited-tasks');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

// The shell image writes values.json itself after every op (exec records
// {"exitCode"}, eval records {"value": stdout, "exitCode"}), so a task that
// wants a value downstream uses op: eval and prints it — a values.json the
// command writes is overwritten (images/activity-shell/README.md).
function workflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
description: Plan, wait for an approval, apply.
tasks:
  - id: plan
    uses: shell
    timeout_seconds: 120
    with:
      op: eval
      command: ["printf", "deploy staging"]
  - id: approval
    needs: [plan]
    await:
      timeout_seconds: 3600
      callback: true
      schema:
        approved: { type: boolean, required: true }
        approver: { type: string, required: true }
  - id: apply
    uses: shell
    needs: [approval]
    condition: "{{ .tasks.approval.output.approved }}"
    timeout_seconds: 120
    with:
      op: eval
      command: ["sh", "-c", "printf 'applied by %s' \\"{{ .tasks.approval.output.approver }}\\""]
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18424)) throw new Error('port 18424 already in use');

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ...uiEnv(), ORK_HTTP_ADDR: '127.0.0.1:18424'
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('awaited-tasks') });
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'awaited tasks' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow(), namespace: NS });
	note(`namespace ${NS}, orkflow ${NAME}@0.1.0, engine ${ENGINE}, ui ${UI}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

/** Submits from the orkflow's dialog and returns the run id and the token the one-time panel disclosed. */
async function submitWithCallback(page: Page): Promise<{ runId: string; token: string; command: string }> {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByTestId('run-submit-submit').click();
	await expect(page.getByTestId('await-callbacks')).toBeVisible();
	await expect(page.getByTestId('await-callbacks-once')).toHaveText(
		'Shown once: only a digest of each token is kept. Copy it now.'
	);
	await expect(page.getByTestId('await-callback-approval')).toBeVisible();
	const runId = ((await page.getByTestId('await-callbacks-run').textContent()) ?? '').trim();
	await page.getByTestId('await-callback-approval-cli-trigger').click();
	const command = ((await page.getByTestId('await-callback-approval-cli-command').textContent()) ?? '').trim();
	const token = /Bearer ([0-9a-f]{64})'/.exec(command)?.[1] ?? '';
	expect(token).toMatch(/^[0-9a-f]{64}$/);
	expect(command).toContain(`/v1/runs/${runId}/awaits/approval`);
	await shot(page, 'screenshot-callback-once.png');
	await page.getByTestId('await-callback-approval-cli-trigger').click();
	await page.getByTestId('await-callbacks-continue').click();
	await page.waitForURL(new RegExp(`/runs/${runId}`));
	return { runId, token, command };
}

async function waitAwaiting(page: Page) {
	await expect(page.getByTestId('task-approval-status')).toHaveText('AWAITING', { timeout: 120_000 });
}

test('submitting a run with a callback await shows each callback once', async ({ page }) => {
	const { runId, token } = await submitWithCallback(page);
	note(`submit ${runId}: one-time panel disclosed a 64-hex token for approval`);
	await waitAwaiting(page);
	const html = await page.content();
	expect(html).not.toContain(token);
	await page.getByTestId('task-approval-await-callback-cli-trigger').click();
	await expect(page.getByTestId('task-approval-await-callback-cli-command')).toContainText(
		'"Authorization: Bearer $ORKSTACK_CALLBACK_TOKEN"'
	);
	expect(await page.content()).not.toContain(token);
	await shot(page, 'screenshot-run-page-placeholder.png');
	await rpc('RunService/CancelRun', { runId, namespace: NS, reason: 'e2e cleanup' });
});

test('an awaiting task shows its deadline and resolves from the run page', async ({ page }) => {
	const { runId } = await submitWithCallback(page);
	await waitAwaiting(page);
	await expect(page.getByTestId('task-approval-await-state')).toHaveText('Waiting for external completion');
	await expect(page.getByTestId('task-approval-await-deadline')).not.toHaveText('—');
	const remaining = page.getByTestId('task-approval-await-remaining');
	await expect(remaining).toHaveText(/^in (59m|1h0m)$/);
	await expect(page.getByTestId('task-summary')).toContainText('1 awaiting');
	await shot(page, 'screenshot-awaiting.png');

	await page.getByTestId('task-approval-resolve').click();
	await expect(page.getByTestId('await-resolve-dialog')).toBeVisible();
	await page.getByTestId('declared-input-approved').check();
	await page.getByTestId('declared-input-approver').fill('e2e-operator');
	await shot(page, 'screenshot-resolve-dialog.png');
	await page.getByTestId('await-resolve-submit').click();
	await expect(page.getByTestId('await-resolve-dialog')).toHaveCount(0);
	// Delivered, never "resolved" on the RPC's word alone; it gives way to the
	// applied outcome once the stream says so.
	await expect(page.getByTestId('task-approval-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-await-state')).toHaveText(/^Completed via api/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('task-values-apply')).toContainText('e2e-operator', { timeout: 60_000 });
	note(`resolve from the page ${runId}: approval SUCCEEDED via api, apply read approver e2e-operator`);
	await shot(page, 'screenshot-resolved.png');
});

test('a rejection fails the run with AWAIT_REJECTED and its reason', async ({ page }) => {
	const { runId } = await submitWithCallback(page);
	await waitAwaiting(page);
	await page.getByTestId('task-approval-reject').click();
	await page.getByTestId('await-reject-reason').fill('change window closed');
	await page.getByTestId('await-reject-submit').click();
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-error')).toContainText('AWAIT_REJECTED');
	await expect(page.getByTestId('task-approval-error')).toContainText('change window closed');
	await expect(page.getByTestId('task-approval-await-state')).toHaveText(/^Rejected via api/);
	note(`reject from the page ${runId}: run FAILED AWAIT_REJECTED "change window closed"`);
	await shot(page, 'screenshot-rejected.png');
});

test('a callback posted over HTTP resolves the await', async ({ page, request }) => {
	const { runId, token } = await submitWithCallback(page);
	await waitAwaiting(page);
	const res = await request.post(`${ENGINE}/v1/runs/${runId}/awaits/approval`, {
		headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Ork-Principal': 'ci-bot' },
		data: '{"approved": true, "approver": "ci-bot"}'
	});
	expect(res.status()).toBe(202);
	note(`POST /v1/runs/${runId}/awaits/approval -> ${res.status()} ${(await res.text()).trim()}`);
	// No reload: the page follows the run's event stream.
	await expect(page.getByTestId('task-approval-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-await-state')).toHaveText('Completed via callback by ci-bot', {
		timeout: 30_000
	});
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await shot(page, 'screenshot-callback-resolved.png');
});

// A link clicked in the instant the run's await resolves still navigates. The
// run page re-reads itself on TASK_RESOLVED and again on the terminal event;
// SvelteKit gives the router to whichever started last, so a re-read begun
// while the reader's navigation was loading used to discard that navigation
// and leave the reader on the run page ($lib/settle). Holding the destination's
// load open makes the overlap certain instead of a matter of milliseconds: the
// await is resolved, and the run finishes, while the click is still loading.
test('a link clicked as the run resolves still navigates', async ({ page, request }) => {
	const { runId, token } = await submitWithCallback(page);
	await waitAwaiting(page);

	// The run page reads the versions list once, after it renders, and that
	// read has settled before this hold exists; the hold sees only the
	// navigation's read, so holding that one RPC holds the navigation without
	// touching anything the run page re-reads. The `clicked` flag is a second
	// guard: a read made before the click is continued untouched.
	await expect(
		page.locator('[data-testid="run-versions"][data-loaded="true"], [data-testid="run-versions-error"]')
	).toBeVisible();
	let clicked = false;
	let held!: () => void;
	const navigationHeld = new Promise<void>((resolve) => (held = resolve));
	let release!: () => void;
	const released = new Promise<void>((resolve) => (release = resolve));
	await page.route('**/orkstack.v1.OrkflowService/ListOrkflowVersions', async (route) => {
		if (!clicked) return route.continue();
		held();
		await released;
		await route.continue();
	});

	clicked = true;
	await page.getByTestId('pinned-orkflow-link').click();
	await navigationHeld;
	const res = await request.post(`${ENGINE}/v1/runs/${runId}/awaits/approval`, {
		headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Ork-Principal': 'ci-bot' },
		data: '{"approved": true, "approver": "ci-bot"}'
	});
	expect(res.status()).toBe(202);
	// The run page stays on screen until the navigation lands and keeps
	// following the stream, so these prove both events were handled while the
	// click was still loading.
	await expect(page.getByTestId('task-approval-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page).toHaveURL(new RegExp(`/runs/${runId}`));

	release();
	await expect(page).toHaveURL(new RegExp(`/orkflows/${NAME}\\?`));
	await expect(page.getByTestId('orkflow-submit-run')).toBeVisible();
	note(`link clicked while ${runId} resolved and finished: landed on /orkflows/${NAME}`);
	await shot(page, 'screenshot-link-during-resolution.png');
});
