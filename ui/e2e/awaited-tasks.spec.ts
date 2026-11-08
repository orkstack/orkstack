import { inspectTask, selectTask } from './uiHelpers';
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

// Two approvals in a row: the run ends only when the test resolves the
// second, so the test decides when the terminal event arrives.
const GATES = 'two-gates';
function gatesWorkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${GATES}
version: "0.1.0"
description: Two approvals in a row.
tasks:
  - id: first
    await:
      timeout_seconds: 3600
      schema:
        approver:
          type: string
          required: true
  - id: second
    needs: [first]
    await:
      timeout_seconds: 3600
      schema:
        approver:
          type: string
          required: true
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
	await rpc('OrkflowService/ApplyOrkflow', { yaml: gatesWorkflow(), namespace: NS });
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
	await expect(page.getByTestId('task-approval-status')).toHaveText('Awaiting', { timeout: 120_000 });
	// The page selects the awaiting task by itself; choosing it keeps its panel
	// (resolution, outcome) on screen after the await ends, when nothing is
	// left for the page to choose.
	await selectTask(page, 'approval');
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
	await expect(page.getByTestId('task-approval-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-await-state')).toHaveText(/^Completed via api/);
	// What it was resolved with is on the panel, as a rejection's reason is.
	await expect(page.getByTestId('task-approval-values')).toContainText('"approver": "e2e-operator"', { timeout: 30_000 });
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 120_000 });
	await inspectTask(page, 'apply');
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
	await expect(page.getByTestId('run-status')).toHaveText('Failed', { timeout: 60_000 });
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
	await expect(page.getByTestId('task-approval-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-await-state')).toHaveText('Completed via callback by ci-bot', {
		timeout: 30_000
	});
	await expect(page.getByTestId('task-approval-values')).toContainText('"approver": "ci-bot"', { timeout: 30_000 });
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 120_000 });
	await shot(page, 'screenshot-callback-resolved.png');
});

// The maintainer's case: the approval-gate example, resolved with these values.
test('resolved values: the approval-gate example shows what it was resolved with', async ({ page }) => {
	for (const name of ['notify-block', 'approval-gate']) {
		await rpc('OrkflowService/ApplyOrkflow', { yaml: readFileSync(path.join(uiDir, '..', 'examples', `${name}.ork.yaml`), 'utf8'), namespace: NS });
	}
	const started = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		namespace: NS,
		orkflowName: 'approval-gate',
		input: { data: { changes: [{ service: 'payments', setting: 'limit', value: '10' }], notify_url: 'http://127.0.0.1:9/post' } }
	});
	await page.goto(`${UI}/runs/${started.run.id}?ns=${NS}`);
	await waitAwaiting(page);
	await page.getByTestId('task-approval-resolve').click();
	await page.getByTestId('declared-input-approved').check();
	await page.getByTestId('declared-input-approver').fill('me');
	await page.getByTestId('await-resolve-submit').click();
	await expect(page.getByTestId('task-approval-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('task-approval-values')).toContainText('"approved": true', { timeout: 30_000 });
	await expect(page.getByTestId('task-approval-values')).toContainText('"approver": "me"');
	await shot(page, 'screenshot-approval-gate-values.png');
	await rpc('RunService/CancelRun', { namespace: NS, runId: started.run.id, reason: 'seen' }).catch(() => undefined);
});

// One await for each shape of value, between two rows that must not move.
const SHAPES = 'value-shapes';
function shapesWorkflow(): string {
	const gate = (id: string) => [`  - id: ${id}`, '    await:', '      timeout_seconds: 3600'].join('\n');
	return [
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${SHAPES}`,
		'version: "0.1.0"',
		'tasks:',
		gate('short'),
		gate('long'),
		gate('deep'),
		gate('deepest'),
		gate('big'),
		gate('after'),
		''
	].join('\n');
}
const deepValue = (): unknown => {
	let value: unknown = 'bottom';
	for (let i = 0; i < 300; i++) value = { d: value };
	return value;
};
const HOSTILE = '<img src=x onerror="window.__hit=1">\u0007\u001b[31mred';

test('resolved values: a long one stays closed until opened, reads as plain text, and moves nothing around it', async ({ page }) => {
	test.setTimeout(5 * 60_000);
	await rpc('OrkflowService/ApplyOrkflow', { yaml: shapesWorkflow(), namespace: NS });
	const { run } = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { namespace: NS, orkflowName: SHAPES });
	const resolve = async (taskId: string, values: unknown) =>
		await expect(async () => {
			await rpc('RunService/ResolveAwait', { namespace: NS, runId: run.id, taskId, valuesJson: JSON.stringify(values) });
		}).toPass({ timeout: 60_000 });
	await resolve('short', { approver: 'me', approved: true });
	await resolve('long', { note: 'x'.repeat(500), hostile: HOSTILE });
	await resolve('deep', deepValue());
	// As deep as 4 KiB of inline JSON can be: arrays, the shortest level there is.
	await resolve('deepest', JSON.parse(`{"a":${'['.repeat(2000)}${']'.repeat(2000)}}`));
	// Past the 4 KiB a run carries inline, so read by reference, on request; then past what a panel shows.
	await resolve('big', { blob: 'y'.repeat(200 * 1024) });

	for (const [width, height] of [[1280, 800], [2560, 1440]] as const) {
		await page.setViewportSize({ width, height });
		await page.goto(`${UI}/runs/${run.id}?ns=${NS}`);
		await expect(page.getByTestId('task-big-status')).toHaveText('Succeeded', { timeout: 60_000 });

		// A short one sits in the flow, whole, with nothing to open.
		await selectTask(page, 'short');
		const short = page.getByTestId('task-short-values');
		await expect(short).toContainText('{"approved": true, "approver": "me"}');
		await expect(short.locator('summary')).toHaveCount(0);
		const shortHeight = (await short.boundingBox())!.height;

		// A long one is closed: its text is not on the page, and the section is the same height.
		await selectTask(page, 'long');
		const long = page.getByTestId('task-long-values');
		const toggle = page.getByTestId('task-long-values-more');
		await expect(toggle).toBeVisible();
		await expect(page.getByTestId('task-long-values-full')).toBeHidden();
		expect(Math.abs((await long.boundingBox())!.height - shortHeight)).toBeLessThanOrEqual(1);

		// Opening moves only what is below it, and closing puts that back.
		const above = page.getByTestId('task-short');
		const below = page.getByTestId('task-after');
		// On the page, not the window: taking a picture scrolls it.
		const y = async (l: typeof above) => l.evaluate((el) => el.getBoundingClientRect().top + globalThis.scrollY);
		const [aboveAt, belowAt] = [await y(above), await y(below)];
		await toggle.click();
		const full = page.getByTestId('task-long-values-full');
		await expect(full).toBeVisible();
		expect(await y(above)).toBe(aboveAt);
		expect((await full.boundingBox())!.height).toBeLessThanOrEqual(241);
		await expect(full).toContainText('<img src=x');
		await expect(long.locator('img')).toHaveCount(0);
		expect(await page.evaluate(() => (window as unknown as { __hit?: number }).__hit)).toBeUndefined();
		for (const theme of ['light', 'dark'] as const) {
			await page.emulateMedia({ colorScheme: theme });
			await page.getByTestId('task-panel').screenshot({ path: out(`screenshot-values-long-${width}-${theme}.png`) });
		}
		await page.emulateMedia({ colorScheme: 'light' });
		await toggle.click();
		await expect(full).toBeHidden();
		expect(await y(below)).toBe(belowAt);

		// Deep JSON opens without trouble.
		await selectTask(page, 'deep');
		await page.getByTestId('task-deep-values-more').click();
		await expect(page.getByTestId('task-deep-values-full')).toContainText('"bottom"');
		await selectTask(page, 'deepest');
		await page.getByTestId('task-deepest-values-more').click();
		await expect(page.getByTestId('task-deepest-values-full')).toContainText('[');

		// A very large one asks for its bytes only when opened, and shows a bounded part of them.
		await selectTask(page, 'big');
		const read = page.waitForRequest((r) => r.url().includes('RunService/ReadArtifact'));
		await expect(page.getByTestId('task-big-values-more')).toContainText('KiB');
		await page.getByTestId('task-big-values-more').click();
		await read;
		const bigFull = page.getByTestId('task-big-values-full');
		await expect(bigFull).toContainText('yyyy');
		expect(((await bigFull.textContent()) ?? '').length).toBeLessThanOrEqual(64 * 1024 + 1);
		await expect(page.getByTestId('task-big-values-cut')).toContainText('first 64 KiB');
		expect((await bigFull.boundingBox())!.height).toBeLessThanOrEqual(241);
		await page.getByTestId('task-panel').screenshot({ path: out(`screenshot-values-big-${width}.png`) });
	}
	await page.emulateMedia({ colorScheme: 'light' });
	await rpc('RunService/ResolveAwait', { namespace: NS, runId: run.id, taskId: 'after', valuesJson: '{}' });
});

// A link clicked in the instant the run's await resolves still navigates. The
// run page re-reads itself on TASK_RESOLVED and again on the terminal event,
// and a re-read begun while the reader's navigation was loading once
// discarded that navigation and left the reader on the run page
// (#lib/settle.ts; SvelteKit 3 keeps the navigation by itself, and this pins
// that it still does). Holding the destination's
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
	// (The versions list is read only when the header's Versions popover is
	// opened, so there is no read of the run page's own to wait out.)
	await expect(page.getByTestId('run-dag')).toBeVisible();
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
	await expect(page.getByTestId('task-approval-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 120_000 });
	await expect(page).toHaveURL(new RegExp(`/runs/${runId}`));

	release();
	await expect(page).toHaveURL(new RegExp(`/orkflows/${NAME}\\?`));
	await expect(page.getByTestId('orkflow-submit-run')).toBeVisible();
	note(`link clicked while ${runId} resolved and finished: landed on /orkflows/${NAME}`);
	await shot(page, 'screenshot-link-during-resolution.png');
});

// A link that stays on the run page, clicked while the run is about to end,
// still leaves the page holding the run's final snapshot. The run page
// re-reads itself on the terminal event; a re-read begun while a navigation
// is loading is dropped by the router once that navigation lands (SvelteKit
// 3's client.js `_invalidate`), and the navigation's own load read the run
// before it ended, so the page must wait the navigation out and re-read after
// it (#lib/settle.ts). Every snapshot read is held, each as the engine
// answered it when it was made, so the overlap is certain: the navigation's
// read is made while the second approval still waits, and the run ends while
// that navigation is loading.
test('a same-page link loading as the run ends still shows its final values', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: GATES, namespace: NS });
	const runId = submitted.run.id;
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('task-first-status')).toHaveText('Awaiting', { timeout: 120_000 });
	await expect(page.getByTestId('run-dag')).toBeVisible();

	let armed = false;
	let landed = false;
	const held: (() => void)[] = [];
	await page.route('**/orkstack.v1.RunService/GetRun', async (route) => {
		if (!armed) return route.continue();
		const response = await route.fetch();
		if (!landed) await new Promise<void>((release) => held.push(release));
		await route.fulfill({ response });
	});
	armed = true;
	// A failed assertion below must not leave the page's reads pending.
	const releaseAll = () => {
		landed = true;
		armed = false;
		for (const release of held) release();
	};
	try {
		// The first approval's TASK_RESOLVED starts a re-read; its read is held.
		await rpc('RunService/ResolveAwait', {
			runId, namespace: NS, taskId: 'first', valuesJson: '{"approver": "first-operator"}'
		});
		await expect.poll(() => held.length, { timeout: 60_000 }).toBeGreaterThan(0);
		const beforeClick = held.length;
		// That re-read is still loading, so the click's navigation re-runs the
		// page's load, and its read of the run is held too: the second approval
		// is still waiting when it is made.
		await page.getByTestId('events-toggle').click();
		await expect.poll(() => held.length, { timeout: 30_000 }).toBeGreaterThan(beforeClick);
		const navigationReads = held.length;

		// The run ends while the navigation is loading. The second approval's
		// status comes from the event stream, which nothing holds.
		await expect(page.getByTestId('task-second-status')).toHaveText('Awaiting', { timeout: 60_000 });
		await rpc('RunService/ResolveAwait', {
			runId, namespace: NS, taskId: 'second', valuesJson: '{"approver": "ci-bot"}'
		});
		await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 120_000 });

		// The navigation lands on the snapshot it read before the run ended.
		for (const release of held.slice(0, navigationReads)) release();
		await expect(page).toHaveURL(/[?&]events=1/);
		// Every read from here on is answered as it is made; a re-read the router
		// would drop was begun while the navigation loaded, and is held until now.
		landed = true;
		armed = false;
		for (const release of held.slice(navigationReads)) release();

		await expect(page.getByTestId('task-second-status')).toHaveText('Succeeded');
		await inspectTask(page, 'second');
		await expect(page.getByTestId('task-values-second')).toContainText('ci-bot', { timeout: 60_000 });
		note(`same-page link while ${runId} ended: ${navigationReads} reads held until the navigation landed, final values shown`);
		await shot(page, 'screenshot-same-page-link-as-run-ends.png');
	} finally {
		releaseAll();
	}
});
