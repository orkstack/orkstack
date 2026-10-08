import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
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

// What started a run, against a real engine, Orker, Temporal and MinIO: the
// submit dialog records a reference (a link only when it is an http(s) URL),
// Run again records run-again linked to its source, a trigger invoke records
// trigger with the trigger's description and the caller's header, a gateway's
// Ork-Principal is recorded and captioned as unauthenticated, the runs list
// filters by origin with the filter in the URL, and a run archived before
// origins existed reads "unrecorded" — never "submit".
const ENGINE = 'http://127.0.0.1:18097';
const ENGINE_WS = 'ws://127.0.0.1:18097';
const UI = 'http://127.0.0.1:14191';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-origin';
const STAMP = Date.now().toString(36);
const NS = `origin-${STAMP}`;
const NAME = 'deploy-service';
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-origin')
	: path.join(uiDir, 'test-results', 'run-origin');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

function workflow(): string {
	return `name: ${NAME}
version: "0.1.0"
description: Run origin coverage.
tasks:
  - id: deploy
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        printf '{}' > /orkstack/output/values.json
`;
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18097, 14191]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18097', ORK_WS_ALLOWED_ORIGINS: UI
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('run-origin') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14191', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'run origin' } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: workflow(), namespace: NS });
	note(`namespace ${NS}, orkflow ${NAME}@0.1.0, engine ${ENGINE}, ui ${UI}`);
});

test.afterAll(async () => { await killStarted(); });

/** Opens the orkflow's submit dialog, fills the origin reference, and submits. */
async function submitFromDialog(page: Page, reference: string, check?: (page: Page) => Promise<void>): Promise<string> {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('submit-origin-ref').fill(reference);
	if (check) await check(page);
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	return decodeURIComponent(new URL(page.url()).pathname.split('/').pop() ?? '');
}

async function invokeTrigger(id: string, secret: string, reference: string, principal = 'ci-github'): Promise<string> {
	const res = await fetch(`${ENGINE}/v1/triggers/${id}`, {
		method: 'POST',
		headers: {
			Authorization: `Bearer ${secret}`,
			'Content-Type': 'application/json',
			'Ork-Origin-Reference': reference,
			'Ork-Principal': principal
		},
		body: '{}'
	});
	const text = await res.text();
	note(`POST /v1/triggers/${id} Ork-Origin-Reference: ${reference} -> ${res.status} ${text.trim()}`);
	if (res.status !== 201) throw new Error(`invoke ${id} = ${res.status}: ${text}`);
	return (JSON.parse(text) as { run: { id: string } }).run.id;
}

test('a run submitted from the dialog with an origin reference shows submit, the reference as a link, and the CLI copy carries --origin-ref', async ({ page }) => {
	const reference = 'https://github.com/acme/svc/actions/runs/123';
	const runId = await submitFromDialog(page, reference, async (dialog) => {
		await dialog.getByTestId('run-submit-cli-trigger').click();
		await expect(dialog.getByTestId('run-submit-cli-command')).toContainText(`--origin-ref '${reference}'`);
		await expect(dialog.getByTestId('run-submit-cli-command')).toContainText(`orkctl run submit '${NAME}'`);
		await shot(dialog, 'screenshot-submit-dialog-origin-ref.png');
		await dialog.getByTestId('run-submit-cli-trigger').click();
	});
	await expect(page.getByTestId('run-origin-kind')).toHaveText('submit');
	const link = page.getByTestId('run-origin-reference');
	await expect(link).toHaveText(reference);
	expect(await link.evaluate((node) => node.tagName)).toBe('A');
	await expect(link).toHaveAttribute('href', reference);
	await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
	await expect(link).toHaveAttribute('target', '_blank');
	await expect(page.getByTestId('run-origin-principal')).toHaveCount(0);
	note(`dialog submit ${runId}: origin submit, reference link ${reference}`);
	await shot(page, 'screenshot-run-origin-submit.png');
});

test('a non-URL reference renders as text, not a link', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: NAME, namespace: NS, originReference: 'INC-4412'
	});
	await page.goto(`${UI}/runs/${submitted.run.id}?ns=${NS}`);
	const reference = page.getByTestId('run-origin-reference');
	await expect(reference).toHaveText('INC-4412');
	expect(await reference.evaluate((node) => node.tagName)).not.toBe('A');
	await expect(page.getByTestId('run-origin').locator('a[href="INC-4412"]')).toHaveCount(0);
	note(`rpc submit ${submitted.run.id} with reference INC-4412: rendered as text`);
});

test('Run again records run-again linked to the source', async ({ page }) => {
	const source = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME, namespace: NS });
	const sourceId = source.run.id;
	await page.goto(`${UI}/runs/${sourceId}?ns=${NS}`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await page.getByTestId('run-again').click();
	await expect(page.getByTestId('run-again-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-submit')).toBeEnabled();
	await page.getByTestId('run-submit-cli-trigger').click();
	await expect(page.getByTestId('run-submit-cli-command')).toContainText(`orkctl run again '${sourceId}'`);
	await expect(page.getByTestId('run-submit-cli-command')).not.toContainText('run submit');
	await page.getByTestId('run-submit-cli-trigger').click();
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL((url) => url.pathname.startsWith('/runs/') && !url.pathname.endsWith(sourceId));
	const rerunId = decodeURIComponent(new URL(page.url()).pathname.split('/').pop() ?? '');
	expect(rerunId).not.toBe(sourceId);
	await expect(page.getByTestId('run-origin-kind')).toHaveText('run-again');
	const link = page.getByTestId('run-origin-link');
	await expect(link).toHaveText(sourceId);
	await expect(link).toHaveAttribute('href', `/runs/${sourceId}?ns=${NS}`);
	note(`run again of ${sourceId} -> ${rerunId}, origin link /runs/${sourceId}`);
	await shot(page, 'screenshot-run-origin-run-again.png');
	await link.click();
	await expect(page.getByTestId('run-id')).toHaveText(sourceId);
});

test('a trigger-started run links to the Triggers tab and shows the description', async ({ page }) => {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}&tab=triggers`);
	await page.getByTestId('trigger-create-description').fill('GitHub deploy hook');
	await page.getByTestId('trigger-create-submit').click();
	await expect(page.getByTestId('trigger-secret-once')).toBeVisible();
	const triggerId = (await page.getByTestId('trigger-secret-id').textContent())?.trim() ?? '';
	const secret = (await page.getByTestId('trigger-secret-value').textContent())?.trim() ?? '';
	expect(triggerId).toMatch(/^trg-/);
	await page.getByTestId('trigger-secret-cli-trigger').click();
	await expect(page.getByTestId('trigger-secret-cli-command')).toContainText("# optional: -H 'Ork-Origin-Reference: <your CI job URL>'");
	await page.getByTestId('trigger-secret-cli-trigger').click();

	const reference = 'https://github.com/acme/svc/actions/runs/456';
	const runId = await invokeTrigger(triggerId, secret, reference);
	await page.goto(`${UI}/runs/${runId}?ns=${NS}`);
	await expect(page.getByTestId('run-origin-kind')).toHaveText('trigger');
	await expect(page.getByTestId('run-origin-description')).toHaveText('(GitHub deploy hook)');
	await expect(page.getByTestId('run-origin-reference')).toHaveAttribute('href', reference);
	await expect(page.getByTestId('run-origin-principal')).toHaveText('asserted by ci-github');
	const link = page.getByTestId('run-origin-link');
	await expect(link).toHaveText(triggerId);
	await shot(page, 'screenshot-run-origin-trigger.png');
	await link.click();
	await expect(page).toHaveURL(/tab=triggers/);
	await expect(page.getByTestId(`trigger-${triggerId}`)).toBeVisible();
	note(`trigger ${triggerId} invoked -> ${runId}; header links to the Triggers tab`);
});

test('an Ork-Principal header in front of the browser is recorded and captioned', async ({ browser }) => {
	const context = await browser.newContext({ extraHTTPHeaders: { 'Ork-Principal': 'e2e-operator' } });
	const page = await context.newPage();
	try {
		const runId = await submitFromDialog(page, 'INC-7');
		const principal = page.getByTestId('run-origin-principal');
		await expect(principal).toHaveText('asserted by e2e-operator');
		await expect(principal).toHaveAttribute(
			'title',
			"Ork-Principal as asserted by the deployment's gateway; OrkStack records it and does not authenticate it."
		);
		note(`dialog submit ${runId} behind Ork-Principal: e2e-operator`);
		await shot(page, 'screenshot-run-origin-principal.png');
	} finally {
		await context.close();
	}
});

test('the runs list filters by origin, the URL carries it, and reload keeps it', async ({ page }) => {
	const created = await rpc<{ trigger: { id: string }; secret: string }>('TriggerService/CreateTrigger', {
		orkflowName: NAME, namespace: NS, description: 'list filter'
	});
	const triggerId = created.trigger.id;
	const triggerRun = await invokeTrigger(triggerId, created.secret, 'LIST-1');
	const submitRun = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME, namespace: NS })).run.id;

	await page.goto(`${UI}/runs?ns=${NS}`);
	await expect(page.getByTestId(`run-row-${triggerRun}`)).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId(`run-row-${submitRun}`)).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId('run-filter-origin-id')).toBeDisabled();

	await selectOption(page, 'run-filter-origin', 'trigger');
	await page.getByTestId('run-filter-origin-id').fill(triggerId);
	await page.getByTestId('run-filters-apply').click();
	await expect(page).toHaveURL(new RegExp(`origin=trigger&originId=${triggerId}`));
	await expect(page.getByTestId(`run-row-${triggerRun}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-${submitRun}`)).toHaveCount(0);
	await expect(page.getByTestId(`run-row-origin-${triggerRun}`)).toHaveText(`trigger ${triggerId}`);
	await expect(page.locator('[data-testid^="run-row-origin-"]')).toHaveCount(1);
	await shot(page, 'screenshot-runs-filtered-by-trigger.png');

	await page.reload();
	await expect(page.getByTestId(`run-row-${triggerRun}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-${submitRun}`)).toHaveCount(0);
	await expect(page.getByTestId('run-filter-origin')).toContainText('Trigger');
	await expect(page.getByTestId('run-filter-origin-id')).toHaveValue(triggerId);

	await page.getByTestId('run-filters-clear').click();
	await expect(page.getByTestId(`run-row-${triggerRun}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-${submitRun}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-origin-${submitRun}`)).toHaveText('submit');
	expect(new URL(page.url()).searchParams.has('origin')).toBe(false);

	await page.goto(`${UI}/runs?ns=${NS}&originId=${triggerId}`);
	await expect(page.getByTestId('engine-error')).toContainText('An origin id needs an origin kind.');

	await page.goto(`${UI}/runs?ns=${NS}&origin=trigger&originId=trg-nothing-here`);
	await expect(page.getByTestId('runs-empty')).toContainText('No runs started by trigger trg-nothing-here match these filters');
	note(`list filter trigger:${triggerId} -> [${triggerRun}]; cleared -> both; bare id refused`);
});

test('a run recorded without an origin reads unrecorded', async ({ page }) => {
	const legacyId = `ork-${Date.now()}-legacy`;
	const output = execFileSync('go', ['run', './test/tools/archive-legacy-run', NS, NAME, '0.1.0', legacyId], {
		cwd: repoRoot,
		env: { ...process.env, ...s3Env }
	}).toString();
	note(output.trim());
	await page.goto(`${UI}/runs/${legacyId}?ns=${NS}`);
	const kind = page.getByTestId('run-origin-kind');
	await expect(kind).toHaveText('unrecorded');
	await expect(kind).toHaveAttribute('title', 'This run started before origins were recorded.');
	await expect(page.getByTestId('run-origin')).not.toContainText('submit');
	await shot(page, 'screenshot-run-origin-unrecorded.png');

	await page.goto(`${UI}/runs?ns=${NS}`);
	await expect(page.getByTestId(`run-row-origin-${legacyId}`)).toHaveText('unrecorded', { timeout: 60_000 });
	for (const slug of ['submit', 'trigger', 'schedule', 'parent-run', 'run-again']) {
		await page.goto(`${UI}/runs?ns=${NS}&origin=${slug}`);
		await expect(page.getByTestId('engine-error')).toHaveCount(0);
		await expect(page.getByTestId('run-filters')).toBeVisible();
		await expect(page.getByTestId(`run-row-${legacyId}`)).toHaveCount(0);
	}
	writeFileSync(out('unrecorded-run.txt'), `${legacyId}\n`);
	note(`pre-origin run ${legacyId}: unrecorded on the page and in the list, matched by no origin filter`);
});
