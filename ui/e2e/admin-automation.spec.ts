import { expect, test } from './test';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { createServer, type IncomingHttpHeaders, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
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

// Admin → Automation, the Triggers tab's edit and the Webhooks page's edit, in
// a browser, against the real engine, Orker, Temporal and MinIO
// (docs/design/proposals/phase-4/declarative-automation §9).
//
// What is measured here:
//   1. Export shows the namespace's document and names its unnamed objects
//      with the command that names them;
//   2. Plan lists every step with its symbol, changes and digest, and writes
//      nothing (ListTriggers before and after are equal);
//   3. Apply creates a trigger and shows its token once — and that token
//      invokes (201) — and the next Plan reads "No changes.";
//   4. a plan that moved between review and Apply is not applied: the page
//      re-plans, shows the new plan and automation-stale, and the hand-made
//      trigger is still there;
//   4b. a plan that gained an ERROR since review (an Orkflow it names was
//      deleted) is not applied either, though its digest did not change;
//   5. --prune's deletions are named in the confirmation;
//   6. a webhook with secret_env needs its secret before Apply, and a real
//      run's delivery verifies with that secret;
//   7. renaming a trigger in place keeps its token;
//   8. editing a webhook's URL keeps its signing secret.
//
// The receiver is this spec's own node:http server on 127.0.0.1: the engine
// POSTs to it. Cleanup stops exactly the processes this spec started, by PID.

const ENGINE = 'http://127.0.0.1:18101';
const ENGINE_WS = 'ws://127.0.0.1:18101';
const UI_ORIGIN = 'http://127.0.0.1:14201';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-admin-automation-${STAMP}`;
const NS = `auto-${STAMP}`;
const ORKFLOW = `deploy-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-automation')
	: path.join(uiDir, 'test-results', 'admin-automation');
const out = proofOut(PROOF_DIR);
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const transcript: string[] = [];
const note = (line: string) => transcript.push(line);

function orkflowYaml(): string {
	return `name: ${ORKFLOW}
version: "0.1.0"
description: Declarative automation browser proof.
tasks:
  - id: deploy
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo deploying
        printf '{}' > /orkstack/output/values.json
`;
}

function header(): string {
	return `apiVersion: orkstack/v1\nkind: Automation\nnamespace: ${NS}\n`;
}

interface Received {
	path: string;
	headers: IncomingHttpHeaders;
	body: string;
}
interface TriggerRow {
	id: string;
	name?: string;
	description?: string;
}

const received: Received[] = [];
let receiver: Server;
let receiverURL = '';
const mine: ChildProcess[] = [];

async function listTriggers(): Promise<TriggerRow[]> {
	const answer = await rpc<{ triggers?: TriggerRow[] }>('TriggerService/ListTriggers', { namespace: NS });
	return answer.triggers ?? [];
}

async function invoke(id: string, token: string): Promise<number> {
	const response = await fetch(`${ENGINE}/v1/triggers/${id}`, {
		method: 'POST',
		headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
		body: '{}'
	});
	note(`POST /v1/triggers/${id} -> ${response.status}`);
	return response.status;
}

function verifies(secret: string, r: Received): boolean {
	const want = 'sha256=' + createHmac('sha256', secret).update(r.body).digest('hex');
	return r.headers['x-ork-signature'] === want;
}

async function stopMine(): Promise<void> {
	for (const child of mine) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 10_000;
	for (const child of mine) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18101, 14201]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	receiver = createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on('data', (chunk: Buffer) => chunks.push(chunk));
		req.on('end', () => {
			received.push({ path: req.url ?? '', headers: req.headers, body: Buffer.concat(chunks).toString('utf8') });
			res.writeHead(200).end();
		});
	});
	await new Promise<void>((resolve) => receiver.listen(0, '127.0.0.1', resolve));
	receiverURL = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}`;

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	mine.push(
		spawnLogged(out('engine.log'), './bin/ork', [], {
			...catalogEnv,
			...cluster,
			...s3Env,
			ORK_HTTP_ADDR: '127.0.0.1:18101',
			ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
		})
	);
	mine.push(spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('admin-automation') }));
	mine.push(
		spawnLogged(
			out('ui.log'),
			'node',
			['build'],
			{ HOST: '127.0.0.1', PORT: '14201', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
			uiDir
		)
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/admin/automation`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflowYaml(), namespace: NS });
});

test.afterAll(async () => {
	writeFileSync(out('transcript.txt'), transcript.join('\n') + '\n');
	const hooks = await rpc<{ webhooks?: { id: string; namespace?: string }[] }>('WebhookService/ListWebhooks', {}).catch(
		() => ({ webhooks: [] })
	);
	for (const hook of hooks.webhooks ?? []) {
		if (hook.namespace === NS) await rpc('WebhookService/DeleteWebhook', { id: hook.id }).catch(() => undefined);
	}
	await stopMine();
	await new Promise<void>((resolve) => (receiver ? receiver.close(() => resolve()) : resolve()));
});

async function planDocument(page: import('@playwright/test').Page, yaml: string, prune = false) {
	await page.goto(`${UI_ORIGIN}/admin/automation?ns=${NS}`);
	await page.getByTestId('automation-yaml').fill(yaml);
	if (prune) await page.getByTestId('automation-prune').check();
	await page.getByTestId('automation-plan').click();
	await expect(page.getByTestId('automation-plan-digest')).toBeVisible();
}

test('export shows the namespace\'s document and names unnamed objects', async ({ page }) => {
	const unnamed = await rpc<{ trigger: { id: string } }>('TriggerService/CreateTrigger', {
		namespace: NS,
		orkflowName: ORKFLOW
	});
	await page.goto(`${UI_ORIGIN}/admin/automation?ns=${NS}`);
	await expect(page.getByTestId('automation-scope')).toContainText('OrkStack does not store the document');
	await page.getByTestId('automation-export').click();
	const yaml = page.getByTestId('automation-export-yaml');
	await expect(yaml).toBeVisible();
	expect((await yaml.innerText()).startsWith('apiVersion: orkstack/v1')).toBe(true);
	const unmanaged = page.getByTestId('automation-unmanaged');
	await expect(unmanaged).toContainText(`orkctl trigger update ${unnamed.trigger.id} -n ${NS} --name NAME`);
	await expect(page.getByTestId('automation-export-download')).toHaveAttribute('download', 'automation.ork.yaml');
	await page.screenshot({ path: out('automation-export.png'), fullPage: true });
	note(`export named the unnamed trigger ${unnamed.trigger.id}`);
});

test('plan lists every step with its symbol and digest and writes nothing', async ({ page }) => {
	const before = await listTriggers();
	await planDocument(
		page,
		header() +
			`settings:\n  description: browser proof\ntriggers:\n  github-deploy:\n    orkflow: ${ORKFLOW}\n    description: GitHub Actions deploy hook\n`
	);
	const table = page.getByTestId('automation-plan-table');
	await expect(table).toBeVisible();
	await expect(page.getByTestId('automation-step-action-0')).toContainText('~ update');
	await expect(table).toContainText('description: "" → "browser proof"');
	await expect(table).toContainText('+ create');
	await expect(table).toContainText('description: "GitHub Actions deploy hook"');
	await expect(table).toContainText('! unmanaged');
	await expect(page.getByTestId('automation-unmanaged-section-schedule')).toContainText('schedules: not managed');
	await expect(page.getByTestId('automation-plan-summary')).toHaveText('Plan: 1 to create, 1 to update, 0 to delete.');
	await expect(page.getByTestId('automation-plan-digest')).toContainText(/sha256:[0-9a-f]{64}/);
	expect(await listTriggers()).toEqual(before);
	await page.screenshot({ path: out('automation-plan.png'), fullPage: true });
});

test('apply creates a trigger and shows its token once, and the token invokes', async ({ page }) => {
	const doc =
		header() +
		`settings:\n  description: browser proof\ntriggers:\n  github-deploy:\n    orkflow: ${ORKFLOW}\n    description: GitHub Actions deploy hook\n`;
	await planDocument(page, doc);
	await page.getByTestId('automation-apply').click();
	await expect(page.getByTestId('automation-apply-confirm')).toBeVisible();
	await page.getByTestId('automation-apply-accept').click();
	const secrets = page.getByTestId('automation-secrets-once');
	await expect(secrets).toContainText('Copy these now. They will not be shown again.');
	const id = (await page.getByTestId('automation-secret-id-github-deploy').innerText()).trim();
	const token = (await page.getByTestId('automation-secret-value-github-deploy').innerText()).trim();
	await expect(page.getByTestId('automation-apply-cli-trigger')).toBeVisible();
	await page.screenshot({ path: out('automation-applied-secrets.png'), fullPage: true });
	expect(await invoke(id, token)).toBe(201);

	await page.getByTestId('automation-plan').click();
	await expect(page.getByTestId('automation-no-changes')).toHaveText('No changes.');
	await expect(page.getByTestId('automation-secrets-once')).toHaveCount(0);
});

test('a plan that changed since review is not applied', async ({ page }) => {
	// The reviewed plan has one actionable step (a description edit), so
	// there is an Apply to press.
	await planDocument(
		page,
		header() + `triggers:\n  github-deploy:\n    orkflow: ${ORKFLOW}\n    description: changed for review\n`,
		true
	);
	await expect(page.getByTestId('automation-plan-summary')).toHaveText('Plan: 0 to create, 1 to update, 0 to delete.');
	// Drift between review and apply: a named trigger appears, so the fresh
	// plan holds a DELETE the shown one does not.
	const handMade = await rpc<{ trigger: { id: string } }>('TriggerService/CreateTrigger', {
		namespace: NS,
		orkflowName: ORKFLOW,
		name: 'hand-made'
	});
	await page.getByTestId('automation-apply').click();
	await page.getByTestId('automation-apply-accept').click();
	await expect(page.getByTestId('automation-stale')).toHaveText(
		'The namespace changed since this plan was made. Review the new plan; nothing was written.'
	);
	await expect(page.getByTestId('automation-plan-table')).toContainText('hand-made');
	expect((await listTriggers()).some((t) => t.id === handMade.trigger.id)).toBe(true);
	note(`stale plan refused; hand-made trigger ${handMade.trigger.id} kept`);
});

test('a plan that gained an ERROR since review is not applied', async ({ page }) => {
	// The fresh plan's digest covers only its actionable steps; an Orkflow a
	// step names that is deleted after review leaves the digest unchanged but
	// adds an ERROR. Apply must stop exactly as `--expect-plan` does.
	const doomed = `doomed-${STAMP}`;
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflowYaml().replace(`name: ${ORKFLOW}`, `name: ${doomed}`), namespace: NS });
	await planDocument(page, header() + `triggers:\n  doomed-hook:\n    orkflow: ${doomed}\n`);
	await expect(page.getByTestId('automation-plan-summary')).toHaveText('Plan: 1 to create, 0 to update, 0 to delete.');
	const before = await listTriggers();
	await rpc('OrkflowService/DeleteOrkflow', { name: doomed, namespace: NS });
	await page.getByTestId('automation-apply').click();
	await page.getByTestId('automation-apply-accept').click();
	await expect(page.getByTestId('automation-stale')).toBeVisible();
	await expect(page.getByTestId('automation-issues')).toContainText(`orkflow "${doomed}" has no stored version`);
	await expect(page.getByTestId('automation-apply')).toBeDisabled();
	const after = await listTriggers();
	expect(after.some((t) => t.name === 'doomed-hook')).toBe(false);
	expect(after.length).toBe(before.length);
	note(`plan with a new ERROR refused; no trigger created for deleted orkflow ${doomed}`);
});

test('prune deletions are named in the confirmation', async ({ page }) => {
	const target = (await listTriggers()).find((t) => t.name === 'hand-made');
	expect(target).toBeTruthy();
	await planDocument(
		page,
		header() + `triggers:\n  github-deploy:\n    orkflow: ${ORKFLOW}\n    description: GitHub Actions deploy hook\n`,
		true
	);
	await page.getByTestId('automation-apply').click();
	const dialog = page.getByTestId('automation-apply-confirm');
	await expect(dialog).toContainText(`trigger hand-made ${target!.id}`);
	await page.getByTestId('automation-apply-accept').click();
	await expect(page.getByTestId(/automation-step-outcome-/).first()).toContainText('applied');
	expect((await listTriggers()).some((t) => t.id === target!.id)).toBe(false);
});

test('a webhook with secret_env needs its secret before apply', async ({ page }) => {
	const secret = `typed-${STAMP}`;
	await planDocument(
		page,
		header() +
			`webhooks:\n  etl-loader:\n    url: ${receiverURL}/etl\n    orkflow: ${ORKFLOW}\n    events: [RUN_SUCCEEDED]\n    secret_env: ETL_WEBHOOK_SECRET\n`
	);
	await expect(page.getByTestId('automation-apply')).toBeDisabled();
	await page.getByTestId('automation-secret-env-etl-loader').fill(secret);
	await expect(page.getByTestId('automation-apply')).toBeEnabled();
	await page.getByTestId('automation-apply').click();
	await page.getByTestId('automation-apply-accept').click();
	await expect(page.getByTestId('automation-step-outcome-0')).toContainText('applied');

	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, namespace: NS });
	await expect
		.poll(() => received.filter((r) => r.path === '/etl' && r.body.includes(submitted.run.id)).length, {
			timeout: 120_000
		})
		.toBeGreaterThan(0);
	const delivery = received.find((r) => r.path === '/etl' && r.body.includes(submitted.run.id))!;
	expect(verifies(secret, delivery)).toBe(true);
	note(`delivery for ${submitted.run.id} verified with the secret typed for secret_env`);
});

test('renaming a trigger in place keeps its token', async ({ page }) => {
	const created = await rpc<{ trigger: { id: string }; secret: string }>('TriggerService/CreateTrigger', {
		namespace: NS,
		orkflowName: ORKFLOW
	});
	const id = created.trigger.id;
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&tab=triggers`);
	await expect(page.getByTestId(`trigger-name-${id}`)).toHaveText('unnamed');
	await page.getByTestId(`edit-trigger-${id}`).click();
	const dialog = page.getByTestId('trigger-edit-dialog');
	await expect(dialog).toContainText("Editing keeps this trigger's URL and token. Its orkflow cannot change.");
	await page.getByTestId('trigger-edit-name').fill('renamed-in-place');
	await page.getByTestId('trigger-edit-description').fill('named by the browser');
	await page.getByTestId('trigger-edit-submit').click();
	await expect(page.getByTestId(`trigger-name-${id}`)).toHaveText('renamed-in-place');
	await page.screenshot({ path: out('trigger-edit.png'), fullPage: true });
	expect(await invoke(id, created.secret)).toBe(201);
});

test('editing a webhook keeps its secret', async ({ page }) => {
	const registered = await rpc<{ webhook: { id: string }; secret: string }>('WebhookService/RegisterWebhook', {
		url: `${receiverURL}/first`,
		events: ['EVENT_TYPE_RUN_SUCCEEDED'],
		namespace: NS,
		orkflowName: ORKFLOW
	});
	const id = registered.webhook.id;
	await page.goto(`${UI_ORIGIN}/admin/webhooks`);
	await page.getByTestId(`edit-webhook-${id}`).click();
	const dialog = page.getByTestId('webhook-edit-dialog');
	await expect(dialog).toContainText("Editing keeps this subscription's id and signing secret.");
	await page.getByTestId('webhook-edit-name').fill('edited-hook');
	await page.getByTestId('webhook-edit-url').fill(`${receiverURL}/second`);
	await page.getByTestId('webhook-edit-submit').click();
	await expect(page.getByTestId(`webhook-name-${id}`)).toHaveText('edited-hook');
	await page.screenshot({ path: out('webhook-edit.png'), fullPage: true });

	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, namespace: NS });
	await expect
		.poll(() => received.filter((r) => r.path === '/second' && r.body.includes(submitted.run.id)).length, {
			timeout: 120_000
		})
		.toBeGreaterThan(0);
	const delivery = received.find((r) => r.path === '/second' && r.body.includes(submitted.run.id))!;
	expect(verifies(registered.secret, delivery)).toBe(true);
	note(`delivery to the edited URL for ${submitted.run.id} verified with the original secret`);
});
