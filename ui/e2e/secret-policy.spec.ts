import { expect, test, type Page } from './test';
import { createHash, randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	noteTo,
	openNamespaceEdit,
	orkerEnv,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';
import { adminNamespaceUrl } from './uiHelpers';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('secret-policy');

// Tenant-scoped secret policy in a browser, against a real engine, Orker,
// Temporal and MinIO:
//
//   1. Admin → Namespaces: a namespace created with an Allowed secrets entry
//      shows it in its row; one created with the field blank reads
//      `unrestricted`;
//   2. the form's prefilter refuses `file:/a/../b` without a request; a save
//      of two entries shows both;
//   3. the Apply page's Validate shows the refusal anchored to
//      tasks[0].secrets[0].source, and Apply is refused with the same
//      sentence and stores nothing;
//   4. a permitted source applies, runs and SUCCEEDS: the task's log pane
//      shows the token's sha256 and the page's HTML carries the token
//      nowhere;
//   5. with the list narrowed, the Submit a run dialog shows the server's
//      refusal verbatim;
//   6. two tabs: a list changed from a second context after the first opened
//      the edit form makes the first's save a stale-etag conflict.
//
// Isolation (AGENTS.md §11.1): this spec's own Temporal namespace and its own
// OrkStack namespaces, on ports no other spec here uses. It ends only the
// processes it started (killStarted), never anything by name, because other
// stacks share this host.
const ENGINE = 'http://127.0.0.1:18317';
const ENGINE_WS = 'ws://127.0.0.1:18317';
const UI_ORIGIN = 'http://127.0.0.1:14317';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-secret-policy-${STAMP}`;
const NS = `sandbox-${STAMP}`;
const OPEN_NS = `open-${STAMP}`;
const TOKEN = randomBytes(32).toString('hex');
const TOKEN_SHA256 = createHash('sha256').update(TOKEN).digest('hex');
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'secret-policy')
	: path.join(uiDir, 'test-results', 'secret-policy');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function orkflow(name: string, source: string, command: string): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "0.1.0"
description: Secret policy coverage.
tasks:
  - id: fetch
    uses: shell
    timeout_seconds: 300
    retry:
      maximum_attempts: 1
    secrets:
      - name: token
        source: ${source}
    with:
      op: exec
      command: ["sh", "-c", "${command}"]
`;
}

const EXFIL = orkflow('exfil', 'env:PROD_DB_PASSWORD', 'cat /orkstack/input/secrets/token');
const FETCH = orkflow('fetch', 'env:E2E_TOKEN', 'sha256sum /orkstack/input/secrets/token');

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18317, 14317]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18317',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('secret-policy'),
		ORK_ORKER_SECRET_ALLOW: 'env:E2E_*',
		E2E_TOKEN: TOKEN
	});
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14317', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, orkstack namespaces ${NS} and ${OPEN_NS}`);
	note('orker ORK_ORKER_SECRET_ALLOW=env:E2E_* with E2E_TOKEN minted by the spec (value never written here)');
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
	// The evidence must not leak what it proves is kept: neither the
	// transcript nor any of the processes' logs may carry the minted token.
	for (const file of ['transcript.txt', 'engine.log', 'orker.log', 'ui.log']) {
		const body = existsSync(out(file)) ? readFileSync(out(file), 'utf8') : '';
		expect(body.includes(TOKEN), `${file} carries the minted token`).toBe(false);
	}
	note(`grep -c <minted E2E_TOKEN> over transcript.txt, engine.log, orker.log, ui.log: 0`);
});

// Admin → Namespaces filtered to this spec's namespace: the list is paged, and
// the filter is how a row is reached whichever page it sorts onto. Before the
// namespace exists the filter matches nothing, so readiness is the filter
// itself, not the table.
async function openAdmin(page: Page): Promise<void> {
	await page.goto(adminNamespaceUrl(UI_ORIGIN, NS, NS));
	await expect(page.getByTestId('namespaces-filter-value-0')).toHaveValue(NS);
}

test('1: Admin → Namespaces shows the Allowed secrets column, set or unrestricted', async ({ page }) => {
	await openAdmin(page);
	await page.getByTestId('ns-create-open').click();
	await page.getByTestId('ns-create-name').fill(NS);
	await page.getByTestId('ns-create-allowed-secrets').fill('env:SANDBOX_*');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${NS}.`);
	await expect(page.getByTestId(`namespace-allowed-secrets-${NS}`)).toHaveText('env:SANDBOX_*');
	note(`${NS} row: ${await page.getByTestId(`namespace-allowed-secrets-${NS}`).innerText()}`);
	await page.screenshot({ path: out('screenshot-allowed-secrets-column.png'), fullPage: true });

	// Each create filters the list to the name it created.
	await page.getByTestId('ns-create-name').fill(OPEN_NS);
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${OPEN_NS}.`);
	await expect(page.getByTestId(`namespace-allowed-secrets-${OPEN_NS}`)).toHaveText('unrestricted');
	note(`${OPEN_NS} row: ${await page.getByTestId(`namespace-allowed-secrets-${OPEN_NS}`).innerText()}`);
});

test('2: the form refuses a traversal locally, and a save of two entries shows both', async ({ page }) => {
	await openAdmin(page);
	await openNamespaceEdit(page, NS);
	const field = page.getByTestId(`ns-edit-allowed-secrets-${NS}`);
	await expect(field).toHaveValue('env:SANDBOX_*');

	let updates = 0;
	page.on('request', (request) => {
		if (request.url().includes('NamespaceService/UpdateNamespace')) updates++;
	});
	await field.fill('file:/a/../b');
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	const error = page.getByTestId(`ns-edit-error-allowed-secrets-${NS}`);
	await expect(error).toContainText('"file:/a/../b"');
	await expect(error).toContainText('".." segment');
	expect(updates).toBe(0);
	note(`prefilter refusal (no request made): ${await error.innerText()}`);
	await page.screenshot({ path: out('screenshot-prefilter-refusal.png'), fullPage: true });

	await field.fill('env:SANDBOX_*\nenv:E2E_*');
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-allowed-secrets-${NS}`)).toHaveText('env:SANDBOX_*, env:E2E_*');
	expect(updates).toBe(1);
});

test('3: Validate anchors the refusal to its field, and Apply is refused and stores nothing', async ({ page }) => {
	const sentence = `namespace "${NS}" may not resolve "env:PROD_DB_PASSWORD" (task "fetch", secret "token"); allowed: env:SANDBOX_*, env:E2E_*`;
	await page.goto(`${UI_ORIGIN}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill(EXFIL);
	await page.getByTestId('apply-validate').click();
	const issues = page.getByTestId('apply-validation-issues');
	await expect(issues).toContainText(`tasks[0].secrets[0].source: ${sentence}`);
	note(`validate issue: ${await issues.innerText()}`);
	await page.screenshot({ path: out('screenshot-validate-field-issue.png'), fullPage: true });

	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-error')).toContainText(sentence);
	note(`apply refusal: ${await page.getByTestId('apply-error').innerText()}`);
	const listed = await rpc<{ orkflows?: unknown[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect(listed.orkflows ?? []).toHaveLength(0);
});

test('4: a permitted secret runs, its sha256 is in the log pane and the token is nowhere on the page', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill(FETCH);
	await page.getByTestId('apply-submit').click();
	await page.waitForURL(/\/orkflows\/fetch/);

	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });

	await page.getByTestId('task-log-toggle-fetch').click();
	await expect
		.poll(
			async () =>
				page
					.getByTestId('task-log-fetch')
					.getByTestId('task-log-line')
					.evaluateAll((nodes) => nodes.map((n) => (n.textContent ?? '').trim())),
			{ timeout: 120_000 }
		)
		.toContain(`${TOKEN_SHA256}  /orkstack/input/secrets/token`);
	expect((await page.content()).includes(TOKEN)).toBe(false);
	note(`run ${page.url()} SUCCEEDED; log pane line: ${TOKEN_SHA256}  /orkstack/input/secrets/token`);
	note('page.content() searched for the minted token: 0 matches');
	await page.screenshot({ path: out('screenshot-permitted-run.png'), fullPage: true });
});

test('5: with the list narrowed, the Submit a run dialog shows the server’s sentence verbatim', async ({ page }) => {
	await openAdmin(page);
	await openNamespaceEdit(page, NS);
	await page.getByTestId(`ns-edit-allowed-secrets-${NS}`).fill('env:SANDBOX_*');
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-allowed-secrets-${NS}`)).toHaveText('env:SANDBOX_*');

	await page.goto(`${UI_ORIGIN}/orkflows/fetch?ns=${NS}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
	await page.getByTestId('run-submit-submit').click();
	const sentence = `namespace "${NS}" may not resolve "env:E2E_TOKEN" (task "fetch", secret "token"); allowed: env:SANDBOX_*`;
	await expect(page.getByTestId('run-submit-error')).toContainText(sentence);
	note(`submit refusal shown: ${await page.getByTestId('run-submit-error').innerText()}`);
	await page.screenshot({ path: out('screenshot-submit-refusal.png'), fullPage: true });
});

test('6: a list changed from a second tab turns the first tab’s save into a stale-etag conflict', async ({
	page,
	browser
}) => {
	await openAdmin(page);
	await openNamespaceEdit(page, NS);
	await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toBeVisible();
	await page.getByTestId(`ns-edit-allowed-secrets-${NS}`).fill('env:FIRST_TAB_*');

	const second = await browser.newContext();
	try {
		const other = await second.newPage();
		await openAdmin(other);
		await openNamespaceEdit(other, NS);
		await other.getByTestId(`ns-edit-allowed-secrets-${NS}`).fill('env:SECOND_TAB_*');
		await other.getByTestId(`ns-edit-save-${NS}`).click();
		await expect(other.getByTestId(`namespace-allowed-secrets-${NS}`)).toHaveText('env:SECOND_TAB_*');
	} finally {
		await second.close();
	}

	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-edit-conflict-${NS}`)).toBeVisible();
	await expect(page.getByTestId(`namespace-edit-conflict-${NS}`)).toContainText('changed since it was read');
	await page.screenshot({ path: out('screenshot-two-tabs-conflict.png'), fullPage: true });

	const stored = await rpc<{ namespace: { allowedSecrets?: string[] } }>('NamespaceService/GetNamespace', {
		name: NS
	});
	expect(stored.namespace.allowedSecrets).toEqual(['env:SECOND_TAB_*']);
	note(`after the conflicting save, the stored list is still ${JSON.stringify(stored.namespace.allowedSecrets)}`);
});
