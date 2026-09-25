import { expect, test, type Page } from '@playwright/test';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	clusterEnv,
	noteTo,
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
import { selectOption } from './uiHelpers';

// Admin → Audit, against a real engine on real Temporal and MinIO.
//
// What is measured here, each in a browser:
//
//   1. a write the UI just made is on the Audit page at once, `unattributed`
//      because nothing asserted a principal, under the caption that says the
//      principal is an assertion OrkStack does not authenticate;
//   2. an Ork-Principal header set IN FRONT of the browser (as a gateway
//      would) reaches the engine through the SvelteKit proxy unchanged and is
//      recorded verbatim;
//   3. the scope is every namespace by default and a namespace narrows it —
//      the deployment-wide retention write disappears, and the scope line
//      says which scope is in force;
//   4. a refused write is a record too, with its error code;
//   5. pagination walks newest first without repeats, and a reload keeps the
//      page and Previous;
//   6. a failed read is an ErrorPanel with Retry, never an empty table.
//
// Topology: ONE engine, no Orker (nothing here runs a task), on a Temporal
// namespace and a bucket this run creates and alone writes to — the retention
// maximum is one deployment-wide document, and the trail of a fresh bucket
// holds exactly this run's writes, so every count below is exact.
//
// Process hygiene: this spec kills ONLY the processes it started, by handle.
// It never calls the harness's killAll, whose pattern kill would reach other
// engines on this host.

const ENGINE_PORT = '18096';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14190';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;

const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-audit-${STAMP}`;
const BUCKET = `orkstack-audit-${STAMP}`;
const NS = `audit-e2e-${STAMP}`;
const EXTRA = [`audit-extra-a-${STAMP}`, `audit-extra-b-${STAMP}`];

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-audit')
	: path.join(uiDir, 'test-results', 'admin-audit');
const TRANSCRIPT = 'admin-audit-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];
let engineProc: ChildProcess | undefined;
let engineStarts = 0;

function bucketEnv(action: 'ensure' | 'cleanup'): NodeJS.ProcessEnv {
	return {
		...process.env,
		ORK_BUCKET_ACTION: action,
		ORK_S3_BUCKET: BUCKET,
		ORK_S3_ENDPOINT: s3Env.ORK_S3_ENDPOINT,
		ORK_S3_REGION: s3Env.ORK_S3_REGION,
		ORK_S3_ACCESS_KEY: s3Env.ORK_S3_ACCESS_KEY,
		ORK_S3_SECRET_KEY: s3Env.ORK_S3_SECRET_KEY
	};
}

function startEngine(): ChildProcess {
	engineStarts++;
	const child = spawnLogged(outPath(`engine-${engineStarts}.log`), './bin/ork', [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	mine.push(child);
	return child;
}

async function stop(child: ChildProcess | undefined): Promise<void> {
	if (!child || child.exitCode !== null || child.signalCode) return;
	child.kill('SIGTERM');
	const deadline = Date.now() + 10_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, 100));
	}
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

async function rowTexts(page: Page): Promise<string[]> {
	return page.getByTestId('audit-row').evaluateAll((rows) =>
		rows.map((row) => (row.textContent ?? '').replace(/\s+/g, ' ').trim())
	);
}

async function rowIds(page: Page): Promise<string[]> {
	return page
		.getByTestId('audit-row')
		.evaluateAll((rows) => rows.map((row) => row.getAttribute('data-record-id') ?? ''));
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# Admin → Audit: the control-plane audit trail in a browser.',
			'#',
			'# Written by ui/e2e/admin-audit.spec.ts against a real engine on real Temporal + MinIO.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# bucket:             ${BUCKET} (created fresh by this run alone)`,
			`# ork namespace:      ${NS}`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing: ui/build/index.js');
	execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('ensure') });

	engineProc = startEngine();
	await waitForEngine(ENGINE);
	mine.push(
		spawnLogged(
			outPath('ui.log'),
			'node',
			['build'],
			{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
			uiDir
		)
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	note(`engine up at ${ENGINE}; ui at ${UI_ORIGIN}`);
});

test.afterAll(async () => {
	for (const child of mine.splice(0)) await stop(child);
	try {
		execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('cleanup') });
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

test('Admin → Audit lists a write the UI just made, unattributed, with the caption', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await page.getByTestId('ns-create-name').fill(NS);
	await page.getByTestId('ns-create-run-retention').fill('48');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toBeVisible();

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await page.getByTestId('admin-tab-audit').click();
	await expect(page).toHaveURL(/\/admin\/audit/);
	const first = page.getByTestId('audit-row').first();
	await expect(first.getByTestId('audit-action')).toHaveText('namespace.create');
	await expect(first.getByTestId('audit-target')).toContainText(NS);
	await expect(first.getByTestId('audit-principal')).toHaveText('unattributed');
	await expect(first.getByTestId('audit-result')).toHaveText('ok');
	await expect(page.getByTestId('audit-caption')).toBeVisible();
	await expect(page.getByTestId('audit-caption')).toContainText('does not authenticate it');
	await expect(page.getByTestId('audit-scope')).toContainText('Showing every namespace');
	await shot(page, 'admin-audit-unattributed.png');
	note(`first row: ${(await rowTexts(page))[0]}`);
});

test('an Ork-Principal header set in front of the browser is recorded', async ({ browser }) => {
	// A gateway in front of the UI adds the header to every browser request;
	// the SvelteKit proxy must forward it to the engine untouched.
	const context = await browser.newContext({ extraHTTPHeaders: { 'Ork-Principal': 'e2e-operator' } });
	const page = await context.newPage();
	try {
		await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
		await page.getByTestId(`namespace-edit-${NS}`).click();
		await page.getByTestId(`ns-edit-description-${NS}`).fill('edited behind a gateway');
		await page.getByTestId(`ns-edit-save-${NS}`).click();
		await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toBeHidden();

		await page.goto(`${UI_ORIGIN}/admin/audit?ns=default`);
		const first = page.getByTestId('audit-row').first();
		await expect(first.getByTestId('audit-action')).toHaveText('namespace.update');
		await expect(first.getByTestId('audit-principal')).toHaveText('e2e-operator');
		await expect(first.getByTestId('audit-detail')).toContainText('edited behind a gateway');
		await shot(page, 'admin-audit-e2e-operator.png');
		note(`attributed row: ${(await rowTexts(page))[0]}`);
	} finally {
		await context.close();
	}
});

test('the namespace scope filter narrows to one namespace and the scope line says so', async ({ page }) => {
	// A deployment-wide write, made through the UI.
	await page.goto(`${UI_ORIGIN}/admin/retention?ns=${NS}`);
	await page.getByTestId('retention-set-hours').fill('96');
	await page.getByTestId('retention-set-submit').click();
	await expect(page.getByTestId('retention-set-success')).toBeVisible();

	await page.goto(`${UI_ORIGIN}/admin/audit?ns=${NS}`);
	await expect(page.getByTestId('audit-row').first().getByTestId('audit-action')).toHaveText('retention.update');
	const everywhere = await rowTexts(page);
	expect(everywhere.some((row) => row.includes('retention.update'))).toBe(true);

	await selectOption(page, 'audit-scope-select', 'namespace');
	await expect(page).toHaveURL(/scope=namespace/);
	await expect(page.getByTestId('audit-scope')).toContainText(`Showing namespace ${NS}`);
	await expect(page.getByTestId('audit-row').first().getByTestId('audit-action')).toHaveText('namespace.update');
	const narrowed = await rowTexts(page);
	expect(narrowed.some((row) => row.includes('retention.update'))).toBe(false);
	expect(narrowed.every((row) => row.includes(NS))).toBe(true);
	await page.getByTestId('audit-cli-trigger').click();
	await expect(page.getByTestId('audit-cli-command')).toHaveText(`orkctl -n '${NS}' admin audit`);
	await shot(page, 'admin-audit-scoped.png');
	note(`every namespace: ${everywhere.length} rows; namespace ${NS}: ${narrowed.length} rows, no retention.update`);
});

test('a refused write appears with its error code', async ({ page }) => {
	// `default`'s Delete is disabled in the UI, so the refusal is produced
	// through the other write the Admin pages can have refused: lowering the
	// deployment maximum below this namespace's own 48h run retention.
	await page.goto(`${UI_ORIGIN}/admin/retention?ns=default`);
	await page.getByTestId('retention-set-hours').fill('24');
	await page.getByTestId('retention-set-submit').click();
	await expect(page.getByTestId('retention-set-error')).toBeVisible();

	await page.goto(`${UI_ORIGIN}/admin/audit?ns=default`);
	const first = page.getByTestId('audit-row').first();
	await expect(first.getByTestId('audit-action')).toHaveText('retention.update');
	await expect(first.getByTestId('audit-result')).toHaveText('refused failed_precondition');
	await expect(first.getByTestId('audit-namespace')).toHaveText('-');
	await shot(page, 'admin-audit-refused.png');
	note(`refused row: ${(await rowTexts(page))[0]}`);
});

test('pagination walks newest first without repeats and reload restores the page', async ({ page }) => {
	for (const name of EXTRA) await rpc('NamespaceService/CreateNamespace', { namespace: { name } });
	// create, update, retention ok, retention refused, and the two above.
	await page.goto(`${UI_ORIGIN}/admin/audit?ns=default&limit=2`);
	const seen: string[] = [];
	const pages: string[][] = [];
	for (let n = 1; n <= 3; n++) {
		await expect(page.getByTestId('audit-row')).toHaveCount(2);
		const ids = await rowIds(page);
		pages.push(ids);
		seen.push(...ids);
		if (n < 3) {
			await page.getByTestId('audit-next').click();
			await expect(page).toHaveURL(new RegExp(`page=${n + 1}`));
		}
	}
	expect(new Set(seen).size).toBe(seen.length);
	// Newest first: the ids embed the stamp, so the walk is strictly descending.
	expect([...seen].sort().reverse()).toEqual(seen);
	await expect(page.getByTestId('audit-page-status')).toContainText('Page 3');

	await page.reload();
	await expect(page.getByTestId('audit-page-status')).toContainText('Page 3');
	expect(await rowIds(page)).toEqual(pages[2]);
	await expect(page.getByTestId('audit-previous')).not.toHaveAttribute('aria-disabled', 'true');
	await shot(page, 'admin-audit-page-3.png');

	await page.getByTestId('audit-previous').click();
	await expect(page.getByTestId('audit-page-status')).toContainText('Page 2');
	expect(await rowIds(page)).toEqual(pages[1]);
	note(`pages: ${JSON.stringify(pages)}`);
});

test('a failed audit read reports and retries', async ({ page }) => {
	await stop(engineProc);
	await waitFor('engine port closed', async () => !(await tcpOpen(Number(ENGINE_PORT))));
	await page.goto(`${UI_ORIGIN}/admin/audit?ns=default`);
	await expect(page.getByTestId('audit-error')).toBeVisible();
	await expect(page.getByTestId('audit-table')).toHaveCount(0);
	await expect(page.getByTestId('audit-empty')).toHaveCount(0);
	await expect(page.getByTestId('audit-caption')).toBeVisible();
	await shot(page, 'admin-audit-read-failed.png');
	note(`engine stopped: ${await page.getByTestId('audit-error').innerText()}`);

	engineProc = startEngine();
	await waitForEngine(ENGINE);
	await page.getByTestId('audit-error-retry').click();
	await expect(page.getByTestId('audit-table')).toBeVisible();
	await expect(page.getByTestId('audit-error')).toHaveCount(0);
	await shot(page, 'admin-audit-read-recovered.png');
	note(`engine restarted; retry rendered ${(await rowIds(page)).length} rows`);
});
