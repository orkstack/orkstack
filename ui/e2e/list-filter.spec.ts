import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killStarted,
	noteTo,
	proofOut,
	repoRoot,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	waitFor,
	waitForEngine,
	bin,
	temporalAddress,
	temporalReachable
} from './harness';
import { OP, addListFilter } from './uiHelpers';

// The one list filter component (ListFilter; docs/UI_CLI_PARITY.md
// "Filtering lists") on real lists, against a real engine on real Temporal
// and MinIO:
//
//   L1 Orkflows: two filters applied, written as two `filter=` parameters in
//      canonical order, rows narrowed; a reload restores the rows AND the
//      draft;
//   L2 a `created_at >=` row through datetime-local: the CLI equivalent
//      carries the UTC instant (the browser runs in America/Denver, so the
//      conversion is visible);
//   L3 Admin → Pools: `is_default = true` leaves the default pool;
//   L4 Admin → Webhooks: `namespace =` (empty) leaves only the
//      deployment-wide subscriptions;
//   L5 a pasted URL with a field the list does not declare shows the
//      server's INVALID_ARGUMENT text, and Clear filters recovers;
//   L6 changing a filter on page 2 of Orkflows returns to page 1, no cursor.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, its own bucket,
// its own ports. It ends only the processes it started (killStarted).
const ENGINE_PORT = '18493';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14493';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-list-filter-${STAMP}`;
const BUCKET = `orkstack-list-filter-${STAMP}`;
const NS = 'filters';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'list-filter')
	: path.join(uiDir, 'test-results', 'list-filter');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// Twelve orkflows: six of team payments, six of team search — more than one
// page of 10, so L6 has a page 2.
const PAY = Array.from({ length: 6 }, (_, i) => `pay-${String(i + 1).padStart(2, '0')}`);
const SEARCH = Array.from({ length: 6 }, (_, i) => `search-${String(i + 1).padStart(2, '0')}`);

function orkflowYaml(name: string, team: string): string {
	return `name: ${name}
version: "1.0.0"
description: list filter fixture ${name}
labels: {team: ${team}}
tasks:
  - id: emit
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "printf '{}' > /orkstack/output/values.json"]
`;
}

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

test.describe.configure({ mode: 'serial' });
// A zone west of UTC, so a datetime-local reading and the instant it sends
// differ and the conversion is observable.
test.use({ timezoneId: 'America/Denver' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();
	execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('ensure') });

	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: NS, description: 'list filter fixture', defaultPool: 'main', allowedPools: ['main', 'gpu'] }
	});
	for (const name of PAY) await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: orkflowYaml(name, 'payments') });
	for (const name of SEARCH) await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: orkflowYaml(name, 'search') });
	await rpc('WebhookService/RegisterWebhook', { url: 'https://example.test/everything' });
	await rpc('WebhookService/RegisterWebhook', { url: 'https://example.test/payments', namespace: NS, name: 'pay-hook' });
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`namespace ${NS}: orkflows ${[...PAY, ...SEARCH].join(' ')}; two webhooks (one deployment-wide)`);
});

test.afterAll(async () => {
	await killStarted();
	try {
		execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('cleanup') });
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

async function orkflowRows(page: Page): Promise<string[]> {
	return page
		.getByTestId('orkflow-list')
		.locator('tr[data-testid^="orkflow-row-"]')
		.evaluateAll((rows) => rows.map((row) => (row.getAttribute('data-testid') ?? '').replace('orkflow-row-', '')));
}

function urlFilters(page: Page): string[] {
	return new URL(page.url()).searchParams.getAll('filter');
}

async function cliCommand(page: Page, testid: string): Promise<string> {
	await page.getByTestId(`${testid}-filters-cli-trigger`).click();
	const text = await page.getByTestId(`${testid}-filters-cli-command`).innerText();
	await page.keyboard.press('Escape');
	return text;
}

test('L1: two filters, canonical in the URL, survive a reload with their draft', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}&size=25`);
	await expect.poll(() => orkflowRows(page)).toEqual([...PAY, ...SEARCH]);
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'orkflow', { field: 'name', op: OP.contains, value: '-0' });
	await addListFilter(page, 'orkflow', { field: 'labels', key: 'team', op: OP.equals, value: 'payments' });
	await page.getByTestId('orkflow-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['labels.team=payments', 'name~-0']);
	await expect.poll(() => orkflowRows(page)).toEqual(PAY);
	note(`L1 applied: ${page.url()} -> ${(await orkflowRows(page)).join(' ')}`);
	await page.screenshot({ path: out('screenshot-orkflows-filters.png'), fullPage: true });

	await page.reload();
	await expect.poll(() => orkflowRows(page)).toEqual(PAY);
	await expect(page.getByTestId('orkflow-filter-field-0')).toHaveText('Label (labels.KEY)');
	await expect(page.getByTestId('orkflow-filter-key-0')).toHaveValue('team');
	await expect(page.getByTestId('orkflow-filter-value-0')).toHaveValue('payments');
	await expect(page.getByTestId('orkflow-filter-field-1')).toHaveText('Name (name)');
	await expect(page.getByTestId('orkflow-filter-op-1')).toHaveText('contains (~)');
	await expect(page.getByTestId('orkflow-filter-value-1')).toHaveValue('-0');
	note('L1 reload: rows and the draft restored from the URL');
});

test('L2: a created_at row through datetime-local sends the UTC instant', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}&size=25`);
	await page.waitForLoadState('networkidle');
	const row = await addListFilter(page, 'orkflow', { field: 'created_at', op: OP.atOrAfter, value: '2000-01-01T00:00' });
	// Midnight in Denver (UTC-7 in January) is 07:00 UTC.
	await expect(page.getByTestId(`orkflow-filter-utc-${row}`)).toHaveText('2000-01-01T07:00:00Z (UTC)');
	await page.getByTestId('orkflow-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['created_at>=2000-01-01T07:00:00Z']);
	await expect.poll(() => orkflowRows(page)).toEqual([...PAY, ...SEARCH]);
	const cli = await cliCommand(page, 'orkflow');
	expect(cli).toBe(`orkctl -n '${NS}' orkflow list --filter 'created_at>=2000-01-01T07:00:00Z'`);
	note(`L2 CLI equivalent: ${cli}`);
	await page.screenshot({ path: out('screenshot-orkflows-created-at.png'), fullPage: true });
});

test('L3: Admin → Pools, is_default = true leaves the default pool', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${NS}`);
	await expect(page.getByTestId('pool-card-gpu')).toBeVisible();
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'pools', { field: 'is_default', op: OP.equals, select: 'true' });
	// F-ui-ux-9: the comboboxes whose caption is not a <label> carry a name
	// of their own — the sidebar's namespace picker, and a filter row's value.
	await expect(page.getByRole('combobox', { name: 'Namespace', exact: true })).toBeVisible();
	await expect(page.getByRole('combobox', { name: / value$/ })).toHaveAttribute('data-testid', 'pools-filter-value-0');
	await page.getByTestId('pools-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['is_default=true']);
	await expect(page.getByTestId('pool-card-main')).toBeVisible();
	await expect(page.getByTestId('pool-card-gpu')).toHaveCount(0);
	const cli = await cliCommand(page, 'pools');
	expect(cli).toBe(`orkctl -n '${NS}' pool list --filter 'is_default=true'`);
	note(`L3 is_default=true: only main; CLI equivalent: ${cli}`);
	await page.screenshot({ path: out('screenshot-pools-default.png'), fullPage: true });
});

test('L4: Admin → Webhooks, namespace = (empty) leaves the deployment-wide subscriptions', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/webhooks?ns=default`);
	await expect(page.getByTestId('webhook-list')).toContainText('https://example.test/payments');
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'webhooks', { field: 'namespace', op: OP.equals, value: '' });
	await page.getByTestId('webhooks-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['namespace=']);
	await expect(page.getByTestId('webhook-list')).toContainText('https://example.test/everything');
	await expect(page.getByTestId('webhook-list')).not.toContainText('https://example.test/payments');
	note(`L4 namespace= : ${await page.getByTestId('webhook-list').innerText()}`);
	await page.screenshot({ path: out('screenshot-webhooks-deployment-wide.png'), fullPage: true });
});

test('L5: a pasted filter the list does not declare shows the server refusal, and Clear filters recovers', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}&size=25&filter=nope%3Dx`);
	await expect(page.getByTestId('engine-error')).toContainText('filters[0]: "nope" is not a filterable field of Orkflow; filterable: name, version, description, labels.KEY, definition_digest, created_at');
	note(`L5 refusal: ${await page.getByTestId('engine-error').innerText()}`);
	await page.screenshot({ path: out('screenshot-orkflows-refused-filter.png'), fullPage: true });
	await page.getByTestId('orkflow-filters-clear').click();
	await expect.poll(() => urlFilters(page)).toEqual([]);
	await expect(page.getByTestId('engine-error')).toHaveCount(0);
	await expect.poll(() => orkflowRows(page)).toEqual([...PAY, ...SEARCH]);
});

test('L6: changing a filter on page 2 returns to page 1 with no cursor', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${NS}&size=10`);
	await expect.poll(() => orkflowRows(page)).toEqual([...PAY, ...SEARCH].slice(0, 10));
	await page.getByTestId('orkflows-next').click();
	await expect(page).toHaveURL(/cursor=.*page=2/);
	await expect.poll(() => orkflowRows(page)).toEqual(SEARCH.slice(4));
	await page.waitForLoadState('networkidle');
	await addListFilter(page, 'orkflow', { field: 'labels', key: 'team', op: OP.equals, value: 'search' });
	await page.getByTestId('orkflow-filters-apply').click();
	await expect.poll(() => urlFilters(page)).toEqual(['labels.team=search']);
	const params = new URL(page.url()).searchParams;
	expect(params.get('cursor')).toBeNull();
	expect(params.get('page')).toBeNull();
	await expect.poll(() => orkflowRows(page)).toEqual(SEARCH);
	await expect(page.getByTestId('orkflows-page-status')).toHaveText('6 orkflows');
	note(`L6 a new filter on page 2 landed on ${page.url()}`);
});

// F-ui-ux-6: Copy on an origin with no Clipboard API (a plain-HTTP deployment
// address) still copies through the legacy path, and when that is refused too
// the page selects the command and says to copy it by hand — never a silent
// nothing. Both are forced in the page before any app code runs.
test('L7: Copy without a Clipboard API falls back, then says to copy by hand', async ({ page }) => {
	await page.addInitScript(() => {
		Object.defineProperty(Navigator.prototype, 'clipboard', { get: () => undefined, configurable: true });
		const calls: string[] = [];
		(window as unknown as { __execCalls: string[] }).__execCalls = calls;
		(window as unknown as { __execResult: boolean }).__execResult = true;
		document.execCommand = (command: string) => {
			calls.push(command);
			return (window as unknown as { __execResult: boolean }).__execResult;
		};
	});
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${NS}`);
	await expect(page.getByTestId('pool-card-gpu')).toBeVisible();
	await page.waitForLoadState('networkidle');

	await page.getByTestId('pools-filters-cli-trigger').click();
	await page.getByTestId('pools-filters-cli-copy').click();
	await expect(page.getByTestId('pools-filters-cli-copied')).toHaveText('Copied');
	expect(await page.evaluate(() => (window as unknown as { __execCalls: string[] }).__execCalls)).toEqual(['copy']);

	await page.evaluate(() => ((window as unknown as { __execResult: boolean }).__execResult = false));
	await page.getByTestId('pools-filters-cli-copy').click();
	await expect(page.getByTestId('pools-filters-cli-copied')).toContainText('copy it by hand');
	const command = (await page.getByTestId('pools-filters-cli-command').innerText()).trim();
	expect((await page.evaluate(() => window.getSelection()?.toString() ?? '')).trim()).toBe(command);
	note(`L7 no clipboard: fallback copied, then manual with "${command}" selected`);
	await page.screenshot({ path: out('screenshot-copy-manual.png'), fullPage: true });
});
