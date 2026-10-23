import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	clusterEnv,
	ensureBucket,
	killStarted,
	noteTo,
	openNamespaceEdit,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';
import { OP, addListFilter, selectOption } from './uiHelpers';

// Admin → Namespaces, paged and filtered through the one structured list
// filter (ListFilter; docs/UI_CLI_PARITY.md "Filtering lists"), against a
// real engine on real Temporal and MinIO. Measured in a browser:
//
//   N1 pages walk the registry in NAME order (`a`, `a-b`, `ab` — the bucket
//      lists `a-b.json` before `a.json`), Next/Previous, no repeat or gap;
//   N2 a pasted later page disables Previous and offers First page;
//   N3 `name ~ tenant-1` narrows, written to the URL as one `filter=`
//      parameter; `name = default` shows default;
//   N4 `labels.team = payments` at size 10 pages a PROPERTY filter: page 1,
//      Next, Previous, and `a-b` on neither; `hold.reason !=` shows only the
//      held namespace;
//   N5 a filter matching nothing says so, and Clear filters recovers;
//   N6 a create from page 1 lands on `filter=name=<created>` with its row;
//   N7 an edit on page 2 saves and stays on page 2;
//   N8 the CLI equivalent is the same filter as orkctl spells it;
//   N9 page size is a field of the filter form: it applies with Apply
//      filters, never on change.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, its own bucket
// created by this run (so the registry holds exactly what this spec wrote and
// every count below is exact), its own ports. It ends only the processes it
// started (killStarted), never anything by name.
const ENGINE_PORT = '18473';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14473';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-ns-pages-${STAMP}`;
const BUCKET = ownBucket('ns-pages');

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-namespaces-pages')
	: path.join(uiDir, 'test-results', 'admin-namespaces-pages');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// 23 namespaces plus `default`: pages of 10, 10 and 4 at the smallest size the
// page offers. `a`, `a-b` and `ab` are the key-order trap. Twelve carry
// team=payments — `a`, `ab` and the ten even tenants — so `a-b`, which sorts
// between `a` and `ab`, is the one a property filter must skip; tenant-05 is
// held.
const TENANTS = Array.from({ length: 20 }, (_, i) => `tenant-${String(i).padStart(2, '0')}`);
const SEEDED = ['ab', 'a-b', 'a', ...TENANTS];
const ALL = [...SEEDED, 'default'].sort();
const PAGES = [ALL.slice(0, 10), ALL.slice(10, 20), ALL.slice(20)];
const PAYMENTS = ['a', 'ab', ...TENANTS.filter((_, i) => i % 2 === 0)];
const HELD = 'tenant-05';
const CREATED = 'zulu';


test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureBucket(BUCKET);

	spawnLogged(out('engine.log'), bin('ork'), [], {
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

	for (const name of SEEDED) {
		await rpc('NamespaceService/CreateNamespace', {
			namespace: {
				name,
				description: `seeded ${name}`,
				...(PAYMENTS.includes(name) ? { labels: { team: 'payments' } } : {}),
				...(name === HELD ? { hold: { reason: 'billing review' } } : {})
			}
		});
	}
	const listed = await rpc<{ namespaces?: { name: string }[] }>('NamespaceService/ListNamespaces', {});
	expect((listed.namespaces ?? []).map((ns) => ns.name)).toEqual(ALL);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`seeded ${SEEDED.length} namespaces; with default the registry is ${ALL.length}: ${ALL.join(' ')}`);
	note(`team=payments: ${PAYMENTS.join(' ')}; held: ${HELD}`);
});

test.afterAll(async () => {
	await killStarted();
	try {
		removeBucket(BUCKET, note);
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

/** The names of the rows on screen, in the order the table shows them. */
async function rowNames(page: Page): Promise<string[]> {
	return page
		.getByTestId('namespace-list')
		.locator('tr[data-testid^="namespace-row-"]')
		.evaluateAll((rows) => rows.map((row) => (row.getAttribute('data-testid') ?? '').replace('namespace-row-', '')));
}

async function expectRows(page: Page, names: string[]): Promise<void> {
	await expect.poll(() => rowNames(page)).toEqual(names);
}

function shot(page: Page, name: string) {
	return page.screenshot({ path: out(name), fullPage: true });
}

/** The `filter` parameters of the page's address, decoded. */
function urlFilters(page: Page): string[] {
	return new URL(page.url()).searchParams.getAll('filter');
}

async function apply(page: Page): Promise<void> {
	await page.getByTestId('namespaces-filters-apply').click();
}

test('N1–N2: pages walk the registry in name order, and a pasted later page offers First page', async ({ page, context }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 1 · 10 namespaces');
	await expect(page.getByTestId('namespaces-previous')).toHaveAttribute('aria-disabled', 'true');
	await expect(page.getByTestId('namespaces-page-size')).toHaveText('10');
	note(`page 1: ${(await rowNames(page)).join(' ')}`);
	await shot(page, 'screenshot-page-1.png');

	await page.getByTestId('namespaces-next').click();
	await expect(page).toHaveURL(/cursor=.*page=2/);
	await expectRows(page, PAGES[1]);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 2 · 10 namespaces');
	note(`page 2: ${(await rowNames(page)).join(' ')}`);

	await page.getByTestId('namespaces-next').click();
	await expect(page).toHaveURL(/page=3/);
	await expectRows(page, PAGES[2]);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 3 · 4 namespaces · last page');
	await expect(page.getByTestId('namespaces-next')).toHaveAttribute('aria-disabled', 'true');
	note(`page 3: ${(await rowNames(page)).join(' ')}`);
	await shot(page, 'screenshot-page-3-last.png');
	const lastPage = page.url();

	await page.getByTestId('namespaces-previous').click();
	await expectRows(page, PAGES[1]);
	await page.getByTestId('namespaces-previous').click();
	await expectRows(page, PAGES[0]);
	await expect(page).not.toHaveURL(/cursor=/);
	note('Previous, Previous: back on page 1 with no cursor in the URL');

	// A pasted link to page 3 in a new tab: its own sessionStorage, so no path
	// back. Previous is disabled and First page is offered instead.
	const pasted = await context.newPage();
	await pasted.goto(lastPage);
	await expectRows(pasted, PAGES[2]);
	await expect(pasted.getByTestId('namespaces-previous')).toHaveAttribute('aria-disabled', 'true');
	await expect(pasted.getByTestId('namespaces-first')).toBeVisible();
	await shot(pasted, 'screenshot-pasted-page-3.png');
	await pasted.getByTestId('namespaces-first').click();
	await expectRows(pasted, PAGES[0]);
	note('a pasted page-3 link offered First page and it landed on page 1');
	await pasted.close();
});

test('N3: a name filter narrows the list and rides the URL as one filter parameter', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await addListFilter(page, 'namespaces', { field: 'name', op: OP.contains, value: 'tenant-1' });
	await apply(page);
	await expect.poll(() => urlFilters(page)).toEqual(['name~tenant-1']);
	expect(page.url()).toContain('filter=name%7Etenant-1');
	await expectRows(page, TENANTS.filter((name) => name.includes('tenant-1')));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('10 namespaces');
	note(`name~tenant-1: ${(await rowNames(page)).join(' ')} — URL ${page.url()}`);

	// The draft was refilled from the URL; edit it in place.
	await selectOption(page, 'namespaces-filter-op-0', OP.equals);
	await page.getByTestId('namespaces-filter-value-0').fill('default');
	await apply(page);
	await expect.poll(() => urlFilters(page)).toEqual(['name=default']);
	await expectRows(page, ['default']);
	await expect(page.getByTestId('namespaces-empty')).toHaveCount(0);
});

test('N4: a property filter pages with its own cursor, and a hold filter finds the held namespace', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await addListFilter(page, 'namespaces', { field: 'labels', key: 'team', op: OP.equals, value: 'payments' });
	await apply(page);
	await expect.poll(() => urlFilters(page)).toEqual(['labels.team=payments']);
	await expectRows(page, PAYMENTS.slice(0, 10));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 1 · 10 namespaces');
	expect(await rowNames(page)).not.toContain('a-b');
	note(`labels.team=payments page 1: ${(await rowNames(page)).join(' ')}`);
	await shot(page, 'screenshot-filter-labels.png');

	await page.getByTestId('namespaces-next').click();
	await expect(page).toHaveURL(/cursor=.*page=2/);
	await expectRows(page, PAYMENTS.slice(10));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 2 · 2 namespaces · last page');
	await expect.poll(() => urlFilters(page)).toEqual(['labels.team=payments']);
	note(`labels.team=payments page 2: ${(await rowNames(page)).join(' ')}`);
	await shot(page, 'screenshot-filter-labels-page-2.png');

	await page.getByTestId('namespaces-previous').click();
	await expectRows(page, PAYMENTS.slice(0, 10));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 1 · 10 namespaces');
	note('Previous returned to page 1 of the property filter with the same ten rows');

	// Replace the filter: hold.reason != (empty) is every held namespace.
	await page.getByTestId('namespaces-filter-remove-0').click();
	await addListFilter(page, 'namespaces', { field: 'hold.reason', op: OP.notEquals, value: '' });
	await apply(page);
	await expect.poll(() => urlFilters(page)).toEqual(['hold.reason!=']);
	await expectRows(page, [HELD]);
	note(`hold.reason!= : ${(await rowNames(page)).join(' ')}`);
});

test('N5: a filter that matches nothing says so, and Clear filters goes back to page 1', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await addListFilter(page, 'namespaces', { field: 'description', op: OP.contains, value: 'nothing-is-described-this-way' });
	await apply(page);
	await expect(page.getByTestId('namespaces-empty')).toHaveText('No namespaces match these filters.');
	await expect(page.getByTestId('namespace-list')).toHaveCount(0);
	await shot(page, 'screenshot-no-match.png');
	note(`no match: ${await page.getByTestId('namespaces-empty').innerText()}`);

	await page.getByTestId('namespaces-filters-clear').click();
	await expect.poll(() => urlFilters(page)).toEqual([]);
	await expect(page.getByTestId('namespaces-filter-row-0')).toHaveCount(0);
	await expectRows(page, PAGES[0]);
	await expect(page).toHaveURL(/size=10/);
});

test('N6: a create from page 1 lands on the created row, though it sorts onto the last page', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await page.getByTestId('ns-create-name').fill(CREATED);
	await page.getByTestId('ns-create-description').fill('created from page 1');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${CREATED}.`);
	await expect.poll(() => urlFilters(page)).toEqual([`name=${CREATED}`]);
	await expect(page).not.toHaveURL(/cursor=/);
	await expect(page.getByTestId('namespaces-filter-value-0')).toHaveValue(CREATED);
	await expectRows(page, [CREATED]);
	await expect(page.getByTestId(`namespace-row-${CREATED}`)).toContainText('created from page 1');
	await shot(page, 'screenshot-created-row.png');
	note(`create from page 1: URL ${page.url()} shows ${(await rowNames(page)).join(' ')}`);
});

test('N7: an edit on page 2 saves and leaves the reader on page 2', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await page.getByTestId('namespaces-next').click();
	await expectRows(page, PAGES[1]);
	const pageTwo = page.url();
	const target = PAGES[1][3];

	await openNamespaceEdit(page, target);
	await page.getByTestId(`ns-edit-description-${target}`).fill('edited on page 2');
	await page.getByTestId(`ns-edit-save-${target}`).click();
	await expect(page.getByTestId(`namespace-edit-form-${target}`)).toHaveCount(0);
	await expect(page.getByTestId(`namespace-row-${target}`)).toContainText('edited on page 2');
	expect(page.url()).toBe(pageTwo);
	await expectRows(page, PAGES[1]);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 2 · 10 namespaces');
	await shot(page, 'screenshot-edit-on-page-2.png');
	note(`edited ${target} on page 2; still at ${page.url()}`);
});

test('N8: the CLI equivalent is the applied filter as orkctl spells it', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10&filter=${encodeURIComponent('labels.team=payments')}`);
	await expectRows(page, PAYMENTS.slice(0, 10));
	await page.getByTestId('namespaces-filters-cli-trigger').click();
	await expect(page.getByTestId('namespaces-filters-cli-command')).toHaveText("orkctl namespace list --filter 'labels.team=payments'");
	note(`CLI equivalent: ${await page.getByTestId('namespaces-filters-cli-command').innerText()}`);
});

test('N9: page size is a field of the filter form and applies with Apply filters', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await addListFilter(page, 'namespaces', { field: 'name', op: OP.prefix, value: 'tenant-' });
	await apply(page);
	await expect.poll(() => urlFilters(page)).toEqual(['name^tenant-']);
	await expectRows(page, TENANTS.slice(0, 10));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('Page 1 · 10 namespaces');
	const before = page.url();

	await selectOption(page, 'namespaces-page-size', '25');
	await expect(page.getByTestId('namespaces-page-size')).toHaveText('25');
	expect(page.url()).toBe(before);
	await expectRows(page, TENANTS.slice(0, 10));

	await apply(page);
	await expectRows(page, TENANTS);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('20 namespaces');
	const params = new URL(page.url()).searchParams;
	expect(params.getAll('size')).toEqual(['25']);
	expect(params.get('cursor')).toBeNull();
	expect(params.getAll('filter')).toEqual(['name^tenant-']);
	note(`size 25 applied with the filters: ${page.url()}`);
});
