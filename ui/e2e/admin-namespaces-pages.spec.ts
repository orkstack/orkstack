import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	clusterEnv,
	killStarted,
	noteTo,
	openNamespaceEdit,
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

// Admin → Namespaces, paged and filtered, against a real engine on real
// Temporal and MinIO.
//
// The page used to read every namespace document and render them all in one
// table; with a registry of thousands that took seconds and could not redraw
// in time after a create. What is measured here, each in a browser:
//
//   1. a registry larger than one page opens on page 1 in NAME order (`a`,
//      `a-b`, `ab` — the bucket lists `a-b.json` before `a.json`), and Next,
//      Previous and the last page walk it without a repeat or a gap;
//   2. a pasted later page has no path back, so Previous is disabled and
//      First page is offered, and it lands on page 1;
//   3. the name filter narrows the list (`q` in the URL), a filter that
//      matches nothing says so, and Clear filter goes back to everything;
//   4. a create from page 1 lands on the created namespace's row, though it
//      sorts onto the last page;
//   5. an edit on page 2 saves and leaves the reader on page 2.
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
const BUCKET = `orkstack-ns-pages-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-namespaces-pages')
	: path.join(uiDir, 'test-results', 'admin-namespaces-pages');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// 23 namespaces plus `default`: pages of 10, 10 and 4 at the smallest size the
// page offers. `a`, `a-b` and `ab` are the key-order trap.
const TENANTS = Array.from({ length: 20 }, (_, i) => `tenant-${String(i).padStart(2, '0')}`);
const SEEDED = ['ab', 'a-b', 'a', ...TENANTS];
const ALL = [...SEEDED, 'default'].sort();
const PAGES = [ALL.slice(0, 10), ALL.slice(10, 20), ALL.slice(20)];
const CREATED = 'zulu';

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

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing: ui/build/index.js');
	execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('ensure') });

	spawnLogged(out('engine.log'), './bin/ork', [], {
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
		await rpc('NamespaceService/CreateNamespace', { namespace: { name, description: `seeded ${name}` } });
	}
	const listed = await rpc<{ namespaces?: { name: string }[] }>('NamespaceService/ListNamespaces', {});
	expect((listed.namespaces ?? []).map((ns) => ns.name)).toEqual(ALL);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`seeded ${SEEDED.length} namespaces; with default the registry is ${ALL.length}: ${ALL.join(' ')}`);
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

test('1–2: pages walk the registry in name order, and a pasted later page offers First page', async ({ page, context }) => {
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

	// Page size applies on change and starts over at page 1.
	await selectOption(page, 'namespaces-page-size', '25');
	await expect(page).toHaveURL(/size=25/);
	await expectRows(page, ALL);
	await expect(page.getByTestId('namespaces-page-status')).toHaveText(`${ALL.length} namespaces`);
});

test('3: the name filter narrows the list, says when nothing matches, and clears', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	const filter = page.getByTestId('namespaces-filter');

	await filter.fill('tenant-1');
	await filter.press('Enter');
	await expect(page).toHaveURL(/q=tenant-1/);
	await expectRows(page, TENANTS.filter((name) => name.includes('tenant-1')));
	await expect(page.getByTestId('namespaces-page-status')).toHaveText('10 namespaces');
	note(`q=tenant-1: ${(await rowNames(page)).join(' ')}`);

	// `-` is an ordinary character of the substring; `b` matches both trap names.
	await filter.fill('b');
	await page.getByTestId('namespaces-filter-apply').click();
	await expect(page).toHaveURL(/q=b(&|$)/);
	await expectRows(page, ['a-b', 'ab']);
	await shot(page, 'screenshot-filter-b.png');

	// `default` is filtered like any other name.
	await filter.fill('fault');
	await filter.press('Enter');
	await expectRows(page, ['default']);
	await expect(page.getByTestId('namespaces-empty')).toHaveCount(0);

	await filter.fill('nothing-is-called-this');
	await filter.press('Enter');
	await expect(page.getByTestId('namespaces-empty')).toContainText('No namespace name contains nothing-is-called-this');
	await expect(page.getByTestId('namespace-list')).toHaveCount(0);
	await shot(page, 'screenshot-filter-no-match.png');
	note(`no match: ${await page.getByTestId('namespaces-empty').innerText()}`);

	await page.getByTestId('namespaces-filter-clear').click();
	await expect(page).not.toHaveURL(/q=/);
	await expect(filter).toHaveValue('');
	await expectRows(page, PAGES[0]);
});

test('4: a create from page 1 lands on the created row, though it sorts onto the last page', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default&size=10`);
	await expectRows(page, PAGES[0]);
	await page.getByTestId('ns-create-name').fill(CREATED);
	await page.getByTestId('ns-create-description').fill('created from page 1');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${CREATED}.`);
	await expect(page).toHaveURL(new RegExp(`q=${CREATED}`));
	await expect(page).not.toHaveURL(/cursor=/);
	await expect(page.getByTestId('namespaces-filter')).toHaveValue(CREATED);
	await expectRows(page, [CREATED]);
	await expect(page.getByTestId(`namespace-row-${CREATED}`)).toContainText('created from page 1');
	await shot(page, 'screenshot-created-row.png');
	note(`create from page 1: URL ${page.url()} shows ${(await rowNames(page)).join(' ')}`);
});

test('5: an edit on page 2 saves and leaves the reader on page 2', async ({ page }) => {
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
