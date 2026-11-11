import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
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
	s3Env,
	spawnLogged,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { fillYaml } from './yaml';

// The authoring sheets (UI revamp, stage 3), against a real engine and Orker
// on real Temporal and MinIO:
//
//   dashboard  the count band is a summary line, not a hero: every figure
//              smaller than the page heading, in ink (ink-muted when it
//              could not be answered), the band one row at 1280, and the
//              first register inside the first viewport at 1280 × 720; a
//              namespace where nothing finished says `unavailable` rather
//              than a number;
//   orkflows   the register: exactly one link in every row (the NAME);
//   orkflow    the DAG on the grid beside the versions it has, current and
//              head marked, the header naming what is shown (its version and
//              definition plates); its runs a tab, not a link in the
//              header's actions; every tab;
//   palette    on an Orkflow page, Submit a run opens the page's own dialog
//              and sends nothing; a task id jumps to its row;
//   apply      validation issues carry .severity-error / .severity-warning;
//              the examples are a ruled list, each with its name and Use;
//   screens    both themes, desktop and 390 px; axe (WCAG 2.2 AA) on the
//              dashboard, /orkflows, every tab of an Orkflow page and /orkflows/+new.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18971';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-authoring-sheets-${STAMP}`;
const BUCKET = ownBucket('authoring-sheets');
const SAGA = `sheets-saga-${STAMP}`;
const QUIET = `sheets-quiet-${STAMP}`;
const EMPTY_NS = `sheets-empty-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'authoring-sheets')
	: path.join(uiDir, 'test-results', 'authoring-sheets');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);


const step = (id: string, needs: string[], extra = '') =>
	[
		`  - id: ${id}`,
		...(needs.length ? [`    needs: [${needs.join(', ')}]`] : []),
		...(extra ? [extra] : []),
		'    uses: shell',
		'    network: none',
		'    timeout_seconds: 120',
		'    with: {op: exec, command: ["echo", "ok"]}'
	].join('\n');

function saga(version: string): string {
	return [
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${SAGA}`,
		`version: "${version}"`,
		`description: The authoring sheets' saga, ${version}.`,
		'labels: {team: payments, tier: gold}',
		'tasks:',
		step('validate', []),
		step('reserve-stock', ['validate']),
		step('charge', ['reserve-stock']),
		step('ship', ['charge']),
		step('refund', ['charge'], '    join: any_failed'),
		''
	].join('\n');
}

const quiet = () =>
	['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${QUIET}`, 'version: "1.0.0"', 'tasks:', step('only', []), ''].join('\n');

let sagaDigest = '';

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(Number(ENGINE_PORT))) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		...uiEnv(),
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('authoring-sheets') });
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: saga('0.1.0') });
	const applied = await rpc<{ orkflow: { definitionDigest: string } }>('OrkflowService/ApplyOrkflow', { yaml: saga('0.2.0') });
	sagaDigest = applied.orkflow.definitionDigest;
	await rpc('OrkflowService/ApplyOrkflow', { yaml: quiet() });
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: EMPTY_NS, description: 'nothing ever ran here' } });
	// One run that succeeds, so the default namespace's band has figures.
	const runId = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: QUIET })).run.id;
	await waitFor(`run ${runId} succeeded`, async () => {
		const answer = await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId });
		return answer.run?.status === 'STATUS_SUCCEEDED';
	}, 180_000);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`saga ${SAGA} 0.1.0 and 0.2.0 (digest ${sagaDigest}); ${QUIET} ran ${runId}; empty namespace ${EMPTY_NS}`);
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

const sagaUrl = (query = '') => `${UI_ORIGIN}/orkflows/${SAGA}?ns=default${query}`;

/** The colour a token resolves to, as a computed style reports it. */
async function tokenColor(page: Page, token: string): Promise<string> {
	return page.evaluate((name) => {
		const probe = document.createElement('span');
		probe.style.color = `var(${name})`;
		document.body.appendChild(probe);
		const c = getComputedStyle(probe).color;
		probe.remove();
		return c;
	}, token);
}

async function axe(page: Page, label: string): Promise<void> {
	const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze();
	const found = result.violations.map((v) => `${v.id} (${v.nodes.length}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(', ')})`);
	note(`axe ${label}: ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
	expect(found).toEqual([]);
}

async function openPalette(page: Page): Promise<void> {
	await page.keyboard.press('Control+k');
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await expect(page.getByTestId('command-palette-input')).toBeFocused();
}

// What runs past the right edge, named, so a failure says where.
async function pastRightEdge(page: Page): Promise<string[]> {
	return page.evaluate(() =>
		document.documentElement.scrollWidth <= window.innerWidth
			? []
			: [...document.querySelectorAll('body *')]
					.filter((el) => el.getBoundingClientRect().right > window.innerWidth + 0.5)
					.slice(-5)
					.map(
						(el) =>
							`${el.tagName.toLowerCase()}${el.getAttribute('data-testid') ? `[${el.getAttribute('data-testid')}]` : ''}.${el.className.toString().slice(0, 60)} → ${Math.round(el.getBoundingClientRect().right)}`
					)
	);
}

const FIGURES = ['stat-running', 'stat-succeeded', 'stat-failed', 'stat-canceled', 'stat-success-rate', 'stat-p50', 'stat-p95'];

/** Every figure of the band: its size, its colour and whether it was answered; how many rows the band has, the heading's size and where the first register starts. */
async function band(page: Page) {
	return page.evaluate((ids) => {
		const figures = ids.map((id) => {
			const el = document.querySelector(`[data-testid="${id}"]`)!;
			const style = getComputedStyle(el);
			return { id, size: parseFloat(style.fontSize), color: style.color, unavailable: el.getAttribute('data-unavailable') === 'true' };
		});
		const cells = [...document.querySelectorAll('[data-testid="dashboard-counts"] > *')];
		return {
			figures,
			bandHeight: document.querySelector('[data-testid="dashboard-counts"]')!.getBoundingClientRect().height,
			bandRows: new Set(cells.map((el) => Math.round(el.getBoundingClientRect().top))).size,
			h1Size: parseFloat(getComputedStyle(document.querySelector('[data-testid="dashboard-heading"]')!).fontSize),
			registerTop: document.getElementById('dashboard-running-heading')!.getBoundingClientRect().top,
			viewport: window.innerHeight
		};
	}, FIGURES);
}

test('dashboard: the count band is a summary line in ink, one row, the first register in view', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(`${UI_ORIGIN}/?ns=default`);
	await expect(page.getByTestId('stat-succeeded')).not.toHaveText('0', { timeout: 60_000 });
	await expect(page.getByTestId('stat-success-rate')).toHaveAttribute('data-unavailable', 'false');
	const ink = await tokenColor(page, '--color-ink');
	const b = await band(page);
	note(`band at 1280x720: ${JSON.stringify(b)}`);
	for (const f of b.figures) {
		// (The title block's value size is gone; the figure is held under the page's own heading.)
		expect(f.size, `${f.id} is smaller than the page heading`).toBeLessThan(b.h1Size);
		expect(f.unavailable, `${f.id} answered`).toBe(false);
		expect(f.color, `${f.id} is ink`).toBe(ink);
	}
	// A summary line: every cell on one row at this width.
	expect(b.bandRows).toBe(1);
	// The first register starts in the first viewport (the old bound was the bottom title block).
	expect(b.registerTop).toBeLessThan(b.viewport);
	// A status count carries its drawn mark beside its label, not a coloured number.
	await expect(page.getByTestId('stat-failed').locator('xpath=..').locator('.mark-tri')).toHaveCount(1);
	// The page is named in the breadcrumb, and the window in force is the pressed preset.
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('Overview');
	await expect(page.getByTestId('dashboard-window-24h')).toHaveAttribute('aria-current', 'page');
	await page.screenshot({ path: out('screenshot-dashboard-light-1280.png') });
});

test('dashboard: a namespace where nothing finished says unavailable, in the muted face', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/?ns=${EMPTY_NS}`);
	await expect(page.getByTestId('breadcrumb').getByRole('link').first()).toHaveText(EMPTY_NS);
	const muted = await tokenColor(page, '--color-ink-muted');
	for (const id of ['stat-success-rate', 'stat-p50', 'stat-p95']) {
		await expect(page.getByTestId(id)).toHaveAttribute('data-unavailable', 'true');
		await expect(page.getByTestId(id)).toHaveText('unavailable');
		await expect(page.getByTestId(id)).toHaveCSS('color', muted);
	}
	await expect(page.getByTestId('stat-success-rate-note')).toContainText('no run finished');
	await expect(page.getByTestId('stat-succeeded')).toHaveText('0');
	await expect(page.getByTestId('stat-succeeded')).toHaveAttribute('data-unavailable', 'false');
	const b = await band(page);
	expect(b.bandRows).toBe(1);
	await page.screenshot({ path: out('screenshot-dashboard-unavailable.png') });
	await axe(page, 'dashboard, nothing finished (light)');
});

test('orkflows: a register of Name, Version, Digest, Created, labels under the name, with exactly one link per row', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await expect(page.getByTestId(`orkflow-row-${SAGA}`)).toBeVisible();
	const rows = page.locator('tr[data-testid^="orkflow-row-"]');
	const count = await rows.count();
	expect(count).toBeGreaterThanOrEqual(2);
	for (let i = 0; i < count; i++) await expect(rows.nth(i).getByRole('link')).toHaveCount(1);
	const hex = sagaDigest.slice(sagaDigest.indexOf(':') + 1);
	await expect(page.getByTestId(`orkflow-digest-${SAGA}`)).toHaveText(hex.slice(0, 8));
	await expect(page.getByTestId('orkflow-list').locator('thead th')).toHaveText(['Name', 'Version', 'Digest', 'Created']);
	await expect(page.getByTestId(`orkflow-labels-${SAGA}`).locator('.tag')).toContainText(['team=payments']);
	await expect(page.getByTestId(`orkflow-row-${SAGA}`)).toContainText('0.2.0');
	await page.screenshot({ path: out('screenshot-orkflows-light-1280.png') });
	await axe(page, '/orkflows (light)');
	note(`orkflows register: ${count} rows, one link each`);
});

test('orkflow sheet: the DAG beside its versions, the header facts, its runs a tab', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(sagaUrl());
	await expect(page.getByTestId('orkflow-dag')).toBeVisible();
	for (const id of ['validate', 'reserve-stock', 'charge', 'ship', 'refund']) {
		await expect(page.getByTestId(`dag-node-${id}`).locator('rect.dag-outline')).toHaveCount(1);
	}
	await expect(page.getByTestId('orkflow-sheet-versions')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.2.0-shown')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.2.0-current')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.2.0-head')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.1.0')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.1.0-current')).toHaveCount(0);
	// The aside stands beside the drawing at this width.
	const [frame, aside] = await Promise.all([
		page.getByTestId('orkflow-drawing').boundingBox(),
		page.getByTestId('orkflow-sheet-versions').boundingBox()
	]);
	expect(aside!.x).toBeGreaterThan(frame!.x + frame!.width - 1);
	const hex = sagaDigest.slice(sagaDigest.indexOf(':') + 1);
	// The header names what is shown: the Orkflow, its version plate, its definition plate (the digest's
	// first 12 hex characters), and the namespace in the breadcrumb. "current · head" is the aside's marks above.
	await expect(page.getByTestId('orkflow-name')).toHaveText(SAGA);
	await expect(page.getByTestId('orkflow-version')).toContainText('0.2.0');
	await expect(page.locator('main header')).toContainText(hex.slice(0, 4));
	await expect(page.getByTestId('breadcrumb').getByRole('link').first()).toHaveText('default');
	await expect(page.getByTestId('breadcrumb-current')).toHaveText(SAGA);

	// An Orkflow's runs are one of its tabs, and the way to its filtered list.
	await expect(page.getByTestId('tab-runs')).toHaveAttribute('href', /\/runs\?/);
	await expect(page.locator('main header').getByTestId('orkflow-submit-run')).toBeVisible();
	await page.screenshot({ path: out('screenshot-orkflow-overview-light-1440.png') });

	// An older version: shown, neither current nor head.
	await page.locator('[data-testid="orkflow-sheet-version-0.1.0"] a').click();
	await expect(page).toHaveURL(/version=0\.1\.0/);
	await expect(page.getByTestId('orkflow-version')).toContainText('0.1.0');
	await expect(page.getByTestId('orkflow-sheet-version-0.1.0-shown')).toBeVisible();
	await expect(page.getByTestId('orkflow-sheet-version-0.1.0-current')).toHaveCount(0);
	await expect(page.getByTestId('orkflow-sheet-version-0.1.0-head')).toHaveCount(0);
});

test('orkflow sheet: every tab renders, with axe', async ({ page }) => {
	const tabs: [string, string][] = [
		['overview', 'orkflow-dag'],
		['versions', 'version-list'],
		['triggers', 'triggers-scope'],
		['schedules', 'schedules-scope'],
		['edit', 'editor-form']
	];
	for (const [tab, ready] of tabs) {
		await page.goto(sagaUrl(tab === 'overview' ? '' : `&tab=${tab}`));
		await expect(page.getByTestId(ready)).toBeVisible();
		await expect(page.getByTestId(`tab-${tab}`)).toHaveAttribute('aria-current', 'page');
		if (tab === 'versions') await expect(page.getByTestId('callers-table').or(page.getByTestId('callers-empty'))).toBeVisible();
		await expect(page.getByTestId('tab-runs')).toBeVisible();
		await page.screenshot({ path: out(`screenshot-orkflow-${tab}-light-1280.png`), fullPage: true });
		await axe(page, `Orkflow page, ${tab} (light)`);
	}
});

test('palette: on an Orkflow page, Submit a run opens the page’s dialog and sends nothing; a task id jumps to its row', async ({ page }) => {
	const writes: string[] = [];
	page.on('request', (request) => {
		if (/RunService\/(SubmitRun|PlanRun)|OrkflowService\/PlanRun/.test(request.url())) writes.push(request.url());
	});
	await page.goto(sagaUrl());
	await expect(page.getByTestId('orkflow-dag')).toBeVisible();
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('submit');
	await page.getByTestId('command-palette-option-orkflow-submit').click();
	await expect(page.getByTestId('command-palette')).toBeHidden();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-panel')).toBeVisible();
	expect(writes).toEqual([]);
	await page.keyboard.press('Escape');
	await expect(page.getByTestId('orkflow-submit-dialog')).toHaveCount(0);

	// A task id: its row in the register, from this tab and from another.
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('ship');
	await page.getByTestId('command-palette-option-orkflow-task-ship').click();
	await expect(page).toHaveURL(/#task-row-ship$/);
	await expect(page.getByTestId('task-row-ship')).toBeInViewport();

	await page.goto(sagaUrl('&tab=versions'));
	await expect(page.getByTestId('version-list')).toBeVisible();
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('charge');
	await page.getByTestId('command-palette-option-orkflow-task-charge').click();
	await expect(page).toHaveURL(/#task-row-charge$/);
	await expect(page.getByTestId('task-row-charge')).toBeInViewport();
	// The tabs are navigations.
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('schedules');
	await page.getByTestId('command-palette-option-orkflow-tab-schedules').click();
	await expect(page).toHaveURL(/tab=schedules/);
	await expect(page.getByTestId('schedules-scope')).toBeVisible();
	expect(writes).toEqual([]);
	note('palette on the Orkflow page: Submit a run opened the dialog with no SubmitRun or PlanRun; task jumps landed on their rows');
});

test('create orkflow: issues carry their severity classes; the examples are a ruled list', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows/+new?ns=default`);
	// Create Orkflow is a page of Orkflows: the two links whose name contains
	// "Orkflows" are the sidebar's and the breadcrumb's way back, and the
	// breadcrumb names this page.
	await expect(page.getByRole('link', { name: 'Orkflows' })).toHaveCount(2);
	await expect(page.getByTestId('breadcrumb').getByRole('link', { name: 'Orkflows' })).toHaveCount(1);
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('Create Orkflow');
	await expect(page.locator('main header').getByTestId('apply-submit')).toBeVisible();
	await expect(page.locator('main header').getByTestId('apply-validate')).toBeVisible();
	const examples = page.getByTestId('apply-examples');
	await expect(examples).toHaveAttribute('role', 'list');
	const items = examples.locator(':scope > div');
	const n = await items.count();
	expect(n).toBeGreaterThan(0);
	for (let i = 0; i < n; i++) {
		await expect(items.nth(i)).toHaveAttribute('role', 'listitem');
		await expect(items.nth(i).locator('h3')).toHaveCount(1);
		await expect(items.nth(i).getByRole('button', { name: 'Use this example' })).toHaveCount(1);
	}
	const name = `${SAGA}-iface`;
	await fillYaml(page, 'apply-yaml', 
		['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${name}`, 'version: "1.0.0"', 'tasks:', '  - id: copy', '    uses: file', '    with:', '      op: copy', '      srcs: [why]', ''].join('\n')
	);
	await page.getByTestId('apply-validate').click();
	const error = page.locator('[data-testid="apply-validation-issue"][data-severity="error"]').first();
	const warning = page.locator('[data-testid="apply-validation-issue"][data-severity="warning"]').first();
	await expect(error).toHaveClass(/severity-error/);
	await expect(warning).toHaveClass(/severity-warning/);
	await page.screenshot({ path: out('screenshot-apply-issues-light-1280.png'), fullPage: true });
	await axe(page, '/orkflows/+new with issues (light)');
	note(`apply: ${n} examples as list items; error and warning issues carry their severity classes`);
});

test('screens: both themes, desktop and 390 px, with axe', async ({ page }) => {
	const pages: [string, string, string][] = [
		['dashboard', `${UI_ORIGIN}/?ns=default`, 'dashboard-counts'],
		['orkflows', `${UI_ORIGIN}/orkflows?ns=default`, 'orkflow-list'],
		['orkflow', sagaUrl(), 'orkflow-dag'],
		['apply', `${UI_ORIGIN}/orkflows/+new?ns=default`, 'apply-examples']
	];
	for (const theme of ['light', 'dark'] as const) {
		await page.setViewportSize({ width: 1440, height: 900 });
		await page.goto(`${UI_ORIGIN}/?ns=default`);
		await page.getByTestId(`theme-${theme}`).click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
		for (const [label, url, ready] of pages) {
			await page.goto(url);
			await expect(page.getByTestId(ready)).toBeVisible();
			await page.screenshot({ path: out(`screenshot-${label}-${theme}-1440.png`), fullPage: true });
			await axe(page, `${label} (${theme}, 1440)`);
		}
		await page.setViewportSize({ width: 390, height: 844 });
		for (const [label, url, ready] of pages) {
			await page.goto(url);
			await expect(page.getByTestId(ready)).toBeAttached();
			await expect(page.getByTestId('breadcrumb')).toBeVisible();
			expect(await pastRightEdge(page), `${label} at 390 px: nothing runs past the right edge`).toEqual([]);
			await page.screenshot({ path: out(`screenshot-${label}-${theme}-390.png`), fullPage: true });
		}
		// The band wraps into ruled lines of cells, never into a stack of tiles:
		// below 400 px the four status counts are a 2 × 2 grid over the three figures.
		await page.goto(`${UI_ORIGIN}/?ns=default`);
		const tops = await page.evaluate(() =>
			[...document.querySelectorAll('[data-testid="dashboard-counts"] > *')].map((el) => Math.round(el.getBoundingClientRect().top))
		);
		expect(new Set(tops).size, `band rows at 390 px: ${tops.join(',')}`).toBe(3);
	}
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(`${UI_ORIGIN}/?ns=default`);
	await page.getByTestId('theme-system').click();
});

// Create Orkflow lives at /orkflows/+new, a segment no Orkflow name can be
// (`+` fails ^[a-z][a-z0-9-]*$), so an Orkflow called new keeps /orkflows/new.
test('an Orkflow named new opens its own page; Create Orkflow is at /orkflows/+new', async ({ page }) => {
	const ns = `sheets-new-${STAMP}`;
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: ns } });
	await rpc('OrkflowService/ApplyOrkflow', { namespace: ns, yaml: quiet().replace(`name: ${QUIET}`, 'name: new') });

	await page.goto(`${UI_ORIGIN}/orkflows/new?ns=${ns}`);
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('new');
	await expect(page.getByTestId('orkflow-version')).toBeVisible();
	await expect(page.getByTestId('apply-form')).toHaveCount(0);

	await page.goto(`${UI_ORIGIN}/orkflows?ns=${ns}`);
	await page.getByTestId('orkflows-create').click();
	await expect(page).toHaveURL(new RegExp(`/orkflows/\\+new\\?ns=${ns}$`));
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('Create Orkflow');
	await expect(page.getByTestId('apply-form')).toBeVisible();
	note('an Orkflow named new opened /orkflows/new as its own page; the Orkflows page links Create Orkflow at /orkflows/+new');
});
