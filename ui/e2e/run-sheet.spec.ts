import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
import { mkdirSync, writeFileSync } from 'node:fs';
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
	waitFor,
	waitForEngine
} from './harness';
import { taskMapCellCenter } from './uiHelpers';

// The run sheet (UI revamp, stage 2), against a real engine and Orker on real
// Temporal and MinIO:
//
//   DAG        a small run opens on its DAG on the grid: status drawn per
//              node, exactly one <rect> per node, a failure handler dashed by
//              ATTRIBUTE, a node link selecting its task (task=, the card);
//   map        a run of more than 80 tasks opens on its task map, drawn only
//              after the last card is attached; a click and the keyboard
//              select a task and bring its card into view; F jumps to the
//              failed cell; the legend counts; the readout follows the pointer
//              and, with the pointer away, the keyboard cursor;
//   bound      81 tasks: the DAG on request; 301: never laid out, the
//              selected task's neighbourhood instead — on the run page and on
//              the Orkflow page alike;
//   aside      the versions table (THIS RUN, CURRENT, HEAD), the failure in its
//              cloud, the failing image, the commands that reproduce it;
//   degraded   no definition (GetOrkflow refused): a NOTE with the reason, the
//              map in run order, the aside whole, every card; the versions
//              listing refused: an error with Retry, then the table;
//   live       the title block's STATUS turns RUNNING blue → FAILED red with
//              no reload;
//   palette    on a run page a task id selects the task; Cancel run… opens
//              the confirmation and sends nothing;
//   register   a run the list saw RUNNING that fails is held in place by the
//              poll, on its new ground, its status still the 4th cell; held
//              after a reload; cleared by opening it; a run that failed while
//              the list was closed is held on the next visit; Mark all seen
//              (button and palette) clears;
//   compare    two runs side by side;
//   screens    both themes, desktop and 390 px; axe (WCAG 2.2 AA) on the run
//              page in both views, a failed run, the register with a held
//              row, and compare; DOM per task card recorded.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18961';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14961';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-run-sheet-${STAMP}`;
const BUCKET = ownBucket('run-sheet');
const SAGA = `sheet-saga-${STAMP}`;
const WIDE = `sheet-wide-${STAMP}`;
const HUGE = `sheet-huge-${STAMP}`;
const TWENTY = `sheet-twenty-${STAMP}`;
const LIVE = `sheet-live-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-sheet')
	: path.join(uiDir, 'test-results', 'run-sheet');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);


const step = (id: string, needs: string[], command: string[], extra = '') =>
	[
		`  - id: ${id}`,
		...(needs.length ? [`    needs: [${needs.join(', ')}]`] : []),
		...(extra ? [extra] : []),
		'    uses: shell',
		'    network: none',
		'    timeout_seconds: 300',
		`    with: {op: exec, command: [${command.map((c) => JSON.stringify(c)).join(', ')}]}`
	].join('\n');

const header = (name: string, version: string, description: string) =>
	['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${name}`, `version: "${version}"`, `description: ${description}`, 'tasks:'].join('\n');

// The sketch's own drawing: a charge that fails, a ship that never starts, a
// failure handler that runs, and a report that joins them.
function saga(version: string): string {
	const ok = ['echo', 'ok'];
	return [
		header(SAGA, version, `The run sheet's saga, ${version}.`),
		step('validate', [], ok),
		step('reserve-stock', ['validate'], ok),
		step('charge', ['reserve-stock'], ['sh', '-c', 'echo declined >&2; exit 3']),
		step('ship', ['charge'], ok),
		step('refund', ['charge'], ok, '    join: any_failed'),
		step('report', ['ship', 'refund'], ok, '    join: all_done'),
		''
	].join('\n');
}

// A gate that holds, and `count` tasks behind it: a run of this is canceled
// while its gate holds, so it has every task and runs (almost) none.
function gated(name: string, count: number): string {
	const lines = [header(name, '0.1.0', `${count + 1} tasks behind a gate.`), step('gate', [], ['sleep', '600'])];
	for (let i = 1; i <= count; i++) lines.push(step(`t${i}`, i >= 2 && i <= 3 ? [`t${i - 1}`] : ['gate'], ['echo', 'ok']));
	return lines.join('\n') + '\n';
}

// The map draws tasks in DAG order (layer, then declaration): the gate, then
// t1 and t4…tN in layer 1, then t2 and t3 alone in layers 2 and 3. gatedCell
// is the map cell of task t<i> in a run of gated(name, count).
function gatedCell(count: number, i: number): number {
	if (i === 1) return 1;
	if (i === 2) return count - 1;
	if (i === 3) return count;
	return i - 2;
}

function live(): string {
	return [header(LIVE, '0.1.0', 'Runs, then fails.'), step('hold', [], ['sh', '-c', 'sleep "${HOLD:-30}"; exit 1'], '    env: {HOLD: "30"}'), ''].join('\n');
}

const live1 = (): string =>
	[header(LIVE, '0.2.0', 'Fails at once.'), step('hold', [], ['sh', '-c', 'exit 1']), ''].join('\n');

async function status(runId: string): Promise<string> {
	return (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status ?? '';
}

async function submit(name: string, version = ''): Promise<string> {
	const body: Record<string, unknown> = { orkflowName: name };
	if (version) body.orkflowVersion = version;
	return (await rpc<{ run: { id: string } }>('RunService/SubmitRun', body)).run.id;
}

async function terminal(runId: string, timeout = 240_000): Promise<string> {
	let s = '';
	await waitFor(`run ${runId} finished`, async () => {
		s = await status(runId);
		return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(s);
	}, timeout);
	return s;
}

// The listing can still show a just-finished run as RUNNING for a moment
// (docs/UI_CLI_PARITY.md, "Run browsing"); a visit that must see it FAILED
// waits for the listing to say so.
async function listedFailed(runId: string): Promise<void> {
	await waitFor(`run ${runId} is listed FAILED`, async () => {
		const answer = await rpc<{ runs?: { id: string; status?: string }[] }>('RunService/ListRuns', { pageSize: 50 });
		return (answer.runs ?? []).some((r) => r.id === runId && r.status === 'STATUS_FAILED');
	}, 120_000);
}

async function running(runId: string): Promise<void> {
	await waitFor(`run ${runId} has a running task`, async () => {
		const answer = await rpc<{ run?: { taskRuns?: { status?: string }[] } }>('RunService/GetRun', { runId });
		return (answer.run?.taskRuns ?? []).some((t) => t.status === 'STATUS_RUNNING');
	}, 180_000);
}

let sagaRun = '';
let sagaRun2 = '';
let wideRun = '';
let hugeRun = '';
let twentyRun = '';

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(15 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('run-sheet') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: saga('0.1.0') });
	sagaRun = await submit(SAGA);
	expect(await terminal(sagaRun)).toBe('STATUS_FAILED');
	// A later version, so the table has a CURRENT and a HEAD that are not
	// this run's, and a second run to compare with.
	await rpc('OrkflowService/ApplyOrkflow', { yaml: saga('0.2.0') });
	sagaRun2 = await submit(SAGA, '0.2.0');
	await terminal(sagaRun2);

	await rpc('OrkflowService/ApplyOrkflow', { yaml: gated(WIDE, 80) });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: gated(HUGE, 300) });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: gated(TWENTY, 19) });
	wideRun = await submit(WIDE);
	hugeRun = await submit(HUGE);
	twentyRun = await submit(TWENTY);
	for (const id of [wideRun, hugeRun, twentyRun]) await running(id);
	for (const id of [wideRun, hugeRun, twentyRun]) await rpc('RunService/CancelRun', { runId: id, reason: 'run-sheet: drawn, not run' });
	for (const id of [wideRun, hugeRun, twentyRun]) expect(await terminal(id)).toBe('STATUS_CANCELED');
	await rpc('OrkflowService/ApplyOrkflow', { yaml: live() });
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`saga ${sagaRun} (0.1.0, FAILED), ${sagaRun2} (0.2.0); 81 tasks ${wideRun}; 301 tasks ${hugeRun}; 20 tasks ${twentyRun}`);
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

const runUrl = (id: string, query = '') => `${UI_ORIGIN}/runs/${id}?ns=default${query}`;

async function openPalette(page: Page): Promise<void> {
	await page.keyboard.press('Control+k');
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await expect(page.getByTestId('command-palette-input')).toBeFocused();
}

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
	const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
	const found = result.violations.map((v) => `${v.id} (${v.nodes.length}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(', ')})`);
	note(`axe ${label}: ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
	expect(found).toEqual([]);
}

async function inViewport(page: Page, testid: string): Promise<boolean> {
	return page.evaluate((id) => {
		const card = document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect();
		const top = document.querySelector('header')!.getBoundingClientRect().bottom;
		const bottom = document.querySelector('[data-testid="title-block"]')!.getBoundingClientRect().top;
		return card.top >= top - 1 && card.bottom <= bottom + 1;
	}, testid);
}

test('DAG: a small run opens on its DAG, drawn by status, one rect per node, the handler dashed by attribute', async ({ page }) => {
	await page.goto(runUrl(sagaRun));
	await expect(page.getByTestId('run-status')).toHaveText('FAILED');
	await expect(page.getByTestId('run-view-dag')).toHaveAttribute('aria-selected', 'true');
	await expect(page.getByTestId('run-dag')).toBeVisible();
	for (const id of ['validate', 'reserve-stock', 'charge', 'ship', 'refund', 'report']) {
		await expect(page.getByTestId(`run-dag-node-${id}`).locator('rect')).toHaveCount(1);
	}
	await expect(page.getByTestId('run-dag-node-charge')).toHaveAttribute('data-status', 'FAILED');
	await expect(page.getByTestId('run-dag-node-validate')).toHaveAttribute('data-status', 'SUCCEEDED');
	await expect(page.getByTestId('run-dag-node-refund').locator('rect')).toHaveAttribute('stroke-dasharray', '5 4');
	await expect(page.getByTestId('run-dag-edge-charge-refund')).toHaveAttribute('stroke-dasharray', '5 4');
	await expect(page.getByTestId('run-dag-node-charge').locator('rect')).not.toHaveAttribute('stroke-dasharray', /.+/);
	// The Orkflow page's ids are its own.
	await expect(page.getByTestId('orkflow-dag')).toHaveCount(0);
	await page.screenshot({ path: out('screenshot-run-failed-dag-light.png') });

	// A node is a link that selects its task: the URL's task=, the card.
	await page.getByTestId('run-dag-node-charge').locator('a').click();
	await expect(page).toHaveURL(/task=charge/);
	await expect(page.getByTestId('task-charge')).toHaveAttribute('data-selected', 'true');
	expect(await inViewport(page, 'task-charge')).toBe(true);
	await expect(page.getByTestId('run-dag-node-charge').locator('a')).toHaveAttribute('aria-current', 'true');
});

test('aside: versions with THIS RUN / CURRENT / HEAD, the failure in its cloud, the image, the reproduce commands', async ({ page }) => {
	await page.goto(runUrl(sagaRun));
	await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
	await expect(page.getByTestId('run-version-0.1.0-this')).toHaveText('This run');
	await expect(page.getByTestId('run-version-0.2.0-current')).toBeVisible();
	await expect(page.getByTestId('run-version-0.2.0-head')).toBeVisible();
	await expect(page.getByTestId('run-version-0.1.0-current')).toHaveCount(0);
	await expect(page.getByTestId('run-error-tag')).toHaveText('Version 0.1.0 · charge failed');
	await expect(page.getByTestId('run-error')).toBeVisible();
	await expect(page.getByTestId('run-error-attempts')).toHaveText('attempt 1 of 1');
	await expect(page.getByTestId('run-failed-image')).toContainText('activity-shell');
	await expect(page.getByTestId('run-reproduce-get')).toHaveText(`orkctl --namespace 'default' 'run' 'get' '${sagaRun}' '--json'`);
	await expect(page.getByTestId('run-reproduce-again')).toHaveText(`orkctl --namespace 'default' 'run' 'again' '${sagaRun}'`);
	// The title block: what this page is, and never the run id.
	await expect(page.getByTestId('title-block-orkflow')).toContainText(SAGA);
	await expect(page.getByTestId('title-block-version')).toContainText('0.1.0');
	await expect(page.getByTestId('title-block-status')).toContainText('FAILED');
	await expect(page.getByTestId('title-block')).not.toContainText(sagaRun);
	const digest = (await rpc<{ run: { orkflowDefinitionDigest: string } }>('RunService/GetRun', { runId: sagaRun })).run.orkflowDefinitionDigest;
	await expect(page.getByTestId('title-block-digest')).toContainText(digest.split(':').pop()!.slice(0, 8));
	await page.screenshot({ path: out('screenshot-run-aside-light.png') });

	// One version that is this run's, the current and the head: its three
	// marks wrap inside the aside rather than run past it.
	await page.goto(runUrl(wideRun));
	await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
	const aside = await page.getByRole('complementary', { name: 'About this run' }).boundingBox();
	for (const mark of ['this', 'current', 'head']) {
		const box = await page.getByTestId(`run-version-0.1.0-${mark}`).boundingBox();
		expect(box!.x + box!.width, `the ${mark} mark inside the aside`).toBeLessThanOrEqual(aside!.x + aside!.width + 0.5);
	}
});

test('map: past 80 tasks the run opens on its map, painted only after the last card is attached', async ({ page }) => {
	await page.addInitScript(() => {
		const w = window as unknown as { __cardsAtPaint?: number };
		new MutationObserver(() => {
			if (w.__cardsAtPaint !== undefined) return;
			if (document.querySelector('[data-testid="task-map"][data-painted]'))
				w.__cardsAtPaint = document.querySelectorAll('[data-testid^="task-deep-link-"]').length;
		}).observe(document, { subtree: true, attributes: true, childList: true });
	});
	await page.goto(runUrl(wideRun));
	await expect(page.getByTestId('run-view-map')).toHaveAttribute('aria-selected', 'true');
	const map = page.getByTestId('task-map');
	await expect(map).toHaveAttribute('data-painted', 'true');
	await expect(map).toHaveAttribute('data-cells', '81');
	await expect(map).toHaveAttribute('data-order', 'dag');
	expect(await page.evaluate(() => (window as unknown as { __cardsAtPaint?: number }).__cardsAtPaint)).toBe(81);
	await expect(page.getByTestId('task-map-legend')).toBeVisible();
	// A canceled run says why it ended, and draws no failure's cloud: an ink
	// NOTE, its code in ink — red is for failure and attention only.
	await expect(page.getByTestId('run-error')).toContainText('RUN_CANCELED');
	await expect(page.getByTestId('run-error-tag')).toHaveCount(0);
	await expect(page.getByTestId('run-error')).toHaveAttribute('data-tone', 'note');
	expect(await page.getByTestId('run-error-code').evaluate((el) => getComputedStyle(el).color)).toBe(await tokenColor(page, '--color-ink'));
	await page.screenshot({ path: out('screenshot-run-map-81-light.png') });

	// A click selects the task under it and brings its card into view.
	const at = await taskMapCellCenter(page, gatedCell(80, 40));
	await page.mouse.click(at.x, at.y);
	await expect(page).toHaveURL(/task=t40\b/);
	await expect(map).toHaveAttribute('data-selected', 't40');
	await expect(page.getByTestId('task-t40')).toHaveAttribute('data-selected', 'true');
	await expect.poll(() => inViewport(page, 'task-t40')).toBe(true);

	// The readout beside the pointer names the cell under it.
	await page.evaluate(() => window.scrollTo(0, 0));
	const hover = await taskMapCellCenter(page, gatedCell(80, 5));
	await page.mouse.move(hover.x, hover.y);
	await expect(page.getByTestId('task-map-readout')).toBeVisible();
	await expect(page.getByTestId('task-map-readout')).toContainText(/\bt5\b/);
	// With the pointer away, it follows the keyboard cursor.
	await page.mouse.move(1, 1);
	const listbox = page.getByRole('listbox', { name: /Task map/ });
	await listbox.focus();
	await page.keyboard.press('Home', { delay: 0 });
	await page.keyboard.press('Control+Home');
	await page.keyboard.press('ArrowRight');
	await expect(page.getByTestId('task-map-readout')).toContainText(/\bt1\b/);
	await expect(page.getByTestId('task-map-cursor')).toBeVisible();
	// The next cell in reading order is t4: t2 and t3 sit in later layers.
	await page.keyboard.press('ArrowRight');
	await expect(page.getByTestId('task-map-readout')).toContainText(/\bt4\b/);
	await page.keyboard.press('Enter');
	await expect(page).toHaveURL(/task=t4\b/);
	await expect.poll(() => inViewport(page, 'task-t4')).toBe(true);
});

test('map: F jumps to the failed cell, and the legend counts every status present', async ({ page }) => {
	await page.goto(runUrl(sagaRun, '&view=map'));
	const map = page.getByTestId('task-map');
	await expect(map).toHaveAttribute('data-painted', 'true');
	await expect(page.getByTestId('task-map-legend-failed')).toContainText('1');
	await expect(page.getByTestId('task-map-legend-succeeded')).toBeVisible();
	await page.getByRole('listbox', { name: /Task map/ }).focus();
	await page.keyboard.press('f');
	await expect(page.getByTestId('task-map-readout')).toContainText('charge · FAILED');
	await page.keyboard.press('Enter');
	await expect(page).toHaveURL(/task=charge/);
	await expect(map).toHaveAttribute('data-selected', 'charge');
	await page.screenshot({ path: out('screenshot-run-failed-map-light.png') });
});

test('bound: 81 tasks draw on request, 301 never, on the run page and the Orkflow page alike', async ({ page }) => {
	await page.goto(runUrl(wideRun));
	await page.getByTestId('run-view-dag').click();
	await expect(page).toHaveURL(/view=dag/);
	await expect(page.getByTestId('run-dag-large')).toBeVisible();
	await expect(page.locator('[data-testid^="run-dag-node-"]')).toHaveCount(0);
	await page.getByTestId('run-dag-draw-all').click();
	await expect(page.locator('g[data-testid^="run-dag-node-"]')).toHaveCount(81);

	await page.goto(runUrl(hugeRun));
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-cells', '301');
	await page.getByTestId('run-view-dag').click();
	await expect(page.getByTestId('run-dag-too-large')).toContainText('301 tasks');
	await expect(page.locator('[data-testid^="run-dag-node-"]')).toHaveCount(0);
	// With a task selected, its neighbourhood is drawn instead.
	await page.getByTestId('run-view-map').click();
	await page.mouse.click(...(Object.values(await taskMapCellCenter(page, gatedCell(300, 2))) as [number, number]));
	await expect(page).toHaveURL(/task=t2\b/);
	await page.getByTestId('run-view-dag').click();
	await expect(page.locator('g[data-testid^="run-dag-node-"]')).toHaveCount(3);
	for (const id of ['t1', 't2', 't3']) await expect(page.getByTestId(`run-dag-node-${id}`)).toBeVisible();
	await page.screenshot({ path: out('screenshot-run-neighbourhood-light.png') });

	await page.goto(`${UI_ORIGIN}/orkflows/${WIDE}?ns=default`);
	await expect(page.getByTestId('dag-draw-all')).toBeVisible();
	await expect(page.locator('[data-testid^="dag-node-"]')).toHaveCount(0);
	await page.getByTestId('dag-draw-all').click();
	await expect(page.locator('g[data-testid^="dag-node-"]')).toHaveCount(81);
	await page.goto(`${UI_ORIGIN}/orkflows/${HUGE}?ns=default`);
	await expect(page.getByTestId('dag-too-large')).toBeVisible();
	await expect(page.getByTestId('orkflow-dag')).toHaveCount(0);
	await expect(page.locator('[data-testid^="dag-node-"]')).toHaveCount(0);
	await expect(page.getByTestId('task-row-t300')).toBeAttached();
});

test('degraded: with no definition the map follows the run order, the DAG says why, and the rest stands', async ({ page }) => {
	await page.route('**/orkstack.v1.OrkflowService/GetOrkflow', (route) =>
		route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ code: 'unavailable', message: 'held by run-sheet' }) })
	);
	await page.goto(runUrl(sagaRun));
	await expect(page.getByTestId('run-view-map')).toHaveAttribute('aria-selected', 'true');
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-order', 'run');
	await page.getByTestId('run-view-dag').click();
	await expect(page.getByTestId('run-drawing-unavailable')).toContainText('held by run-sheet');
	await expect(page.getByTestId('run-error')).toBeVisible();
	await expect(page.getByTestId('run-reproduce-get')).toBeVisible();
	await expect(page.getByTestId('title-block-digest')).not.toHaveText('—');
	await expect(page.locator('[data-testid^="task-deep-link-"]')).toHaveCount(6);
	await page.screenshot({ path: out('screenshot-run-no-definition-light.png') });
});

test('degraded: a refused versions listing is an error with Retry, then the table', async ({ page }) => {
	let refuse = true;
	await page.route('**/orkstack.v1.OrkflowService/ListOrkflowVersions', (route) =>
		refuse
			? route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ code: 'unavailable', message: 'versions held' }) })
			: route.continue()
	);
	await page.goto(runUrl(sagaRun));
	await expect(page.getByTestId('run-versions-error')).toContainText('versions held');
	await expect(page.getByTestId('run-error')).toBeVisible();
	refuse = false;
	await page.getByTestId('run-versions-retry').click();
	await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
	await expect(page.getByTestId('run-version-0.1.0')).toBeVisible();
});

test('palette: a task id selects the task; Cancel run… opens the confirmation and sends nothing', async ({ page }) => {
	await page.goto(runUrl(sagaRun));
	await expect(page.getByTestId('run-status')).toHaveText('FAILED');
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('refund');
	await expect(page.getByTestId('command-palette-option-task-refund')).toBeVisible();
	await page.keyboard.press('Enter');
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
	await expect(page).toHaveURL(/task=refund/);
	await expect(page.getByTestId('task-refund')).toHaveAttribute('data-selected', 'true');
	await expect.poll(() => inViewport(page, 'task-refund')).toBe(true);

	const liveRun = await submit(LIVE);
	await running(liveRun);
	const cancels: string[] = [];
	page.on('request', (r) => {
		if (r.url().includes('RunService/CancelRun')) cancels.push(r.url());
	});
	await page.goto(runUrl(liveRun));
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING');
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('cancel');
	await page.getByTestId('command-palette-option-run-cancel').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	await page.getByTestId('cancel-run-dismiss').click();
	await expect(page.getByTestId('cancel-run-confirm')).toHaveCount(0);
	expect(cancels).toEqual([]);
	note(`palette: task jump selected refund on ${sagaRun}; Cancel run… on ${liveRun} opened the confirmation and sent no CancelRun`);
});

test('live: the title block STATUS turns RUNNING blue → FAILED red with no reload', async ({ page }) => {
	const liveRun = await submit(LIVE);
	await running(liveRun);
	await page.goto(runUrl(liveRun));
	// A mark on this document: a reload would drop it. (A same-document URL
	// update, as the page makes for its own state, keeps it.)
	await page.evaluate(() => ((window as unknown as { __sameDocument?: boolean }).__sameDocument = true));
	const value = page.getByTestId('title-block-status').locator('dd > span').first();
	await expect(value).toContainText('RUNNING');
	// The colour is asserted with a retrying matcher: the text and the class
	// that colours it arrive in the same update, but a computed style read the
	// instant the text matched has been seen, on a loaded host, to still carry
	// the ink colour of the frame before.
	await expect(value).toHaveCSS('color', await tokenColor(page, '--color-blue'));
	await page.screenshot({ path: out('screenshot-run-live-light.png') });
	await expect(value).toContainText('FAILED', { timeout: 120_000 });
	await expect(value).toHaveCSS('color', await tokenColor(page, '--color-red'));
	expect(await page.evaluate(() => (window as unknown as { __sameDocument?: boolean }).__sameDocument)).toBe(true);
	// It failed while watched: the cloud is there.
	await expect(page.getByTestId('run-error')).toBeVisible();
});

test('register: a run seen RUNNING that fails is held in place, survives a reload, and clears when opened', async ({ page }) => {
	const liveRun = await submit(LIVE);
	await running(liveRun);
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const row = page.getByTestId(`run-row-${liveRun}`);
	await expect(row.locator('td').nth(3)).toHaveText('RUNNING');
	await expect(row).not.toHaveAttribute('data-held', /.+/);
	// The poll patches it in place: no reload, the row stays where it is.
	await expect(row).toHaveAttribute('data-held', 'true', { timeout: 120_000 });
	await expect(row).toHaveAttribute('data-held-from', 'RUNNING');
	await expect(page.getByTestId(`run-row-held-${liveRun}`)).toHaveAttribute('aria-label', /it was running/);
	expect((await row.locator('td:nth-child(4)').textContent())?.trim()).toBe('FAILED');
	expect(await row.evaluate((el) => getComputedStyle(el).backgroundColor)).toBe(
		await page.evaluate(() => {
			const probe = document.createElement('span');
			probe.style.backgroundColor = 'var(--color-red-tint)';
			document.body.appendChild(probe);
			const c = getComputedStyle(probe).backgroundColor;
			probe.remove();
			return c;
		})
	);
	await expect(page.getByTestId('runs-held-summary')).toContainText('changed since you last looked');
	await expect(page.getByTestId('runs-held-summary')).not.toContainText(liveRun);
	await page.screenshot({ path: out('screenshot-runs-held-light.png') });
	await axe(page, 'runs register with a held row (light)');

	await page.reload();
	await expect(page.getByTestId(`run-row-${liveRun}`)).toHaveAttribute('data-held', 'true');

	await page.getByTestId(`run-row-${liveRun}`).getByRole('link', { name: liveRun }).click();
	await expect(page.getByTestId('run-status')).toHaveText('FAILED');
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${liveRun}`)).toBeVisible();
	await expect(page.getByTestId(`run-row-${liveRun}`)).not.toHaveAttribute('data-held', /.+/);
});

test('register: a run that failed while the list was closed is held; Mark all seen clears, from the button and the palette', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId('run-list')).toBeVisible();
	// The visit is recorded once the page is live: this browser's first look
	// at the namespace's list.
	await page.waitForFunction(() => (localStorage.getItem('ork-runs-seen-v1') ?? '').includes('"default"'));
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await rpc('OrkflowService/ApplyOrkflow', { yaml: live1() });
	const away = await submit(LIVE, '0.2.0');
	expect(await terminal(away)).toBe('STATUS_FAILED');
	await listedFailed(away);
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${away}`)).toHaveAttribute('data-held', 'true');
	await expect(page.getByTestId(`run-row-${away}`)).toHaveAttribute('data-held-from', 'UNSPECIFIED');
	await expect(page.getByTestId(`run-row-held-${away}`)).toHaveAttribute('aria-label', /failed while you were away/);
	await page.getByTestId('runs-held-clear').click();
	await expect(page.locator('tr[data-held="true"]')).toHaveCount(0);
	await expect(page.getByTestId('runs-held-summary')).toHaveCount(0);

	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	const away2 = await submit(LIVE, '0.2.0');
	expect(await terminal(away2)).toBe('STATUS_FAILED');
	await listedFailed(away2);
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${away2}`)).toHaveAttribute('data-held', 'true');
	await openPalette(page);
	await expect(page.getByTestId(`command-palette-option-changed-${away2}`)).toBeVisible();
	await page.getByTestId('command-palette-input').fill('mark all');
	await page.getByTestId('command-palette-option-runs-mark-seen').click();
	await expect(page.locator('tr[data-held="true"]')).toHaveCount(0);
});

test('compare: two runs side by side, FROM quiet and TO marked', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs/${sagaRun}/compare?ns=default&to=${sagaRun2}`);
	await expect(page.getByTestId('run-diff-from')).toHaveText(sagaRun);
	await expect(page.getByTestId('run-diff-to')).toHaveText(sagaRun2);
	await expect(page.getByTestId('title-block-from')).toContainText('FAILED');
	await expect(page.getByTestId('title-block')).not.toContainText(sagaRun);
	await page.screenshot({ path: out('screenshot-compare-light.png'), fullPage: true });
	await axe(page, 'compare (light)');
});

test('screens: both themes, desktop and 390 px; axe on the run page in both views and on a failed run', async ({ page }) => {
	for (const theme of ['light', 'dark'] as const) {
		await page.setViewportSize({ width: 1440, height: 900 });
		await page.goto(runUrl(sagaRun));
		await page.getByTestId(`theme-${theme}`).click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
		await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
		await page.screenshot({ path: out(`screenshot-run-failed-dag-${theme}-1440.png`) });
		await axe(page, `failed run, DAG view (${theme})`);
		await page.getByTestId('run-view-map').click();
		await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
		await page.screenshot({ path: out(`screenshot-run-failed-map-${theme}-1440.png`) });
		await axe(page, `failed run, map view (${theme})`);
		await page.goto(runUrl(wideRun));
		await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
		await page.screenshot({ path: out(`screenshot-run-map-81-${theme}-1440.png`) });
		await page.goto(`${UI_ORIGIN}/runs?ns=default`);
		await page.screenshot({ path: out(`screenshot-runs-${theme}-1440.png`) });
		await page.goto(`${UI_ORIGIN}/runs/${sagaRun}/compare?ns=default&to=${sagaRun2}`);
		await page.screenshot({ path: out(`screenshot-compare-${theme}-1440.png`) });
		await axe(page, `compare (${theme})`);

		await page.setViewportSize({ width: 390, height: 844 });
		for (const [name, url] of [
			['run-failed', runUrl(sagaRun)],
			['run-map', runUrl(wideRun)],
			['runs', `${UI_ORIGIN}/runs?ns=default`],
			['compare', `${UI_ORIGIN}/runs/${sagaRun}/compare?ns=default&to=${sagaRun2}`]
		]) {
			await page.goto(url);
			await expect(page.getByTestId('title-block')).toBeVisible();
			// What runs past the right edge, named, so a failure says where.
			const past = await page.evaluate(() =>
				document.documentElement.scrollWidth <= window.innerWidth
					? []
					: [...document.querySelectorAll('body *')]
							.filter((el) => el.getBoundingClientRect().right > window.innerWidth + 0.5)
							.slice(-5)
							.map((el) => `${el.tagName.toLowerCase()}${el.getAttribute('data-testid') ? `[${el.getAttribute('data-testid')}]` : ''}.${el.className.toString().slice(0, 60)} → ${Math.round(el.getBoundingClientRect().right)}`)
			);
			expect(past, `${name} at 390 px: nothing runs past the right edge`).toEqual([]);
			await page.screenshot({ path: out(`screenshot-${name}-${theme}-390.png`) });
		}
	}
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(runUrl(sagaRun));
	await page.getByTestId('theme-system').click();
});

// DOM elements per task card on a 20-task run. With ORK_UI_BASELINE_BUILD
// naming another UI build (`vite build` output of the tree to compare with),
// the same run is opened there too and both counts are recorded: that is how
// the stage's before/after is taken. A proof sets no baseline and records the
// after alone.
async function domPerCard(page: Page, origin: string): Promise<{ cards: number; elements: number; perCard: number; max: number }> {
	await page.goto(`${origin}/runs/${twentyRun}?ns=default`);
	await expect(page.locator('[data-testid^="task-deep-link-"]')).toHaveCount(20);
	const counts = await page.evaluate(() =>
		[...document.querySelectorAll('[data-testid="task-list"] > [data-testid^="task-"]')].map((el) => el.querySelectorAll('*').length + 1)
	);
	const total = counts.reduce((a, b) => a + b, 0);
	return { cards: counts.length, elements: total, perCard: Math.round((total / counts.length) * 10) / 10, max: Math.max(...counts) };
}

// The listeners on the document, by type, as DevTools reports them.
async function documentListeners(page: Page): Promise<Record<string, number>> {
	const cdp = await page.context().newCDPSession(page);
	const { result } = await cdp.send('Runtime.evaluate', { expression: 'document' });
	const { listeners } = await cdp.send('DOMDebugger.getEventListeners', { objectId: result.objectId! });
	await cdp.detach();
	const byType: Record<string, number> = {};
	for (const l of listeners) byType[l.type] = (byType[l.type] ?? 0) + 1;
	return byType;
}

test('DOM per task card is recorded', async ({ page }) => {
	const after = await domPerCard(page, UI_ORIGIN);
	const record: Record<string, unknown> = { run: twentyRun, after };
	note(`DOM per task card (20-task run), this tree: ${JSON.stringify(after)}`);
	// A closed popover listens to nothing: 20 cards add no document listener,
	// and an open one adds its pointerdown and keydown until it closes.
	const closed = await documentListeners(page);
	note(`document listeners with every popover closed: ${JSON.stringify(closed)}`);
	expect(closed.pointerdown ?? 0).toBeLessThan(20);
	await page.getByTestId('run-inspect-cli-trigger').click();
	await expect(page.getByTestId('run-inspect-cli-command')).toBeVisible();
	const opened = await documentListeners(page);
	expect(opened.pointerdown ?? 0).toBe((closed.pointerdown ?? 0) + 1);
	await page.keyboard.press('Escape');
	await expect(page.getByTestId('run-inspect-cli-command')).toHaveCount(0);
	expect((await documentListeners(page)).pointerdown ?? 0).toBe(closed.pointerdown ?? 0);
	record.documentListeners = closed;
	const baseline = process.env.ORK_UI_BASELINE_BUILD;
	if (baseline) {
		const port = String(Number(UI_PORT) + 1);
		spawnLogged(out('ui-baseline.log'), 'node', [baseline], { HOST: '127.0.0.1', PORT: port, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') }, uiDir);
		const origin = `http://127.0.0.1:${port}`;
		await waitFor('baseline ui', async () => (await fetch(`${origin}/orkflows`).catch(() => null))?.ok ?? false);
		record.before = await domPerCard(page, origin);
		record.baseline = baseline;
		note(`DOM per task card (20-task run), baseline ${baseline}: ${JSON.stringify(record.before)}`);
	}
	writeFileSync(out('dom-per-card.json'), JSON.stringify(record, null, 2) + '\n');
	expect(after.cards).toBe(20);
});
