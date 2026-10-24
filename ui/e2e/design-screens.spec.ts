import { expect, test, type Page } from './test';
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
	waitFor,
	waitForEngine
} from './harness';

// The design screens (UI revamp, finish), against a real engine and Orker on
// real Temporal and MinIO: the key pages — the runs register, a failed run on
// its DAG and on its task map, an Orkflow page, Apply, the dashboard and one
// Operate page — in light and dark, at 1440 × 900, 390 × 844 and 320 × 640,
// each as its first viewport and as the full page, written under
// ORK_PROOF_DIR as the record of what shipped. Each page is asserted only far
// enough to know it has rendered what the screen shows, and at 320 px that
// nothing runs past the right edge outside a frame that scrolls (WCAG 1.4.10
// reflow, plan §2.14); the stage specs (run-sheet, authoring-sheets,
// operate-sheets, shell) hold the behaviour.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18991';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14991';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-design-screens-${STAMP}`;
const BUCKET = ownBucket('design-screens');
const SAGA = `screens-saga-${STAMP}`;
const QUIET = `screens-quiet-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'design-screens')
	: path.join(uiDir, 'test-results', 'design-screens');
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

// The chosen sketch's own drawing: a charge that fails, a ship that never
// starts, a failure handler that runs, and a report that joins them.
function saga(version: string): string {
	const ok = ['echo', 'ok'];
	return [
		header(SAGA, version, `Checkout saga, ${version}.`),
		step('validate', [], ok),
		step('reserve-stock', ['validate'], ok),
		step('charge', ['reserve-stock'], ['sh', '-c', 'echo card declined >&2; exit 3']),
		step('ship', ['charge'], ok),
		step('refund', ['charge'], ok, '    join: any_failed'),
		step('report', ['ship', 'refund'], ok, '    join: all_done'),
		''
	].join('\n');
}

const quiet = () => [header(QUIET, '1.0.0', 'Succeeds.'), step('only', [], ['echo', 'ok']), ''].join('\n');

async function submit(name: string, version = ''): Promise<string> {
	const body: Record<string, unknown> = { orkflowName: name };
	if (version) body.orkflowVersion = version;
	return (await rpc<{ run: { id: string } }>('RunService/SubmitRun', body)).run.id;
}

async function terminal(runId: string): Promise<string> {
	let s = '';
	await waitFor(`run ${runId} finished`, async () => {
		s = (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status ?? '';
		return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(s);
	}, 240_000);
	return s;
}

// The listing can show a just-finished run as RUNNING for a moment
// (docs/UI_CLI_PARITY.md, "Run browsing"); the register's screen waits for it
// to list both runs finished.
async function listedFinished(ids: string[]): Promise<void> {
	await waitFor('the runs are listed finished', async () => {
		const answer = await rpc<{ runs?: { id: string; status?: string }[] }>('RunService/ListRuns', { pageSize: 50 });
		return ids.every((id) => (answer.runs ?? []).some((r) => r.id === id && r.status !== 'STATUS_RUNNING'));
	}, 120_000);
}

let failedRun = '';

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
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
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('design-screens') });
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
	failedRun = await submit(SAGA);
	expect(await terminal(failedRun)).toBe('STATUS_FAILED');
	// A later version, so the version tables have a CURRENT that is not the
	// failed run's.
	await rpc('OrkflowService/ApplyOrkflow', { yaml: saga('0.2.0') });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: quiet() });
	const quietRun = await submit(QUIET);
	expect(await terminal(quietRun)).toBe('STATUS_SUCCEEDED');
	await listedFinished([failedRun, quietRun]);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`saga ${SAGA} 0.1.0 run ${failedRun} FAILED, then 0.2.0 applied; ${QUIET} run ${quietRun} SUCCEEDED`);
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

/** One key page: its name in the file, where it lives, and how to know it has rendered what the screen shows. */
type Screen = { name: string; url: () => string; ready: (page: Page) => Promise<void> };

const SCREENS: Screen[] = [
	{
		name: 'runs',
		url: () => `${UI_ORIGIN}/runs?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId(`run-row-${failedRun}`)).toBeVisible();
		}
	},
	{
		name: 'run-dag',
		url: () => `${UI_ORIGIN}/runs/${failedRun}?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('run-view-dag')).toHaveAttribute('aria-selected', 'true');
			await expect(page.getByTestId('run-dag')).toBeVisible();
			await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
		}
	},
	{
		name: 'run-map',
		url: () => `${UI_ORIGIN}/runs/${failedRun}?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
			await page.getByTestId('run-view-map').click();
			await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
		}
	},
	{
		name: 'orkflow',
		url: () => `${UI_ORIGIN}/orkflows/${SAGA}?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('orkflow-dag')).toBeVisible();
			await expect(page.getByTestId('orkflow-sheet-versions')).toBeVisible();
		}
	},
	{
		name: 'apply',
		url: () => `${UI_ORIGIN}/apply?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('apply-examples')).toBeVisible();
		}
	},
	{
		name: 'dashboard',
		url: () => `${UI_ORIGIN}/?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('dashboard-counts')).toBeVisible();
			await expect(page.getByTestId('stat-succeeded')).not.toHaveText('0', { timeout: 60_000 });
		}
	},
	{
		name: 'admin-namespaces',
		url: () => `${UI_ORIGIN}/admin/namespaces?ns=default`,
		ready: async (page) => {
			await expect(page.getByTestId('namespace-list')).toBeVisible();
		}
	}
];

const VIEWPORTS = [
	{ label: '1440', width: 1440, height: 900 },
	{ label: '390', width: 390, height: 844 },
	{ label: '320', width: 320, height: 640 }
];

// What runs past the right edge, named, so a failure says where: only what
// no scrolling frame of its own (a register, the DAG, the strip) contains.
async function pastRightEdge(page: Page): Promise<string[]> {
	return page.evaluate(() => {
		if (document.documentElement.scrollWidth <= window.innerWidth) return [];
		const contained = (el: Element): boolean => {
			for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
				if (['auto', 'scroll', 'hidden', 'clip'].includes(getComputedStyle(p).overflowX)) return true;
			}
			return false;
		};
		const loose = [...document.querySelectorAll('body *')].filter(
			(el) => el.getBoundingClientRect().right > window.innerWidth + 0.5 && !contained(el)
		);
		return [
			`document ${document.documentElement.scrollWidth} px wide`,
			...loose
				.slice(0, 8)
				.map(
					(el) =>
						`${el.tagName.toLowerCase()}${el.getAttribute('data-testid') ? `[${el.getAttribute('data-testid')}]` : ''}.${el.className.toString().slice(0, 60)} → ${Math.round(el.getBoundingClientRect().right)}`
				)
		];
	});
}

test('screens: the key pages in light and dark, at 1440, 390 and 320 px', async ({ page }) => {
	test.setTimeout(10 * 60_000);
	const written: string[] = [];
	const reflowed: string[] = [];
	for (const theme of ['light', 'dark'] as const) {
		await page.setViewportSize({ width: 1440, height: 900 });
		await page.goto(`${UI_ORIGIN}/?ns=default`);
		await page.getByTestId(`theme-${theme}`).click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
		for (const viewport of VIEWPORTS) {
			await page.setViewportSize({ width: viewport.width, height: viewport.height });
			for (const screen of SCREENS) {
				await page.goto(screen.url());
				await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
				await expect(page.getByTestId('title-block')).toBeVisible();
				await screen.ready(page);
				const base = `screen-${screen.name}-${theme}-${viewport.label}`;
				if (viewport.width === 320) {
					expect(await pastRightEdge(page), `${base}: nothing runs past the right edge at 320 px`).toEqual([]);
					reflowed.push(base);
				}
				await page.screenshot({ path: out(`${base}-first.png`) });
				await page.screenshot({ path: out(`${base}.png`), fullPage: true });
				written.push(base);
			}
		}
	}
	note(`${written.length} screens, each its first viewport (-first.png) and the full page (.png): ${written.join(', ')}`);
	note(`reflow at 320 px: ${reflowed.length} pages, nothing past the right edge outside a scrolling frame (${reflowed.join(', ')})`);
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(`${UI_ORIGIN}/?ns=default`);
	await page.getByTestId('theme-system').click();
});

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

/** How the run page's DAG sits in its frame: its scale, and whether the frame scrolls. */
async function dagFit(page: Page) {
	return page.evaluate(() => {
		const svg = document.querySelector('[data-testid="run-dag"]') as SVGSVGElement;
		const panel = svg.closest('[role="tabpanel"]') as HTMLElement;
		const node = document.querySelector('[data-testid="run-dag-node-charge"]')!.getBoundingClientRect();
		const box = panel.getBoundingClientRect();
		return {
			scale: svg.getBoundingClientRect().width / svg.viewBox.baseVal.width,
			scrolls: panel.scrollWidth > panel.clientWidth + 1,
			failedInView: node.left >= box.left - 0.5 && node.right <= box.right + 0.5
		};
	});
}

test('finish review: the DAG fits its frame, the failure speaks plainly, rows are ruled, lists start high and reflow', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default`);
	await expect(page.getByTestId('run-dag')).toBeVisible();

	// The saga's five layers are drawn whole at 1440, scaled no lower than 0.6,
	// and the frame offers no scrolling.
	const wide = await dagFit(page);
	note(`DAG at 1440: ${JSON.stringify(wide)}`);
	expect(wide.scrolls).toBe(false);
	expect(wide.scale).toBeGreaterThanOrEqual(0.6);
	expect(wide.scale).toBeLessThanOrEqual(1);
	await expect(page.getByTestId('run-dag-scroll-right')).toHaveCount(0);
	await expect(page.getByTestId('run-dag-scroll-left')).toHaveCount(0);

	// The failure leads with the task's reason and its sentence; the run's own
	// code stays beside it, and no word of the execution service's wrapper is
	// on the page.
	await expect(page.getByTestId('run-error-reason')).toHaveText('CONTAINER_EXIT_NONZERO');
	await expect(page.getByTestId('run-error-code')).toHaveText('RUN_FAILED');
	await expect(page.getByTestId('run-error-message')).toHaveText('task "charge": the container exited 3');
	await expect(page.getByTestId('task-charge-error-code')).toHaveText('CONTAINER_EXIT_NONZERO');
	await expect(page.getByTestId('task-charge-error-message')).toHaveText('task "charge": the container exited 3');
	for (const id of ['run-error', 'task-charge-error']) {
		for (const word of ['activity error', 'scheduledEventID', 'startedEventID', 'identity:', 'ExecuteTask', '(type:']) {
			await expect(page.getByTestId(id)).not.toContainText(word);
		}
	}

	// A task is a hairline-ruled row on clean ground, not a box: its output
	// and Inspect toggles sit on its own line of facts, and its failure is one
	// tinted ground with no rule of its own.
	const rows = await page.evaluate(() => {
		const read = (id: string) => {
			const el = document.querySelector(`[data-testid="${id}"]`)!;
			const s = getComputedStyle(el);
			return { top: s.borderTopWidth, left: s.borderLeftWidth, bottom: s.borderBottomWidth, ground: s.backgroundColor };
		};
		const top = (id: string) => document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect().top;
		return {
			row: read('task-validate'),
			failure: read('task-charge-error'),
			togglesLevel: Math.abs(top('task-log-toggle-validate') - top('task-inspect-toggle-validate')),
			togglesBelowRowTop: top('task-inspect-toggle-validate') - top('task-validate'),
			rowHeight: document.querySelector('[data-testid="task-validate"]')!.getBoundingClientRect().height
		};
	});
	note(`task rows: ${JSON.stringify(rows)}`);
	expect(rows.row).toMatchObject({ top: '0px', left: '0px', bottom: '1px', ground: 'rgba(0, 0, 0, 0)' });
	expect(rows.failure).toMatchObject({ top: '0px', left: '0px', bottom: '0px' });
	expect(rows.failure.ground).toBe(await tokenColor(page, '--color-red-tint'));
	expect(rows.togglesLevel).toBeLessThanOrEqual(2);
	expect(rows.togglesBelowRowTop).toBeLessThan(64);
	await page.getByTestId('task-log-toggle-validate').click();
	await expect(page.getByTestId('task-log-toggle-validate')).toHaveAttribute('aria-expanded', 'true');
	await expect(page.getByTestId('task-log-state-validate')).toBeVisible();
	await page.getByTestId('task-log-toggle-validate').click();
	await expect(page.getByTestId('task-log-state-validate')).toHaveCount(0);

	// At 390 the drawing scrolls in its frame: the frame says so with a
	// control, and the failed task is the one in view.
	await page.setViewportSize({ width: 390, height: 844 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default`);
	await expect(page.getByTestId('run-dag')).toBeVisible();
	await expect.poll(async () => (await dagFit(page)).failedInView).toBe(true);
	const narrow = await dagFit(page);
	note(`DAG at 390: ${JSON.stringify(narrow)}`);
	expect(narrow.scrolls).toBe(true);
	expect(narrow.scale).toBeGreaterThanOrEqual(0.6 - 0.001);
	await expect(page.getByTestId('run-dag-scroll-left').or(page.getByTestId('run-dag-scroll-right')).first()).toBeVisible();
	await page.screenshot({ path: out('finish-run-dag-390.png') });

	// The registers reflow below 640 px: nothing scrolls sideways, and a run's
	// status is on screen beside its id.
	for (const [list, url, row] of [
		['runs', `${UI_ORIGIN}/runs?ns=default`, `run-row-${failedRun}`],
		['orkflows', `${UI_ORIGIN}/orkflows?ns=default`, `orkflow-row-${SAGA}`]
	] as const) {
		await page.goto(url);
		await expect(page.getByTestId(row)).toBeVisible();
		const fit = await page.evaluate((id) => {
			const tr = document.querySelector(`[data-testid="${id}"]`)!;
			const frame = tr.closest('table')!.parentElement!;
			return {
				scrolls: frame.scrollWidth > frame.clientWidth + 1,
				rightmost: Math.max(...[...tr.children].map((td) => td.getBoundingClientRect().right))
			};
		}, row);
		note(`${list} at 390: ${JSON.stringify(fit)}`);
		expect(fit.scrolls, `${list}: the register does not scroll sideways at 390`).toBe(false);
		expect(fit.rightmost).toBeLessThanOrEqual(390);
		await page.screenshot({ path: out(`finish-${list}-390.png`) });
	}

	// The count band: its sub-lines are whole, and at 390 its two ruled lines
	// both run the band's full width.
	await page.goto(`${UI_ORIGIN}/?ns=default`);
	await expect(page.getByTestId('stat-succeeded')).not.toHaveText('0', { timeout: 60_000 });
	const band = await page.evaluate(() => {
		const counts = document.querySelector('[data-testid="dashboard-counts"]')!;
		const right = counts.getBoundingClientRect().right;
		const cells = [...counts.children].map((el) => el.getBoundingClientRect());
		const rows = [...new Set(cells.map((c) => Math.round(c.top)))];
		const notes = [...counts.querySelectorAll('[data-testid$="-note"]')].map((el) => ({
			id: el.getAttribute('data-testid'),
			cut: el.scrollWidth > el.clientWidth + 1
		}));
		return { rows: rows.map((top) => Math.abs(Math.round(right - Math.max(...cells.filter((c) => Math.round(c.top) === top).map((c) => c.right))))), notes };
	});
	note(`band at 390: ${JSON.stringify(band)}`);
	// Below 400 px the four status counts are a 2 × 2 grid over the three figures.
	expect(band.rows).toEqual([0, 0, 0]);
	for (const n of band.notes) expect(n.cut, `${n.id} is whole`).toBe(false);

	// The runs register at 1280 × 720: the filter toolbar is one row, and the
	// table starts in the top half of the first viewport.
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${failedRun}`)).toBeVisible();
	const toolbar = await page.evaluate(() => {
		const top = (id: string) => Math.round(document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect().top);
		const mid = (id: string) => {
			const r = document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect();
			return Math.round(r.top + r.height / 2);
		};
		return {
			rows: new Set(['run-filter-add', 'run-filter-size', 'run-filters-apply', 'run-filters-cli-trigger'].map(mid)).size,
			table: top('run-list')
		};
	});
	note(`runs toolbar at 1280x720: ${JSON.stringify(toolbar)}`);
	expect(toolbar.rows).toBe(1);
	expect(toolbar.table).toBeLessThan(360);
	await page.screenshot({ path: out('finish-runs-1280.png') });
});
