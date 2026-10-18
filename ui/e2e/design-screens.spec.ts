import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
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
const BUCKET = `orkstack-design-screens-${STAMP}`;
const SAGA = `screens-saga-${STAMP}`;
const QUIET = `screens-quiet-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'design-screens')
	: path.join(uiDir, 'test-results', 'design-screens');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

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
	execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('ensure') });
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
		execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('cleanup') });
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
