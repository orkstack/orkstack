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
import { fullPageShot } from './uiHelpers';

// The finish review's fixes (2026-10-22), proven in the browser against a
// real engine and Orker on real Temporal and MinIO:
//
//   - a RUNNING task is a soft green box that breathes — on the DAG, the task
//     map and its stamp — and stands still under reduced motion;
//   - a failed run tells one story: the cloud and the task panel say the same
//     thing about retries, a never-started task is stamped CANCELED with
//     "never started" in every view, and no faded node is struck through;
//   - the run header is one line of facts, the body has no second title
//     block, the DAG frame fits its drawing, task rows share one timeline,
//     and each page has one primary;
//   - at 390 px the drawing starts within 220 px and the secondary actions
//     fold into More; at 320 px the count band is a 2 × 2 grid, run ids are
//     shortened in the middle and nothing runs past the right edge;
//   - Admin → Namespaces leads with its registry, and Create namespace opens
//     the form.
//
// Screens land under ORK_PROOF_DIR as the record. Isolation (AGENTS.md §11.1):
// its own Temporal namespace, bucket and ports; it ends only what it started.
const ENGINE_PORT = '18951';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14951';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-sheet-polish-${STAMP}`;
const BUCKET = ownBucket('sheet-polish');
const SAGA = `polish-saga-${STAMP}`;
const LIVE = `polish-live-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-sheet-polish')
	: path.join(uiDir, 'test-results', 'run-sheet-polish');
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
		'    timeout_seconds: 900',
		`    with: {op: exec, command: [${command.map((c) => JSON.stringify(c)).join(', ')}]}`
	].join('\n');

const header = (name: string, version: string, description: string) =>
	['apiVersion: orkstack/v1', 'kind: Orkflow', `name: ${name}`, `version: "${version}"`, `description: ${description}`, 'tasks:'].join('\n');

// The design screens' saga: a charge that fails, a ship that never starts, a
// failure handler that runs and a report that joins them.
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

// A run held RUNNING for as long as the spec looks at it: `long` sleeps, and
// `after` waits for it.
const live = () =>
	[
		header(LIVE, '1.0.0', 'Holds one task running.'),
		step('warm', [], ['echo', 'ok']),
		step('long', ['warm'], ['sleep', '600']),
		step('after', ['long'], ['echo', 'ok']),
		''
	].join('\n');

async function submit(name: string): Promise<string> {
	return (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: name })).run.id;
}

async function terminal(runId: string): Promise<string> {
	let s = '';
	await waitFor(`run ${runId} finished`, async () => {
		s = (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status ?? '';
		return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(s);
	}, 240_000);
	return s;
}

async function taskRunning(runId: string, taskId: string): Promise<void> {
	await waitFor(`task ${taskId} of ${runId} is running`, async () => {
		const answer = await rpc<{ run?: { taskRuns?: { taskId?: string; status?: string }[] } }>('RunService/GetRun', { runId });
		return (answer.run?.taskRuns ?? []).some((t) => t.taskId === taskId && t.status === 'STATUS_RUNNING');
	}, 180_000);
}

let failedRun = '';
let liveRun = '';

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('object storage is not reachable on 127.0.0.1:9000');
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
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('sheet-polish') });
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
	await rpc('OrkflowService/ApplyOrkflow', { yaml: saga('0.2.0') });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: live() });
	liveRun = await submit(LIVE);
	await taskRunning(liveRun, 'long');
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`saga ${SAGA} 0.1.0 run ${failedRun} FAILED; ${LIVE} run ${liveRun} holds task long RUNNING`);
});

test.afterAll(async () => {
	if (liveRun) {
		try {
			await rpc('RunService/CancelRun', { runId: liveRun, reason: 'run-sheet-polish e2e: the running state was recorded' });
			note(`canceled ${liveRun}: ${await terminal(liveRun)}`);
		} catch (err) {
			note(`cancel of ${liveRun} failed (not fatal): ${err}`);
		}
	}
	await killStarted();
	try {
		removeBucket(BUCKET, note);
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
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

async function setTheme(page: Page, theme: 'light' | 'dark' | 'system'): Promise<void> {
	await page.getByTestId(`theme-${theme}`).click();
	if (theme !== 'system') await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
}

/** What a running DAG node draws: its outline, its ground and the ground's animation. */
async function runningNode(page: Page, id: string) {
	return page.evaluate((testid) => {
		const rect = document.querySelector(`[data-testid="${testid}"] rect.dag-outline`) as SVGRectElement;
		const s = getComputedStyle(rect);
		return { stroke: s.stroke, fill: s.fill, animation: s.animationName, duration: s.animationDuration };
	}, `run-dag-node-${id}`);
}

/** One pixel of the task map's canvas, one CSS pixel inside the cell at `index` (top-left). */
async function mapPixel(page: Page, index: number): Promise<number[]> {
	return page.evaluate((i) => {
		const map = document.querySelector('[data-testid="task-map"]') as HTMLElement;
		const canvas = document.querySelector('[data-testid="task-map-canvas"]') as HTMLCanvasElement;
		const pitch = Number(map.dataset.pitch);
		const columns = Number(map.dataset.columns);
		const dpr = canvas.width / canvas.getBoundingClientRect().width;
		const x = ((i % columns) * pitch + 1) * dpr;
		const y = (Math.floor(i / columns) * pitch + 1) * dpr;
		return [...canvas.getContext('2d')!.getImageData(Math.floor(x), Math.floor(y), 1, 1).data];
	}, index);
}

const rgb = (css: string) => (css.match(/\d+(\.\d+)?/g) ?? []).slice(0, 3).map(Number);
const near = (a: number[], b: number[], tolerance = 3) => a.every((v, i) => Math.abs(v - b[i]) <= tolerance);

test('running: a RUNNING task is a soft green box that breathes, and stands still under reduced motion', async ({ page }) => {
	test.setTimeout(6 * 60_000);
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default&view=dag`);
	await setTheme(page, 'light');
	const node = page.getByTestId('run-dag-node-long');
	await expect(node).toHaveAttribute('data-status', 'RUNNING', { timeout: 60_000 });

	// The DAG: a green outline round a ground that fades between the green
	// tint and paper, every 2.4 s.
	const green = await tokenColor(page, '--color-green');
	const first = await runningNode(page, 'long');
	note(`DAG running node (light): ${JSON.stringify(first)}`);
	expect(first.stroke).toBe(green);
	expect(first.animation).toBe('live-fill');
	expect(first.duration).toBe('2.4s');
	await expect.poll(async () => (await runningNode(page, 'long')).fill, { timeout: 5_000 }).not.toBe(first.fill);
	// A finished node does not breathe.
	expect((await runningNode(page, 'warm')).animation).toBe('none');
	// Its stamp's dot is the same green, and so is the LIVE badge's.
	const dot = await page.getByTestId('task-long-status').locator('.mark-dot').evaluate((el) => getComputedStyle(el).backgroundColor);
	expect(dot).toBe(green);
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE');

	for (const theme of ['light', 'dark'] as const) {
		// The appearance switch is in the top bar from 1024 px; below that it
		// is in the menu drawer.
		await page.setViewportSize({ width: 1440, height: 900 });
		await setTheme(page, theme);
		for (const viewport of [
			{ label: '1440', width: 1440, height: 900 },
			{ label: '390', width: 390, height: 844 }
		]) {
			await page.setViewportSize({ width: viewport.width, height: viewport.height });
			await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default&view=dag&task=long`);
			await expect(page.getByTestId('run-dag-node-long')).toHaveAttribute('data-status', 'RUNNING');
			expect((await runningNode(page, 'long')).stroke).toBe(await tokenColor(page, '--color-green'));
			await page.screenshot({ path: out(`running-dag-${theme}-${viewport.label}.png`) });
			await page.getByTestId('run-view-map').click();
			await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
			await expect(page.getByTestId('task-map-legend-running')).toBeVisible();
			await page.screenshot({ path: out(`running-map-${theme}-${viewport.label}.png`) });
		}
	}

	// The task map: the running cell's mark fades and returns.
	await page.setViewportSize({ width: 1440, height: 900 });
	await setTheme(page, 'light');
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default&view=map`);
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	// DAG order: warm, long, after — `long` is the second cell.
	const sample = await mapPixel(page, 1);
	await expect.poll(async () => near(await mapPixel(page, 1), sample, 6), { timeout: 5_000 }).toBe(false);
	note(`task map: the running cell moved from ${sample} within 5 s`);

	// Reduced motion: nothing breathes. The node rests on the green tint and
	// the map's cell is drawn whole, in the green.
	await page.emulateMedia({ reducedMotion: 'reduce' });
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default&view=dag&task=long`);
	await expect(page.getByTestId('run-dag-node-long')).toHaveAttribute('data-status', 'RUNNING');
	const still = await runningNode(page, 'long');
	note(`DAG running node (reduced motion): ${JSON.stringify(still)}`);
	expect(still.animation).toBe('none');
	expect(rgb(still.fill)).toEqual(rgb(await tokenColor(page, '--color-green-tint')));
	await page.screenshot({ path: out('running-dag-light-1440-reduced-motion.png') });
	await page.getByTestId('run-view-map').click();
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	expect(await page.evaluate(() => matchMedia('(prefers-reduced-motion: reduce)').matches)).toBe(true);
	const whole = rgb(green).join(',');
	const cell = async () => (await mapPixel(page, 1)).slice(0, 3).join(',');
	await expect.poll(cell, { timeout: 5_000 }).toBe(whole);
	// And it stays whole: a page making progress (the clock ticks) has not
	// repainted the cell in the meantime.
	const elapsed = await page.getByTestId('run-elapsed').innerText();
	await expect(page.getByTestId('run-elapsed')).not.toHaveText(elapsed, { timeout: 5_000 });
	expect(await cell()).toBe(whole);
	await page.screenshot({ path: out('running-map-light-1440-reduced-motion.png') });
	await page.emulateMedia({ reducedMotion: null });
	await page.setViewportSize({ width: 1440, height: 900 });
	await setTheme(page, 'system');
});

test('a failed run tells one story: retries, never started, no strike, one facts line', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default&view=dag`);
	await expect(page.getByTestId('run-dag')).toBeVisible();
	await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');

	// Retries: the cloud and the task's panel say the same, and neither says a
	// bare "retryable".
	// `charge` declares no retry policy, so it was tried once and its budget
	// ran out; the run's own wrapper (never retryable itself) is not quoted.
	await expect(page.getByTestId('task-charge-error-retry-state')).toHaveText('retries exhausted');
	await expect(page.getByTestId('run-error-retry-state')).toHaveText('retries exhausted');
	await expect(page.locator('[data-testid$="-retryable"]')).toHaveCount(0);
	await expect(page.getByTestId('run-error')).not.toContainText('not retryable');
	note('retries: cloud and panel both say "retries exhausted"');

	// Never started: CANCELED, "never started", in the row, on the node and in
	// the map's key — and no faded node is struck through.
	await expect(page.getByTestId('task-ship-status')).toHaveText('CANCELED');
	await expect(page.getByTestId('task-ship-status-note')).toHaveText('never started');
	await expect(page.getByTestId('run-dag-node-ship')).toContainText('never started');
	await expect(page.locator('[data-testid="run-dag"] line')).toHaveCount(0);

	// The header: one line of facts; origin is words, not a stamp; a finished
	// run says nothing about being live.
	const facts = page.getByTestId('run-origin');
	await expect(facts).toContainText('origin submit');
	await expect(page.getByTestId('run-origin-kind')).not.toHaveClass(/stamp/);
	await expect(page.getByTestId('live-badge')).toHaveCount(0);
	expect((await facts.boundingBox())!.height).toBeLessThan(40);

	// The body: Timing is a register of the aside, not a band across the page.
	await expect(page.locator('aside').getByTestId('run-elapsed')).toBeVisible();
	await expect(page.locator('aside').getByTestId('task-summary')).toHaveText(/succeeded/);
	// An empty input and no aggregated output are one NOTE.
	await expect(page.getByTestId('run-values-note')).toContainText('no input values');
	await expect(page.getByTestId('run-values-note')).toContainText('without aggregated output values');
	await expect(page.locator('pre[data-testid="run-input"], pre[data-testid="run-output"]')).toHaveCount(0);

	// The DAG frame fits its drawing: content + the 32 px patch and 16 px
	// padding, never more than the drawing measure.
	const fit = await page.evaluate(() => {
		const frame = document.querySelector('[data-testid="run-drawing"]')!.getBoundingClientRect();
		const svg = document.querySelector('[data-testid="run-dag"]')!.getBoundingClientRect();
		return { frame: frame.height, svg: svg.height };
	});
	note(`DAG frame ${fit.frame.toFixed(1)} px round a ${fit.svg.toFixed(1)} px drawing`);
	expect(Math.abs(fit.frame - (fit.svg + 48 + 3))).toBeLessThanOrEqual(4);

	// Rows share one timeline: each task's mark starts where its run had got
	// to, so a later task starts further right on the same track. (The rows
	// are brought into view first: a card off screen is not laid out.)
	await page.getByTestId('task-charge').scrollIntoViewIfNeeded();
	await expect(page.getByTestId('task-validate-run-segment')).toBeVisible();
	const lefts = await page.evaluate(() =>
		['validate', 'reserve-stock', 'charge'].map((id) => {
			const track = document.querySelector(`[data-testid="task-${id}-run-segment"]`)!.parentElement!.getBoundingClientRect();
			const seg = document.querySelector(`[data-testid="task-${id}-run-segment"]`)!.getBoundingClientRect();
			return { track: Math.round(track.left), width: Math.round(track.width), left: seg.left, height: seg.height };
		})
	);
	note(`timeline: ${JSON.stringify(lefts)}`);
	expect(new Set(lefts.map((l) => `${l.track}/${l.width}`)).size).toBe(1);
	expect(lefts[0].left).toBeLessThan(lefts[1].left);
	expect(lefts[1].left).toBeLessThan(lefts[2].left);
	expect(lefts[0].height).toBe(4);

	// A short value is said on its row in the mono; the CLI buttons say which
	// command they hold.
	await expect(page.getByTestId('task-values-validate')).toHaveText('{"exitCode": 0}');
	expect(await page.getByTestId('task-values-validate').evaluate((el) => el.tagName)).toBe('CODE');
	await expect(page.getByTestId('run-task-validate-logs-cli-trigger')).toHaveText('run logs');
	await expect(page.getByTestId('run-task-validate-timing-cli-trigger')).toHaveText('run timing');
	await expect(page.getByTestId('run-reproduce-get')).toHaveText(`orkctl --namespace default run get ${failedRun} --json`);

	// The accents: a node's tags are ink, This run's row is sunken paper, and
	// the page has one primary.
	expect(await page.getByTestId('run-dag-node-report-join').evaluate((el) => getComputedStyle(el).fill)).toBe(
		await tokenColor(page, '--color-ink-secondary')
	);
	expect(await page.getByTestId('run-version-0.1.0').evaluate((el) => getComputedStyle(el).backgroundColor)).toBe(
		await page.evaluate(() => {
			const probe = document.createElement('span');
			probe.style.backgroundColor = 'var(--color-paper-sunken)';
			document.body.appendChild(probe);
			const c = getComputedStyle(probe).backgroundColor;
			probe.remove();
			return c;
		})
	);
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	// The body starts under the drawing, beside the aside, not after it.
	const body = await page.evaluate(() => ({
		tasks: document.querySelector('[data-testid="task-list"]')!.getBoundingClientRect().top,
		asideEnd: document.querySelector('aside[aria-label="About this run"]')!.getBoundingClientRect().bottom
	}));
	note(`body at 1440: ${JSON.stringify(body)}`);
	expect(body.tasks).toBeLessThan(body.asideEnd);
	await fullPageShot(page, out('failed-run-1440.png'));

	// The task map's key says it too.
	await page.getByTestId('run-view-map').click();
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	await expect(page.getByTestId('task-map-legend-canceled-note')).toHaveText('· never started');
	// The map's frame fits its six cells too: their rows, the 32 px patch,
	// the 16 px padding and the frame's rules.
	const mapFit = await page.evaluate(() => {
		const frame = document.querySelector('[data-testid="run-drawing"]')!.getBoundingClientRect().height;
		const map = document.querySelector('[data-testid="task-map"]') as HTMLElement;
		const canvas = document.querySelector('[data-testid="task-map-canvas"]')!.getBoundingClientRect().height;
		return { frame, canvas, pitch: Number(map.dataset.pitch) };
	});
	note(`map frame at 1440: ${JSON.stringify(mapFit)}`);
	expect(mapFit.pitch).toBe(16);
	// (A single row is given at least one line of text's height, 20 px.)
	expect(mapFit.frame - (Math.max(mapFit.canvas, 20) + 51)).toBeGreaterThanOrEqual(-1);
	expect(mapFit.frame - (Math.max(mapFit.canvas, 20) + 51)).toBeLessThanOrEqual(1);
	await page.screenshot({ path: out('failed-run-map-1440.png') });

	// One primary on the Runs register too: Apply filters is a plain button.
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${failedRun}`)).toBeVisible();
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	await expect(page.getByTestId('run-filters-apply')).not.toHaveClass(/btn-primary/);
});

test('at 390 px the drawing starts within 220 px, and the secondary actions fold into More', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default&view=dag`);
	await expect(page.getByTestId('run-dag')).toBeVisible();
	const top = await page.getByTestId('run-drawing').evaluate((el) => el.getBoundingClientRect().top + scrollY);
	note(`drawing top at 390: ${top.toFixed(1)} px`);
	expect(top).toBeLessThanOrEqual(220);
	// The heading is drawn short; the whole id stays the heading.
	await expect(page.getByTestId('run-id')).toHaveText(failedRun);
	await expect(page.getByText(`ork-…${failedRun.slice(-6)}`, { exact: true })).toBeVisible();
	for (const id of ['run-evidence', 'compare-run', 'export-run-json', 'events-toggle']) await expect(page.getByTestId(id)).toBeHidden();
	// On a phone the saga's DAG reads top to bottom (the Direction Rule), in
	// a frame tall enough for all of it — or, where it is not, one that says
	// there is more below.
	await expect(page.getByTestId('run-dag')).toHaveAttribute('data-direction', 'TB');
	const phone = await page.evaluate(() => {
		const panel = document.querySelector('[data-testid="run-dag"]')!.closest('[role="tabpanel"]')!.getBoundingClientRect();
		const boxes = [...document.querySelectorAll('[data-testid="run-dag"] g.dag-node rect')].map((r) => r.getBoundingClientRect());
		return { all: boxes.length, inside: boxes.filter((b) => b.top >= panel.top - 0.5 && b.bottom <= panel.bottom + 0.5).length };
	});
	note(`DAG at 390: ${phone.inside} of ${phone.all} boxes inside the frame`);
	if (phone.inside < phone.all) await expect(page.getByTestId('run-dag-scroll-down')).toBeVisible();
	else expect(phone.inside).toBe(6);
	// The caption keeps its count whatever it shortens.
	await expect(page.getByTestId('run-drawing')).toContainText('· 6 tasks');
	await page.screenshot({ path: out('failed-run-390-first.png') });
	// Read left to right, it scrolls sideways; the scroll controls sit on the
	// frame's top band, never over a node.
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default&view=dag&dag=lr`);
	await expect(page.getByTestId('run-dag')).toHaveAttribute('data-direction', 'LR');
	const band = await page.evaluate(() => {
		const panel = document.querySelector('[data-testid="run-dag"]')!.closest('[role="tabpanel"]')!.getBoundingClientRect();
		const buttons = ['run-dag-scroll-left', 'run-dag-scroll-right'].map((id) => document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect());
		return { panelTop: panel.top, bottoms: buttons.map((b) => b.bottom) };
	});
	for (const bottom of band.bottoms) expect(bottom).toBeLessThanOrEqual(band.panelTop + 0.5);
	await page.screenshot({ path: out('failed-run-390-lr.png') });

	await page.getByTestId('run-more').click();
	for (const id of ['run-more-evidence', 'run-more-compare', 'run-more-export', 'run-more-events', 'run-more-stability']) {
		await expect(page.getByTestId(id)).toBeVisible();
	}
	// The list is on screen, whole, and nothing of the frame is over it.
	const list = (await page.getByTestId('run-more-list').boundingBox())!;
	note(`More list at 390: ${JSON.stringify(list)}`);
	expect(list.x).toBeGreaterThanOrEqual(0);
	expect(list.x + list.width).toBeLessThanOrEqual(390);
	const topmost = await page.evaluate(() => {
		const r = document.querySelector('[data-testid="run-more-compare"]')!.getBoundingClientRect();
		return document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2)?.closest('[data-testid]')?.getAttribute('data-testid');
	});
	expect(topmost).toBe('run-more-compare');
	await page.screenshot({ path: out('failed-run-390-more.png') });
	await page.getByTestId('run-more-events').click();
	await expect(page.getByTestId('run-events')).toBeVisible();
	await expect(page.getByTestId('run-more-list')).toHaveCount(0);
});

// What runs past the right edge, outside any frame that scrolls of its own.
async function pastRightEdge(page: Page): Promise<string[]> {
	return page.evaluate(() => {
		if (document.documentElement.scrollWidth <= window.innerWidth) return [];
		return [`document ${document.documentElement.scrollWidth} px wide`];
	});
}

test('at 320 px: the count band is 2 × 2, run ids shorten in the middle, the title block wraps', async ({ page }) => {
	await page.setViewportSize({ width: 320, height: 640 });
	await page.goto(`${UI_ORIGIN}/?ns=default`);
	await expect(page.getByTestId('dashboard-counts')).toBeVisible();
	const band = await page.evaluate(() => {
		const counts = document.querySelector('[data-testid="dashboard-counts"]')!;
		const cells = [...counts.children].map((el) => el.getBoundingClientRect());
		const labels = [...counts.querySelectorAll('.caps > span:last-child')].map((el) => ({
			text: el.textContent,
			lines: el.getClientRects().length
		}));
		const mid = (id: string) => {
			const r = document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect();
			return r.top + r.height / 2;
		};
		return {
			rows: [...new Set(cells.map((c) => Math.round(c.top)))].length,
			firstRow: cells.filter((c) => Math.round(c.top) === Math.round(cells[0].top)).length,
			labels,
			cliOffWindowRow: Math.abs(mid('dashboard-cli-trigger') - mid('dashboard-window-15m'))
		};
	});
	note(`band at 320: ${JSON.stringify(band)}`);
	expect(band.rows).toBe(3);
	expect(band.firstRow).toBe(2);
	for (const l of band.labels) expect(l.lines, `${l.text} is one line`).toBe(1);
	expect(band.cliOffWindowRow).toBeLessThanOrEqual(4);
	// The stacked failures register says what its figures count, in words
	// apart from the figures.
	const finished = (await page.getByTestId(`failure-${SAGA}-finished`).innerText()).replace(/\u00a0/g, ' ');
	expect(finished).toBe('of 1 finished');
	expect((await page.getByTestId(`failure-${SAGA}-failed`).innerText()).replace(/\u00a0/g, ' ')).toBe('1 failed');
	expect(await pastRightEdge(page)).toEqual([]);
	await fullPageShot(page, out('dashboard-320.png'));

	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const row = page.getByTestId(`run-row-${failedRun}`);
	await expect(row).toBeVisible();
	await expect(row.getByText(`ork-…${failedRun.slice(-6)}`, { exact: true })).toBeVisible();
	await expect(row.getByRole('link', { name: failedRun })).toBeVisible();
	expect(await pastRightEdge(page)).toEqual([]);
	await page.screenshot({ path: out('runs-320.png') });

	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default`);
	await expect(page.getByTestId('run-dag')).toBeVisible();
	const cell = await page.getByTestId('title-block-orkflow').locator('dd').evaluate((el) => ({
		cut: el.scrollWidth > el.clientWidth + 1 || el.scrollHeight > el.clientHeight + 1,
		lines: Math.round(el.getBoundingClientRect().height / parseFloat(getComputedStyle(el).lineHeight))
	}));
	note(`title block ORKFLOW at 320: ${JSON.stringify(cell)}`);
	expect(cell.cut).toBe(false);
	expect(cell.lines).toBeLessThanOrEqual(2);
	expect(await pastRightEdge(page)).toEqual([]);
	await page.screenshot({ path: out('run-320-first.png') });

	// Admin → Namespaces stacks its register: the name whole on its own line,
	// each fact under its words, the actions on the row's last line.
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	const nsRow = page.getByTestId('namespace-row-default');
	await expect(nsRow).toBeVisible();
	const stacked = await nsRow.evaluate((tr) => {
		const name = tr.querySelector('td')!;
		const edit = tr.querySelector('[data-testid="namespace-edit-default"]')!.getBoundingClientRect();
		return { nameCut: name.scrollWidth > name.clientWidth + 1, editRight: edit.right, labelled: tr.querySelectorAll('td[data-label]').length };
	});
	expect(stacked.nameCut).toBe(false);
	expect(stacked.editRight).toBeLessThanOrEqual(320);
	expect(stacked.labelled).toBe(13);
	expect(await pastRightEdge(page)).toEqual([]);
	await fullPageShot(page, out('admin-namespaces-320.png'));
});

test('Admin → Namespaces leads with its registry, and Create namespace opens the form', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('namespace-list')).toBeVisible();
	await expect(page.getByTestId('ns-create-name')).toHaveCount(0);
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	await expect(page.getByTestId('ns-create-open')).toHaveClass(/btn-primary/);
	await expect(page.getByTestId('namespaces-filters-apply')).not.toHaveClass(/btn-primary/);
	// The row's actions are in view at 1440: the register scrolls under them.
	const edit = await page.getByTestId('namespace-edit-default').evaluate((el) => {
		const r = el.getBoundingClientRect();
		const frame = el.closest('table')!.parentElement!.getBoundingClientRect();
		return { right: r.right, frameRight: frame.right, left: r.left, frameLeft: frame.left };
	});
	expect(edit.right).toBeLessThanOrEqual(edit.frameRight + 0.5);
	expect(edit.left).toBeGreaterThanOrEqual(edit.frameLeft);
	await page.screenshot({ path: out('admin-namespaces-1440.png') });

	await page.getByTestId('ns-create-open').click();
	await expect(page.getByTestId('ns-create-open')).toHaveAttribute('aria-expanded', 'true');
	await expect(page.getByTestId('ns-create-name')).toBeVisible();
	// Hints, not placeholders that look like values.
	await expect(page.locator('#ns-create-form input[placeholder], #ns-create-form textarea[placeholder]')).toHaveCount(0);
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	await expect(page.getByTestId('ns-create-submit')).toHaveClass(/btn-primary/);
	await fullPageShot(page, out('admin-namespaces-create-1440.png'));
});
