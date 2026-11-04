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
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { fullPageShot, inspectTask, tasksMore } from './uiHelpers';

// The finish review's fixes (2026-10-22), proven in the browser against a
// real engine and Orker on real Temporal and MinIO:
//
//   - a RUNNING task is drawn in the running green — on the DAG, the task map
//     and its stamp's dot — the dot and the map's cell move, and both stand
//     still under reduced motion;
//   - a failed run tells one story: the cloud and the task panel say the same
//     thing about retries, a never-started task is stamped CANCELED with
//     "never started" in every view, and no faded node is struck through;
//   - the run header is one strip of facts, the body has no second title
//     block, the DAG frame fits its drawing, task rows share one timeline,
//     and each page has one primary;
//   - at 390 px the drawing starts on the first screen and the secondary
//     actions wrap inside it (there is no More menu); at 320 px the count band
//     is a 2 × 2 grid, run ids are shortened in the middle and nothing runs
//     past the right edge;
//   - Admin → Namespaces leads with its registry, and Create namespace opens
//     the form.
//
// Screens land under ORK_PROOF_DIR as the record. Isolation (AGENTS.md §11.1):
// its own Temporal namespace, bucket and ports; it ends only what it started.
const ENGINE_PORT = '18951';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;
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
	if (await tcpOpen(Number(ENGINE_PORT))) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		...uiEnv()
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('sheet-polish') });
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
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
		const x0 = Number(map.dataset.x0 ?? 0);
		// A small run's map is drawn stage by stage: where this cell's stage starts and stands.
		const stages = (map.dataset.rows ?? '0:0').split(',').map((s) => s.split(':').map(Number));
		const [start, top] = stages.filter(([first]) => first <= i).at(-1)!;
		const local = i - start;
		const dpr = canvas.width / canvas.getBoundingClientRect().width;
		// The middle of the cell: a running cell is a circle, so a corner is the card behind it.
		const x = (x0 + (local % columns) * pitch + (pitch - 3) / 2) * dpr;
		const y = (top + Math.floor(local / columns) * pitch + (pitch - 3) / 2) * dpr;
		return [...canvas.getContext('2d')!.getImageData(Math.floor(x), Math.floor(y), 1, 1).data];
	}, index);
}

const rgb = (css: string) => (css.match(/\d+(\.\d+)?/g) ?? []).slice(0, 3).map(Number);
const near = (a: number[], b: number[], tolerance = 3) => a.every((v, i) => Math.abs(v - b[i]) <= tolerance);

test('running: a RUNNING task is drawn in the running green, its dot moves, its map cell is whole, and all stand still under reduced motion', async ({ page }) => {
	test.setTimeout(6 * 60_000);
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await setTheme(page, 'light');
	const node = page.getByTestId('run-dag-node-long');
	await expect(node).toHaveAttribute('data-status', 'RUNNING', { timeout: 60_000 });

	// The DAG: a green outline round the node. (Harbour's node no longer
	// breathes its ground; the motion moved to the stamp's dot below.)
	const green = await tokenColor(page, '--color-green');
	// The page selects the task it follows, and a selected node's outline is the
	// selection's: let it go to read the running colour.
	await page.getByTestId('run-dag-clear').click();
	// (The outline's colour eases, so it is read until it has.)
	await expect.poll(async () => (await runningNode(page, 'long')).stroke).toBe(green);
	const first = await runningNode(page, 'long');
	note(`DAG running node (light): ${JSON.stringify(first)}`);
	// A finished node is not drawn in the running green.
	expect((await runningNode(page, 'warm')).stroke).not.toBe(green);
	// Its stamp's dot is the same green and pulses (live-dot).
	const dotOf = () =>
		page.getByTestId('task-long-status').locator('.mark-dot').evaluate((el) => ({ color: getComputedStyle(el).backgroundColor, animation: getComputedStyle(el).animationName }));
	const dot = await dotOf();
	expect(dot.color).toBe(green);
	expect(dot.animation).toBe('live-dot');
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE');

	for (const theme of ['light', 'dark'] as const) {
		// The appearance switch is in the sidebar's foot; below 900 px the
		// sidebar is the menu drawer.
		await page.setViewportSize({ width: 1440, height: 900 });
		await setTheme(page, theme);
		for (const viewport of [
			{ label: '1440', width: 1440, height: 900 },
			{ label: '390', width: 390, height: 844 }
		]) {
			await page.setViewportSize({ width: viewport.width, height: viewport.height });
			await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
			await expect(page.getByTestId('run-dag-node-long')).toHaveAttribute('data-status', 'RUNNING');
			await page.getByTestId('run-dag-clear').click();
			const running = await tokenColor(page, '--color-green');
			await expect.poll(async () => (await runningNode(page, 'long')).stroke).toBe(running);
			await page.screenshot({ path: out(`running-dag-${theme}-${viewport.label}.png`) });
			await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
			await expect(page.getByTestId('task-map-legend-running')).toBeVisible();
			await page.screenshot({ path: out(`running-map-${theme}-${viewport.label}.png`) });
		}
	}

	// The task map: the running cell is the running colour, drawn whole. Only
	// the pills and the live dot breathe, and only three times.
	await page.setViewportSize({ width: 1440, height: 900 });
	await setTheme(page, 'light');
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default`);
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	// DAG order: warm, long, after — `long` is the second cell.
	await expect.poll(async () => near((await mapPixel(page, 1)).slice(0, 3), rgb(green), 3), { timeout: 5_000 }).toBe(true);

	// Reduced motion: nothing moves. The stamp's dot has no animation and
	// the map's cell is drawn whole, in the green.
	await page.emulateMedia({ reducedMotion: 'reduce' });
	await page.goto(`${UI_ORIGIN}/runs/${liveRun}?ns=default&task=long`);
	await expect(page.getByTestId('run-dag-node-long')).toHaveAttribute('data-status', 'RUNNING');
	const still = await runningNode(page, 'long');
	note(`DAG running node (reduced motion): ${JSON.stringify(still)}`);
	expect(still.animation).toBe('none');
	expect((await dotOf()).animation).toBe('none');
	await page.screenshot({ path: out('running-dag-light-1440-reduced-motion.png') });
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	expect(await page.evaluate(() => matchMedia('(prefers-reduced-motion: reduce)').matches)).toBe(true);
	const whole = rgb(green).join(',');
	const cell = async () => (await mapPixel(page, 1)).slice(0, 3).join(',');
	await expect.poll(cell, { timeout: 5_000 }).toBe(whole);
	// And it stays whole: a page making progress (the clock ticks) has not
	// repainted the cell in the meantime.
	const elapsed = await page.getByTestId('run-lasted').innerText();
	await expect(page.getByTestId('run-lasted')).not.toHaveText(elapsed, { timeout: 5_000 });
	expect(await cell()).toBe(whole);
	await page.screenshot({ path: out('running-map-light-1440-reduced-motion.png') });
	await page.emulateMedia({ reducedMotion: null });
	await page.setViewportSize({ width: 1440, height: 900 });
	await setTheme(page, 'system');
});

test('a failed run tells one story: retries, never started, no strike, one facts line', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default`);
	await expect(page.getByTestId('run-dag')).toBeVisible();

	// Retries: the run's card and the task's panel say the same, and neither says a
	// bare "retryable".
	// `charge` declares no retry policy and was tried once, so it exhausted
	// nothing: its attempt line says "no retry policy", and neither panel claims
	// a budget ran out.
	await expect(page.getByTestId('task-charge-failure')).toContainText('no retry policy');
	await expect(page.getByTestId('task-charge-error-retry-state')).toHaveCount(0);
	await expect(page.getByTestId('run-error-retry-state')).toHaveCount(0);
	await expect(page.locator('[data-testid$="-retryable"]')).toHaveCount(0);
	await expect(page.getByTestId('run-error')).not.toContainText('not retryable');
	note('retries: a task with no retry policy says so, and no panel claims its retries ran out');

	// Never started: CANCELED, "never started", in the row, on the node and in
	// the map's key — and no faded node is struck through.
	await expect(page.getByTestId('task-ship-status')).toHaveText('Canceled');
	await expect(page.getByTestId('task-ship-status-note')).toHaveText('never started');
	await expect(page.getByTestId('run-dag-node-ship')).toContainText('never started');
	await expect(page.locator('[data-testid="run-dag"] line')).toHaveCount(0);

	// The header: one strip of facts; origin is a word, not a stamp; a finished
	// run says nothing about being live.
	const facts = page.getByTestId('run-origin');
	await expect(facts).toContainText('Submit');
	await expect(page.getByTestId('run-origin-kind')).not.toHaveClass(/stamp/);
	await expect(page.getByTestId('live-badge')).toHaveCount(0);
	expect((await facts.boundingBox())!.height).toBeLessThan(40);

	// The body: how long the run lasted is one fact of the header's strip (the
	// Timing card is gone) and the task count is the Tasks card's guide row,
	// neither of them in the aside.
	await expect(page.getByTestId('run-header').getByTestId('run-lasted')).toBeVisible();
	await expect(page.getByTestId('task-summary')).toHaveText(/succeeded/);
	await expect(page.locator('aside').getByTestId('task-summary')).toHaveCount(0);
	// A finished run that took no input and gave no output has no card for them.
	await expect(page.getByTestId('run-inputs')).toHaveCount(0);
	await expect(page.getByTestId('run-why')).toBeVisible();

	// The graph card holds its drawing: the drawing sits inside the frame, and
	// the frame is not a page of empty paper around it.
	const fit = await page.evaluate(() => {
		const frame = document.querySelector('[data-testid="run-drawing"]')!.getBoundingClientRect();
		const svg = document.querySelector('[data-testid="run-dag"]')!.getBoundingClientRect();
		return { frame: frame.height, svg: svg.height, above: svg.top - frame.top, below: frame.bottom - svg.bottom, inside: svg.left >= frame.left - 0.5 && svg.right <= frame.right + 0.5 };
	});
	note(`graph card ${fit.frame.toFixed(1)} px round a ${fit.svg.toFixed(1)} px drawing: ${JSON.stringify(fit)}`);
	expect(fit.inside).toBe(true);
	expect(fit.below).toBeGreaterThanOrEqual(-0.5);
	expect(fit.frame - fit.svg).toBeLessThanOrEqual(120);

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
	expect(lefts[0].height).toBeGreaterThan(0);

	// A short value is said in the mono in the selected task's Inspect (values
	// moved off the row); the task's commands are one popover in its Item card
	// that lists the log and the timing, and the header's lists the run's.
	await inspectTask(page, 'validate');
	await expect(page.getByTestId('task-values-validate')).toHaveText('{"exitCode": 0}');
	expect(await page.getByTestId('task-values-validate').evaluate((el) => el.tagName)).toBe('CODE');
	await page.getByTestId('run-inspect-cli-trigger').click();
	const taskCommands = page.locator('[data-testid^="run-inspect-cli-command-"]');
	await expect(taskCommands.filter({ hasText: 'run logs' }).first()).toContainText(`--task validate`);
	await expect(taskCommands.filter({ hasText: 'run timing' }).filter({ hasText: '--task validate' })).toBeVisible();
	await expect(page.locator('[data-testid^="run-inspect-cli-command-"]').filter({ hasText: 'run get' })).toHaveText(
		`orkctl --namespace default run get ${failedRun} --json`
	);
	await page.keyboard.press('Escape');

	// The accents: a node's tags are ink, This run's row is sunken paper, and
	// the page has one primary.
	// (A node's join word is now the node's <desc>, read by assistive technology, not painted.)
	await expect(page.getByTestId('run-dag-node-report-join')).toContainText('join');
	await page.getByTestId('run-versions-toggle').click();
	await expect(page.getByTestId('run-versions')).toHaveAttribute('data-loaded', 'true');
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
	await page.keyboard.press('Escape');
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	// The graph comes before the tasks, the tasks start beside the aside rather
	// than after it, and nothing runs past the right edge.
	const body = await page.evaluate(() => ({
		graphEnd: document.querySelector('[data-testid="run-drawing"]')!.getBoundingClientRect().bottom,
		tasks: document.querySelector('[data-testid="task-list"]')!.getBoundingClientRect().top,
		asideEnd: document.querySelector('aside[aria-label="Run details"]')!.getBoundingClientRect().bottom
	}));
	note(`body at 1440: ${JSON.stringify(body)}`);
	expect(body.graphEnd).toBeLessThanOrEqual(body.tasks);
	expect(body.tasks).toBeLessThan(body.asideEnd);
	expect(await pastRightEdge(page)).toEqual([]);
	await fullPageShot(page, out('failed-run-1440.png'));

	// A finished small run's map needs no key: its rows say what each cell is.
	await expect(page.getByTestId('task-map')).toHaveAttribute('data-painted', 'true');
	await expect(page.getByTestId('task-map-legend')).toHaveCount(0);
	// The map sits in its card: the canvas is inside it, at the largest pitch.
	const mapFit = await page.evaluate(() => {
		const card = document.querySelector('[data-testid="run-map-card"]')!.getBoundingClientRect();
		const map = document.querySelector('[data-testid="task-map"]') as HTMLElement;
		const canvas = document.querySelector('[data-testid="task-map-canvas"]')!.getBoundingClientRect();
		return { pitch: Number(map.dataset.pitch), inside: canvas.left >= card.left - 0.5 && canvas.right <= card.right + 0.5 && canvas.bottom <= card.bottom + 0.5 };
	});
	note(`map card at 1440: ${JSON.stringify(mapFit)}`);
	expect(mapFit.pitch).toBe(17);
	expect(mapFit.inside).toBe(true);
	await page.screenshot({ path: out('failed-run-map-1440.png') });

	// One primary on the Runs register too: Apply filters is a plain button.
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId(`run-row-${failedRun}`)).toBeVisible();
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	// Apply filters is drawn only once a filter row is drafted.
	await page.getByTestId('run-filter-add').click();
	await expect(page.getByTestId('run-filters-apply')).not.toHaveClass(/btn-primary/);
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
});

test('at 390 px the drawing starts on the first screen, and the secondary actions wrap inside it', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default`);
	await expect(page.getByTestId('run-dag')).toBeVisible();
	const top = await page.getByTestId('run-drawing').evaluate((el) => el.getBoundingClientRect().top + scrollY);
	note(`drawing top at 390: ${top.toFixed(1)} px`);
	// (There is no More menu to fold the actions into any more, so the header
	// is taller than the old 220 px: the bound is that 200 px of the frame is on the first screen.)
	expect(top).toBeLessThanOrEqual(844 - 200);
	// The id is drawn short in its plate; the whole id stays in the plate's text.
	await expect(page.getByTestId('run-id')).toContainText(failedRun);
	await expect(page.getByTestId('run-id').getByText(`ork-…${failedRun.slice(-6)}`, { exact: true })).toBeVisible();
	// On a phone the saga's DAG reads top to bottom (the Direction Rule), in
	// a frame tall enough for all of it — or, where it is not, one that says
	// there is more below.
	await expect(page.getByTestId('run-dag')).toHaveAttribute('data-direction', 'TB');
	const phone = await page.evaluate(() => {
		const panel = document.querySelector('[data-testid="run-drawing"]')!.getBoundingClientRect();
		const boxes = [...document.querySelectorAll('[data-testid="run-dag"] g.dag-node rect.dag-outline')].map((r) => r.getBoundingClientRect());
		return { all: boxes.length, inside: boxes.filter((b) => b.top >= panel.top - 0.5 && b.bottom <= panel.bottom + 0.5).length };
	});
	note(`DAG at 390: ${phone.inside} of ${phone.all} boxes inside the frame`);
	// A phone swipes its drawing, and a keyboard scrolls its region.
	if (phone.inside < phone.all) await expect(page.getByTestId('run-drawing').getByRole('region')).toHaveAttribute('tabindex', '0');
	else expect(phone.inside).toBe(6);
	// The caption keeps its count whatever it shortens.
	await expect(page.getByTestId('run-drawing-sub')).toContainText('6 tasks');
	await page.screenshot({ path: out('failed-run-390-first.png') });
	// Read left to right, it scrolls sideways; the scroll controls sit on the
	// frame's top band, never over a node.
	await page.goto(`${UI_ORIGIN}/runs/${failedRun}?ns=default&dag=lr`);
	await expect(page.getByTestId('run-dag')).toHaveAttribute('data-direction', 'LR');
	const band = await page.evaluate(() => {
		const nodes = [...document.querySelectorAll('[data-testid="run-dag"] g.dag-node')].map((n) => n.getBoundingClientRect());
		const buttons = ['run-dag-scroll-left', 'run-dag-scroll-right'].map((id) => document.querySelector(`[data-testid="${id}"]`)!.getBoundingClientRect());
		const over = buttons.filter((b) => nodes.some((n) => b.left < n.right && b.right > n.left && b.top < n.bottom && b.bottom > n.top)).length;
		return { over };
	});
	expect(band.over, 'no scroll control sits over a node').toBe(0);
	await page.screenshot({ path: out('failed-run-390-lr.png') });

	// The secondary actions are header buttons that wrap inside the screen.
	const actions = ['run-evidence', 'compare-run', 'events-toggle', 'run-tasks-more'];
	for (const id of actions) {
		await expect(page.getByTestId(id)).toBeVisible();
		const box = (await page.getByTestId(id).boundingBox())!;
		expect(box.x, `${id} starts inside the screen`).toBeGreaterThanOrEqual(0);
		expect(box.x + box.width, `${id} ends inside the screen`).toBeLessThanOrEqual(390);
	}
	expect(await pastRightEdge(page)).toEqual([]);
	// Nothing of the frame is over them.
	const topmost = await page.evaluate(() => {
		const r = document.querySelector('[data-testid="compare-run"]')!.getBoundingClientRect();
		return document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2)?.closest('[data-testid]')?.getAttribute('data-testid');
	});
	expect(topmost).toBe('compare-run');
	await page.screenshot({ path: out('failed-run-390-actions.png') });
	await page.getByTestId('events-toggle').click();
	await expect(page.getByTestId('run-events')).toBeVisible();
});

// What runs past the right edge, outside any frame that scrolls of its own.
async function pastRightEdge(page: Page): Promise<string[]> {
	return page.evaluate(() => {
		if (document.documentElement.scrollWidth <= window.innerWidth) return [];
		return [`document ${document.documentElement.scrollWidth} px wide`];
	});
}

test('at 320 px: the count band is 2 × 2, run ids shorten in the middle, the run header wraps', async ({ page }) => {
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
	const cell = await page.getByTestId('pinned-orkflow-link').evaluate((el) => ({
		cut: el.scrollWidth > el.clientWidth + 1 || el.scrollHeight > el.clientHeight + 1,
		lines: Math.round(el.getBoundingClientRect().height / parseFloat(getComputedStyle(el).lineHeight))
	}));
	note(`run header Orkflow name at 320: ${JSON.stringify(cell)}`);
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
