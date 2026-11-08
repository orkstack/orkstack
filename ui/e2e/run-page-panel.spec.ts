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
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { selectTask } from './uiHelpers';

// The run page's task panel: one element that opens under the selected row
// and holds everything about that task, so reading a task never means
// scrolling past every other task to its details.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = 18973;
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = temporalNamespace('run-page-panel');
const BUCKET = ownBucket('run-page-panel');
const ORKFLOW = `panel-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'run-page-panel') : path.join(uiDir, 'test-results', 'run-page-panel');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

const STEPS = 12;
// A long condition: its row says `condition false`, its panel says all of this.
const SKIP_CONDITION = '{{ and (ne .tasks.prepare.output.result.route "hold") .tasks.prepare.output.result.route }}';
const shell = (id: string, needs: string[], command: string, extra: string[] = []) => [
	`  - id: ${id}`,
	...(needs.length ? [`    needs: [${needs.join(', ')}]`] : []),
	...extra,
	'    uses: shell',
	'    network: none',
	'    with:',
	'      op: exec',
	`      command: [sh, -c, ${JSON.stringify(command)}]`
];
const orkflow = (version: string) =>
	[
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${ORKFLOW}`,
		`version: "${version}"`,
		'description: The task panel.',
		'inputs:',
		'  greeting:',
		'    type: string',
		'    default: hello',
		'tasks:',
		'  - id: prepare',
		'    uses: json',
		'    network: none',
		'    with:',
		'      op: query',
		'      nullInput: true',
		`      filter: '{parts: ["a", "b"], route: "hold"}'`,
		...shell('notify-owner', ['prepare'], 'echo telling the owner', [`    condition: '${SKIP_CONDITION}'`]),
		...Array.from({ length: STEPS }, (_, i) => shell(`step-${i}`, ['prepare'], `echo step ${i} says hello`)).flat(),
		'  - id: convert',
		'    needs: [prepare]',
		'    each:',
		"      over: '{{ .tasks.prepare.output.result.parts }}'",
		'      as: part',
		'      max_items: 10',
		'    uses: shell',
		'    network: none',
		'    with:',
		'      op: exec',
		'      command: [sh, -c, echo converting]'
	].join('\n') + '\n';

const SLOW = `panel-slow-${STAMP}`;
const slow =
	[
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${SLOW}`,
		'version: "0.1.0"',
		'description: One task ends while the other runs on.',
		'tasks:',
		...shell('quick', [], 'echo quick'),
		...shell('slowly', ['quick'], 'sleep 20')
	].join('\n') + '\n';

let finished = '';

async function terminal(run: string): Promise<string> {
	let status = '';
	await waitFor(
		`run ${run} ended`,
		async () => {
			const answer = await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId: run });
			status = answer.run?.status ?? '';
			return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(status);
		},
		300_000,
		500
	);
	return status;
}

const open = async (page: Page, run: string) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(`${ENGINE}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-status')).toBeVisible();
};
const top = (page: Page, testid: string) => page.getByTestId(testid).evaluate((el) => el.getBoundingClientRect().top);
// A theme change runs every control's colour transition, so what is read or
// captured before they end is neither theme's colour. Only transitions: a live
// dot's animation never ends.
const settled = (page: Page) =>
	page.evaluate(() => Promise.all(document.getAnimations().filter((a) => a instanceof CSSTransition).map((a) => a.finished.catch(() => undefined))).then(() => undefined));
const shots = async (page: Page, name: string) => {
	for (const scheme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: scheme });
		await settled(page);
		await page.screenshot({ path: out(`${name}-${scheme}.png`) });
	}
	await page.emulateMedia({ colorScheme: 'light' });
};

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('The object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(ENGINE_PORT)) throw new Error(`port ${ENGINE_PORT} already in use (stray process?)`);
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], { ...catalogEnv, ...cluster, ...storeEnv(BUCKET), ...uiEnv(), ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}` });
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('run-page-panel') });
	await waitForEngine(ENGINE);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow('0.1.0') });
	finished = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW, input: { data: { greeting: 'hi there' } } })).run.id;
	expect(await terminal(finished)).toBe('STATUS_SUCCEEDED');
	note(`${finished} finished`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET, note);
});

test('the selected task opens under its own row, and a row never moves under the pointer', async ({ page }) => {
	await open(page, finished);
	// No choice yet on a finished run: no panel and no log at the card's foot.
	await expect(page.getByTestId('task-panel')).toHaveCount(0);

	await page.getByTestId('task-deep-link-step-1').click();
	const panel = page.getByTestId('task-panel');
	await expect(panel).toHaveCount(1);
	// Directly under its row, and everything about the task is in it.
	const under = (id: string) => page.evaluate((id) => document.querySelector(`[data-testid="task-${id}"]`)!.nextElementSibling?.getAttribute('data-testid'), id);
	expect(await under('step-1')).toBe('task-panel');
	await expect(panel.getByTestId('item-card')).toBeVisible();
	await expect(panel.getByTestId('task-step-1-label')).toHaveText('Task step-1');
	await expect(panel.getByTestId('task-log-step-1')).toContainText('step 1 says hello', { timeout: 30_000 });
	await shots(page, 'panel-open');

	// A row further down: the panel leaves the row above it, and the row stays
	// where the pointer is; only one panel ever exists.
	await page.evaluate(() => window.scrollTo(0, 0));
	await page.getByTestId('task-step-3').scrollIntoViewIfNeeded();
	const before = await top(page, 'task-step-3');
	await page.getByTestId('task-deep-link-step-3').click();
	await expect(panel.getByTestId('task-log-step-3')).toContainText('step 3 says hello', { timeout: 30_000 });
	expect(await under('step-3')).toBe('task-panel');
	await expect(panel).toHaveCount(1);
	await expect(page.getByTestId('task-log-step-1')).toHaveCount(0);
	// The row kept its place, or moved up only as far as showing the panel asked.
	const after = await top(page, 'task-step-3');
	expect(after).toBeLessThanOrEqual(before + 1);
	const bar = await page.locator('header').first().evaluate((el) => el.getBoundingClientRect().bottom);
	expect(after).toBeGreaterThanOrEqual(bar - 1);
	await expect(page.getByTestId('task-deep-link-step-3')).toBeFocused();

	// Row and panel read together: the panel's end is on screen, or the row is
	// right under the bar with the panel taking the rest.
	const box = await panel.boundingBox();
	const fits = box!.y + box!.height <= 801;
	expect(fits || Math.abs(after - bar) <= 1).toBe(true);

	// Inspect opens inside the panel.
	await page.getByTestId('task-inspect-toggle-step-3').click();
	await expect(panel.getByTestId('task-step-3-inspect')).toBeVisible();
	await page.getByTestId('task-inspect-toggle-step-3').click();

	// In dark, once its transition ends, Inspect reads in the secondary ink.
	const inspect = page.getByTestId('task-inspect-toggle-step-3');
	const inkOf = () => inspect.evaluate((el) => getComputedStyle(el).color);
	await page.mouse.move(0, 0);
	await page.emulateMedia({ colorScheme: 'dark' });
	const during = await inkOf();
	await settled(page);
	const steady = await inkOf();
	const ink2 = await page.evaluate(() => {
		const probe = document.createElement('i');
		probe.style.color = 'var(--ink-2)';
		document.body.append(probe);
		const color = getComputedStyle(probe).color;
		probe.remove();
		return color;
	});
	note(`Inspect in dark: ${during} right after the switch, ${steady} settled, --ink-2 is ${ink2}`);
	expect(steady).toBe(ink2);
	await page.emulateMedia({ colorScheme: 'light' });
	await settled(page);

	// Pressing the chosen row again lets it go.
	await page.getByTestId('task-deep-link-step-3').click();
	await expect(panel).toHaveCount(0);
});

test('a fan-out keeps its grid under its row, with the panel below the grid', async ({ page }) => {
	await open(page, finished);
	await selectTask(page, 'convert');
	const grid = page.getByTestId('task-convert-each');
	await expect(grid).toBeVisible();
	const order = await page.evaluate(() => {
		const row = document.querySelector('[data-testid="task-convert"]')!;
		const first = row.nextElementSibling!;
		return [!!first.querySelector('[data-testid="task-convert-each"]'), first.nextElementSibling?.getAttribute('data-testid')];
	});
	expect(order).toEqual([true, 'task-panel']);
	await expect(page.getByTestId('task-panel').getByTestId('run-log-pick-item')).toBeVisible();
});

test('the run reads between its graph and its timeline, and the task keeps what is about the task', async ({ page }) => {
	await open(page, finished);
	const inputs = page.getByTestId('run-inputs');
	const outputs = page.getByTestId('run-outputs');
	await expect(inputs).toContainText('hi there');
	await expect(page.getByTestId('run-output-kind')).toHaveText('Outputs');
	// The undeclared case explains itself: in words, to a keyboard and a screen reader, never by colour.
	const mark = page.getByTestId('run-output-mark');
	await expect(mark).toContainText('not declared');
	await expect(mark).toContainText('This Orkflow declares no outputs, so these are the values of the tasks no other task needs.');
	await expect(mark).toHaveAttribute('tabindex', '0');
	await mark.focus();
	await expect(page.locator('.tt')).toHaveText('This Orkflow declares no outputs, so these are the values of the tasks no other task needs.');
	await page.keyboard.press('Escape');
	expect(/leaf/i.test(await page.locator('main#content').innerText())).toBe(false);
	await page.setViewportSize({ width: 1280, height: 800 });
	for (const theme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: theme });
		await outputs.screenshot({ path: out(`screenshot-outputs-undeclared-${theme}.png`) });
	}
	await page.emulateMedia({ colorScheme: 'light' });
	const box = async (testid: string) => (await page.getByTestId(testid).boundingBox())!;
	const [graph, tasks, map, inBox, outBox] = [await box('run-drawing'), await page.locator('section.tasks').boundingBox(), await box('run-map-card'), await box('run-inputs'), await box('run-outputs')];
	// Between the graph and the timeline, side by side, the width of the page.
	expect(inBox.y).toBeGreaterThanOrEqual(graph.y + graph.height);
	expect(inBox.y + inBox.height).toBeLessThanOrEqual(tasks!.y);
	expect(outBox.y).toBe(inBox.y);
	expect(outBox.x).toBeGreaterThanOrEqual(inBox.x + inBox.width);
	await expect(outputs).toBeVisible();
	// Task map and Since last run stay in the aside; nothing about one task is there.
	const aside = page.locator('aside[aria-label="Run details"]');
	await expect(aside.getByTestId('run-map-card')).toBeVisible();
	await expect(aside.getByText('Since the last run')).toBeVisible();
	await expect(aside.getByTestId('run-inputs')).toHaveCount(0);
	await expect(aside.getByTestId('run-outputs')).toHaveCount(0);
	expect(map.x).toBeGreaterThanOrEqual(tasks!.x + tasks!.width);

	await selectTask(page, 'step-2');
	await expect(aside.getByTestId('run-image')).toHaveCount(0);
	await expect(aside.getByTestId('item-card')).toHaveCount(0);
	const panel = page.getByTestId('task-panel');
	await expect(panel.getByTestId('run-image')).toContainText('activity-shell');
	await expect(panel.getByTestId('run-image')).toContainText('sha256');
	await shots(page, 'run-values-and-panel');
});

test('a row says a word for why it ended, and its panel says the whole reason', async ({ page }) => {
	await open(page, finished);
	const row = page.getByTestId('task-notify-owner');
	await expect(page.getByTestId('task-notify-owner-reason-word')).toHaveText('condition false');
	await expect(row).not.toContainText('evaluated false');
	// One line of words: the condition's expression does not make the row tall.
	expect((await row.boundingBox())!.height).toBeLessThan(80);
	// An unpinned row names no Orker; its panel does.
	await expect(page.getByTestId('task-step-5-orker')).toHaveCount(0);
	await selectTask(page, 'notify-owner');
	await expect(page.getByTestId('task-panel').getByTestId('task-notify-owner-reason')).toHaveText(`task skipped: condition ${JSON.stringify(SKIP_CONDITION)} evaluated false`);
	await selectTask(page, 'step-5');
	await expect(page.getByTestId('task-panel').getByTestId('task-step-5-card-orker')).not.toBeEmpty();

	// The axis keeps its marks apart however narrow the timeline is (1184 px sits
	// just above where the aside drops under the column).
	for (const width of [1184, 1280, 1600]) {
		await page.setViewportSize({ width, height: 800 });
		await expect
			.poll(() =>
				page.locator('.tl-axis span').evaluateAll((marks) => {
					const boxes = marks.map((m) => m.getBoundingClientRect());
					return boxes.every((box, i) => i === 0 || box.left >= boxes[i - 1].right);
				})
			)
			.toBe(true);
	}
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.getByTestId('task-notify-owner').scrollIntoViewIfNeeded();
	await shots(page, 'timeline-rows');
});

test('every row names its Orker once asked, and the choice is remembered', async ({ page }) => {
	await open(page, finished);
	await page.evaluate(() => localStorage.removeItem('orkstack.run.orkers'));
	await page.reload();
	await expect(page.getByTestId('task-step-5')).toBeVisible();
	// Rows only: the panel of a selected task names its Orker either way.
	const named = page.locator('[data-testid^="task-"][data-testid$="-orker"]:not([data-testid$="-card-orker"])');
	// Not asked: no row says it, and a row stays as short as it was.
	await expect(named).toHaveCount(0);
	const before = (await page.getByTestId('task-step-5').boundingBox())!.height;

	await page.getByTestId('run-tasks-more').click();
	await expect(page.getByTestId('run-orkers-toggle')).toHaveText('Show Orkers');
	await page.getByTestId('run-orkers-toggle').click();

	// Asked: the row says what its panel says.
	const row = page.getByTestId('task-step-5-orker');
	await expect(row).toBeVisible();
	await selectTask(page, 'step-5');
	const panel = (await page.getByTestId('task-panel').getByTestId('task-step-5-card-orker').textContent())!.trim();
	expect(panel).not.toBe('');
	await expect(row).toContainText(panel);
	// The row may clip a long id, so its tip holds the whole of it.
	await expect(row).toHaveAttribute('data-tip', panel);
	await expect(page.getByTestId('task-prepare-orker')).toContainText(panel);
	// A fan-out's row speaks for its items, all on this one Orker here.
	await expect(page.getByTestId('task-convert-orker')).toContainText(panel);
	// A task that reached no Orker has nothing to say, and says nothing.
	await expect(page.getByTestId('task-notify-owner-orker')).toHaveCount(0);
	expect((await page.getByTestId('task-step-5').boundingBox())!.height).toBe(before);
	await page.getByTestId('task-step-5').scrollIntoViewIfNeeded();
	await shots(page, 'orkers-shown');

	// A reader who asked keeps it across pages.
	await page.reload();
	await expect(page.getByTestId('task-step-5-orker')).toBeVisible();

	// The phone floor: however long an id is, the page does not scroll sideways.
	await page.setViewportSize({ width: 390, height: 800 });
	await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
	await page.setViewportSize({ width: 1280, height: 800 });

	await page.getByTestId('run-tasks-more').click();
	await expect(page.getByTestId('run-orkers-toggle')).toHaveText('Hide Orkers');
	await page.getByTestId('run-orkers-toggle').click();
	await expect(named).toHaveCount(0);
	await page.reload();
	await expect(page.getByTestId('task-step-5')).toBeVisible();
	await expect(named).toHaveCount(0);
});

test('Finished is said for the run and for each task, and a live task has it before the run ends', async ({ page }) => {
	await rpc('OrkflowService/ApplyOrkflow', { yaml: slow });
	const run = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: SLOW })).run.id;
	await open(page, run);
	await expect(page.getByTestId('run-status')).toHaveText('Running');
	// The ring is blue while the run runs.
	const liveRing = await page.getByTestId('run-progress').locator('.val').evaluate((el) => getComputedStyle(el).stroke);
	const runBlue = await page.evaluate(() => {
		const probe = document.createElement('i');
		probe.style.color = 'var(--run)';
		document.body.append(probe);
		const color = getComputedStyle(probe).color;
		probe.remove();
		return color;
	});
	expect(liveRing).toBe(runBlue);
	await expect(page.getByTestId('task-quick-status')).toHaveText('Succeeded', { timeout: 120_000 });
	// Still running, with no reload: the task knows when it finished, the run does not yet.
	await selectTask(page, 'quick');
	await expect(page.getByTestId('task-quick-finished')).toHaveText(/\d\d:\d\d:\d\d/);
	await expect(page.getByTestId('run-status')).toHaveText('Running');
	await expect(page.getByTestId('run-finished')).toHaveCount(0);
	await shots(page, 'finished-live');
	expect(await terminal(run)).toBe('STATUS_SUCCEEDED');
	await expect(page.getByTestId('run-finished')).toBeVisible({ timeout: 30_000 });

	// A finished run: the ring is green here and on the Runs list, which shares it.
	await open(page, run);
	const stroke = (at: ReturnType<Page['getByTestId']>) => at.locator('.val').evaluate((el) => getComputedStyle(el).stroke);
	const token = (name: string) =>
		page.evaluate((name) => {
			const probe = document.createElement('i');
			probe.style.color = `var(${name})`;
			document.body.append(probe);
			const color = getComputedStyle(probe).color;
			probe.remove();
			return color;
		}, name);
	expect(await stroke(page.getByTestId('run-progress'))).toBe(await token('--ok-mark'));
	await page.emulateMedia({ colorScheme: 'dark' });
	expect(await stroke(page.getByTestId('run-progress'))).toBe(await token('--ok-mark'));
	await page.emulateMedia({ colorScheme: 'light' });

	// A finished run: the header says it, and so does a task's panel.
	await open(page, finished);
	await expect(page.getByTestId('run-finished')).toContainText(/\d\d:\d\d:\d\d/);
	await selectTask(page, 'step-4');
	await expect(page.getByTestId('task-step-4-finished')).toHaveText(/\d\d:\d\d:\d\d/);
	// Not per row: selected or not, a row has no finish of its own.
	await expect(page.getByTestId('task-step-5').getByTestId('task-step-5-finished')).toHaveCount(0);
	// A skipped task has one too, though it never started.
	await selectTask(page, 'notify-owner');
	await expect(page.getByTestId('task-notify-owner-finished')).toHaveText(/\d\d:\d\d:\d\d/);
});

test('Edit goes to the editor on the run\'s version, and is neutral', async ({ page }) => {
	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow('0.2.0') });
	await open(page, finished);
	const edit = page.getByTestId('run-edit');
	await expect(edit).toHaveAttribute('data-tip', 'Edit from 0.1.0');
	await expect(edit.locator('use')).toHaveAttribute('href', '#i-edit');
	await expect(edit).toHaveClass(/\bbtn\b/);
	await expect(edit).not.toHaveClass(/btn-(primary|quiet)/);
	await expect(page.locator('.btn-primary:visible')).toHaveCount(1);
	await shots(page, 'run-header');
	await edit.click();
	await expect(page).toHaveURL(new RegExp(`/orkflows/${ORKFLOW}\\?.*tab=edit`));
	expect(new URL(page.url()).searchParams.get('version')).toBe('0.1.0');
	// A newer head shows the editor's existing notice: it forks forward from this version.
	await expect(page.getByTestId('editor-fork-notice')).toContainText('0.1.0');
});

test('the version plate names the run\'s version alone and opens it on the Orkflow\'s page', async ({ page }) => {
	await open(page, finished);
	const plate = page.getByTestId('run-version-link');
	await expect(plate).toContainText('0.1.0');
	await expect(page.getByTestId('run-versions')).toHaveCount(0);
	await plate.click();
	await expect(page).toHaveURL(new RegExp(`/orkflows/${ORKFLOW}\\?version=0\\.1\\.0`));
	await expect(page.getByTestId('orkflow-version')).toContainText('0.1.0');
});
