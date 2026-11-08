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

// The CLI panel, at the 1280 × 800 design floor, against a real engine on real
// Temporal and the dev object store (UX2):
//
//   sweep    every CLI icon on the seeded pages opens a panel of one width,
//            hung off its own trigger, whose every command shows in full;
//   table    an icon in a table's scrolling frame is not clipped by the frame,
//            scrolled or not, and the panel follows its trigger;
//   dialog   an icon in a modal is not clipped by the modal, and Escape closes
//            the panel and leaves the modal open;
//   edges    a trigger against each viewport edge and corner keeps its panel
//            on screen;
//   short    a panel taller than the viewport scrolls rather than loses a
//            command.
//
// "Shows" is measured by what the browser hit-tests at the corners of each
// command, so a clip is seen as a clip however the panel's own box measures.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and port; it
// ends only the process it started. The Orkflow is one awaited task, so no
// Orker and no image is involved.

const PORT = 19021;
const ENGINE = `http://127.0.0.1:${PORT}`;
const UI = ENGINE;
const BUCKET = ownBucket('cli-panel');
const STAMP = Date.now().toString(36);
const NS = `cli-${STAMP}`;
const NAME = 'panel';
const SCHEDULE = 'nightly-with-a-long-name-so-its-command-wraps';
const BUILD = 'v1';
const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'cli-panel') : path.join(uiDir, 'test-results', 'cli-panel');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

const ORKFLOW = `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
tasks:
  - id: approval
    await:
      timeout_seconds: 3600
`;

/** The width every CLI panel has at the design floor: 35rem of content, its padding and its border. */
const PANEL_WIDTH = 590;
const GAP = 6;
const EDGE = 8;
const view = { width: 1280, height: 800 };

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (await tcpOpen(PORT)) throw new Error(`port ${PORT} already in use`);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...clusterEnv(temporalNamespace('cli-panel')),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: `127.0.0.1:${PORT}`,
		ORK_ADMIN_ADDR: '127.0.0.1:0',
		// A versioned engine, so the Releases table has a row with its own icons.
		ORK_DEPLOYMENT_NAME: `cli-panel-${STAMP}`,
		ORK_BUILD_ID: BUILD,
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: ORKFLOW, namespace: NS });
	await rpc('WebhookService/RegisterWebhook', { url: 'https://example.com/hook', events: [] });
	await rpc('ScheduleService/CreateSchedule', { namespace: NS, name: SCHEDULE, orkflowName: NAME, spec: { cron: ['0 2 * * *'], timezone: 'UTC' }, paused: true });
	await rpc('TriggerService/CreateTrigger', { namespace: NS, orkflowName: NAME, description: 'panel' });
	note(`namespace ${NS}, orkflow ${NAME}, engine ${ENGINE}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test.beforeEach(async ({ page }) => {
	await page.setViewportSize(view);
});

interface Box {
	left: number;
	right: number;
	top: number;
	bottom: number;
	width: number;
}

/** Opens one icon and reports where its panel is and what of its commands can be seen. */
async function open(page: Page, trigger: string) {
	await page.getByTestId(trigger).click();
	const panel = page.getByTestId(trigger.replace(/-trigger$/, '-popover'));
	await expect(panel).toBeVisible();
	return panel.evaluate(async (el, id) => {
		await Promise.all(el.getAnimations().map((a) => a.finished));
		const box = (e: Element): Box => {
			const r = e.getBoundingClientRect();
			return { left: Math.round(r.left), right: Math.round(r.right), top: Math.round(r.top), bottom: Math.round(r.bottom), width: Math.round(r.width) };
		};
		const hidden = [...el.querySelectorAll('pre')].flatMap((pre) => {
			const r = pre.getBoundingClientRect();
			const corners = [
				[r.left + 2, r.top + 2],
				[r.right - 2, r.top + 2],
				[r.left + 2, r.bottom - 2],
				[r.right - 2, r.bottom - 2]
			];
			const seen = corners.every(([x, y]) => {
				const at = document.elementFromPoint(x, y);
				return !!at && pre.contains(at);
			});
			const cut = pre.scrollWidth > pre.clientWidth + 1;
			return seen && !cut ? [] : [`${pre.textContent}: ${seen ? 'runs past its box' : 'covered or clipped'}`];
		});
		return { panel: box(el), trigger: box(document.querySelector(`[data-testid="${id}"]`)!), hidden };
	}, trigger);
}

async function close(page: Page, trigger: string): Promise<void> {
	await page.keyboard.press('Escape');
	await expect(page.getByTestId(trigger.replace(/-trigger$/, '-popover'))).toBeHidden();
}

function expectInView(panel: Box, where: string): void {
	expect(panel.left, `${where}: left`).toBeGreaterThanOrEqual(EDGE);
	expect(panel.right, `${where}: right`).toBeLessThanOrEqual(view.width - EDGE);
	expect(panel.top, `${where}: top`).toBeGreaterThanOrEqual(EDGE);
	expect(panel.bottom, `${where}: bottom`).toBeLessThanOrEqual(view.height - EDGE);
}

/** Beside its trigger, on the trigger's end edge: the same place relative to it every time. */
function expectHung(m: { panel: Box; trigger: Box }, where: string): void {
	// Whole pixels at either end, so a fractional layout of the page is not a move.
	const near = (a: number, b: number) => Math.abs(a - b) <= 1;
	const under = near(m.panel.top, m.trigger.bottom + GAP);
	const over = near(m.panel.bottom, m.trigger.top - GAP);
	expect(under || over, `${where}: panel ${JSON.stringify(m.panel)} hangs off trigger ${JSON.stringify(m.trigger)}`).toBe(true);
	// Unless that edge is nearer the viewport's than the panel is wide: then it stops at the edge.
	const room = m.trigger.right - EDGE >= m.panel.width;
	expect(near(m.panel.right, m.trigger.right) || (!room && near(m.panel.left, EDGE)), `${where}: panel ${JSON.stringify(m.panel)} on trigger's end edge ${m.trigger.right}`).toBe(true);
}

const PAGES: { name: string; url: string; ready: string }[] = [
	{ name: 'dashboard', url: `/?ns=${NS}`, ready: 'dashboard-heading' },
	{ name: 'runs', url: `/runs?ns=${NS}`, ready: 'runs-cli-trigger' },
	{ name: 'orkflows', url: `/orkflows?ns=${NS}`, ready: `orkflow-row-${NAME}` },
	{ name: 'schedules', url: `/orkflows/${NAME}?ns=${NS}&tab=schedules`, ready: `schedule-row-${SCHEDULE}` },
	{ name: 'triggers', url: `/orkflows/${NAME}?ns=${NS}&tab=triggers`, ready: 'trigger-list' },
	{ name: 'versions', url: `/orkflows/${NAME}?ns=${NS}&tab=versions`, ready: 'page-cli-trigger' },
	{ name: 'namespaces', url: `/admin/namespaces?ns=${NS}`, ready: 'namespace-list' },
	{ name: 'pools', url: `/admin/pools?ns=${NS}`, ready: 'pools-cli-trigger' },
	{ name: 'releases', url: `/admin/releases?ns=${NS}`, ready: `release-cli-promote-${BUILD}-trigger` },
	{ name: 'audit', url: `/admin/audit?ns=${NS}`, ready: 'audit-scope' },
	{ name: 'webhooks', url: `/admin/webhooks?ns=${NS}`, ready: 'webhook-list' },
	{ name: 'usage', url: `/admin/usage?ns=${NS}`, ready: 'usage-admin-scope' },
	{ name: 'cache', url: `/admin/cache?ns=${NS}`, ready: 'cache-scope' },
	{ name: 'automation', url: `/admin/automation?ns=${NS}`, ready: 'automation-scope' }
];

test('every CLI icon opens one width of panel, hung off its trigger, with its whole command showing', async ({ page }) => {
	test.setTimeout(5 * 60_000);
	let opened = 0;
	for (const { name, url, ready } of PAGES) {
		await page.goto(`${UI}${url}`);
		await expect(page.getByTestId(ready).first()).toBeVisible();
		const ids = await page.locator('[data-cli-icon]').evaluateAll((els) => els.map((e) => e.getAttribute('data-testid')!));
		expect(ids.length, `${name} has a CLI icon to open`).toBeGreaterThan(0);
		for (const id of ids) {
			await page.getByTestId(id).scrollIntoViewIfNeeded();
			const m = await open(page, id);
			note(`${name} ${id} panel ${JSON.stringify(m.panel)} trigger ${JSON.stringify(m.trigger)}`);
			expect(m.hidden, `${name} ${id}`).toEqual([]);
			expect(m.panel.width, `${name} ${id}: panel width`).toBe(PANEL_WIDTH);
			expectInView(m.panel, `${name} ${id}`);
			expectHung(m, `${name} ${id}`);
			await close(page, id);
			opened += 1;
		}
	}
	note(`opened ${opened} icons on ${PAGES.length} pages`);
});

test('an icon in a table row is not clipped by the table, scrolled or not', async ({ page }) => {
	await page.goto(`${UI}/admin/releases?ns=${NS}`);
	const id = `release-cli-promote-${BUILD}-trigger`;
	await expect(page.getByTestId(id)).toBeVisible();

	// The control: the table's frame really does clip what hangs out of it.
	const frame = page.getByTestId('releases-table').locator('xpath=ancestor::div[contains(@class,"overflow-x-auto")]');
	const clips = await frame.evaluate((el) => getComputedStyle(el).overflowY);
	expect(['auto', 'scroll', 'hidden', 'clip'], 'the frame clips vertically').toContain(clips);

	const plain = await open(page, id);
	await page.screenshot({ path: out('table-row.png') });
	expect(plain.hidden).toEqual([]);
	expectHung(plain, 'in the frame');
	await close(page, id);

	// Narrowed until it really scrolls, and scrolled so the icon is in its view.
	await frame.evaluate((el) => {
		el.style.width = '700px';
		el.scrollLeft = el.scrollWidth;
	});
	expect(await frame.evaluate((el) => el.scrollWidth > el.clientWidth), 'the frame scrolls').toBe(true);
	await expect(page.getByTestId(id)).toBeInViewport();
	const scrolled = await open(page, id);
	await page.screenshot({ path: out('table-row-scrolled.png') });
	expect(scrolled.hidden).toEqual([]);
	expectHung(scrolled, 'in the scrolled frame');

	// Open, it follows its trigger when the frame scrolls under it.
	await frame.evaluate((el) => {
		el.scrollLeft -= 40;
	});
	await expect
		.poll(async () => {
			const [panel, trigger] = await Promise.all(
				[id.replace(/-trigger$/, '-popover'), id].map((t) => page.getByTestId(t).evaluate((e) => Math.round(e.getBoundingClientRect().right)))
			);
			return panel - trigger;
		})
		.toBe(0);
	await close(page, id);
});

test('an icon in a modal is not clipped by it, and Escape closes only the panel', async ({ page }) => {
	await page.goto(`${UI}/admin/webhooks?ns=${NS}`);
	await page.locator('[data-testid^="edit-webhook-"]').first().click();
	await expect(page.getByTestId('webhook-edit-dialog')).toBeVisible();
	const id = 'webhook-edit-cli-trigger';
	const m = await open(page, id);
	await page.screenshot({ path: out('dialog.png') });
	expect(m.hidden).toEqual([]);
	expectInView(m.panel, 'webhook dialog');
	// The panel takes its clicks inside the modal: the copy button answers.
	await page.getByTestId('webhook-edit-cli-copy').click();
	await expect(page.getByTestId('webhook-edit-cli-copied')).not.toBeEmpty();
	await close(page, id);
	await expect(page.getByTestId('webhook-edit-dialog')).toBeVisible();
});

test('a trigger against each viewport edge and corner keeps its panel on screen', async ({ page }) => {
	test.setTimeout(3 * 60_000);
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}&tab=schedules`);
	const id = `schedule-cli-${SCHEDULE}-trigger`;
	await expect(page.getByTestId(id)).toBeVisible();
	const places: [string, Record<string, string>][] = [
		['top left', { left: '0', top: '0' }],
		['top', { left: '50%', top: '0' }],
		['top right', { right: '0', top: '0' }],
		['right', { right: '0', top: '50%' }],
		['bottom right', { right: '0', bottom: '0' }],
		['bottom', { left: '50%', bottom: '0' }],
		['bottom left', { left: '0', bottom: '0' }],
		['left', { left: '0', top: '50%' }]
	];
	for (const [where, style] of places) {
		// The Popover's own wrapper, pinned where the page would never otherwise put it.
		await page.getByTestId(id).evaluate((button, s) => {
			const root = button.parentElement!;
			root.removeAttribute('style');
			root.style.position = 'fixed';
			root.style.zIndex = '40';
			Object.assign(root.style, s);
		}, style);
		const m = await open(page, id);
		note(`${where}: panel ${JSON.stringify(m.panel)} trigger ${JSON.stringify(m.trigger)}`);
		await page.screenshot({ path: out(`edge-${where.replace(' ', '-')}.png`) });
		expect(m.hidden, where).toEqual([]);
		expect(m.panel.width, where).toBe(PANEL_WIDTH);
		expectInView(m.panel, where);
		// Beside its trigger, never over it: the trigger stays what closes it.
		expect(m.panel.bottom <= m.trigger.top || m.panel.top >= m.trigger.bottom, `${where}: clear of its trigger`).toBe(true);
		await close(page, id);
	}
});

test('a panel taller than the viewport scrolls rather than loses a command', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 320 });
	await page.goto(`${UI}/runs?ns=${NS}`);
	const id = 'runs-cli-trigger';
	await expect(page.getByTestId(id)).toBeVisible();
	await page.getByTestId(id).click();
	const panel = page.getByTestId('runs-cli-popover');
	await expect(panel).toBeVisible();
	const size = await panel.evaluate(async (el) => {
		await Promise.all(el.getAnimations().map((a) => a.finished));
		return { scroll: el.scrollHeight, client: el.clientHeight, bottom: el.getBoundingClientRect().bottom };
	});
	expect(size.scroll, 'the panel has more than it can show').toBeGreaterThan(size.client);
	expect(Math.round(size.bottom)).toBeLessThanOrEqual(320 - EDGE);
	const last = page.locator('[data-testid^="runs-cli-command-"]').last();
	await last.scrollIntoViewIfNeeded();
	await expect(last).toBeInViewport({ ratio: 1 });
});
