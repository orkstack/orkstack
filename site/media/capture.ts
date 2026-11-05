// The site's screenshots and its recordings of the UI, taken from the real
// stack by a script, never by hand (make site-media runs it through
// media/run.sh, which first checks the stack is up).
//
// It applies the Orkflows it shows into the `default` namespace, the one a
// reader of the quickstart sees, runs them, and captures the UI in both themes.
// Every frame shows those runs and nothing else. Most are the examples in
// examples/, and never one its README marks older. Two are its own
// (media/orkflows/): `greet`, the one the first tutorial has a reader write, and
// `render-configs`, which runs on a pool of its own so that items really wait
// for capacity. That pool's Orker is started here and stopped here, and so is
// the small HTTP API order-saga and approval-gate call.
//
// A screenshot is captured twice per theme, at one and at two device pixels per
// CSS pixel, and saved losslessly; a page serves the pair as a srcset. Each is a
// cut of whole cards. The viewports are tall, so a cut never scrolls and the
// top bar never covers it:
//   - TOUR, the landing page's: the UI's full two-column layout;
//   - NARROW, the docs': the UI's one-column layout, its cards a little wider
//     than the docs column (starlight.css, --sl-content-width);
//   - WIDE, the docs' for a task beside its Item card: a two-column layout whose
//     task timeline still has room for its axis, a little wider than the docs
//     column.
//
// The browser is the UI e2e suite's own: Playwright's browser server in the
// image ui/e2e/browser-image.txt pins by digest, reached over its connection
// with the host's loopback exposed.
import { chromium, type Browser, type BrowserContext, type Locator, type Page } from '@playwright/test';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { closeSync, mkdirSync, openSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import path from 'node:path';
import sharp from 'sharp';
import { startBrowserServer, stopBrowserServer } from '../../ui/e2e/browser-server';
import { ffmpeg, READ, stage, Take } from './demo';

const repo = path.resolve(import.meta.dirname, '..', '..');
const assets = path.join(repo, 'site', 'src', 'assets', 'media');
const publicMedia = path.join(repo, 'site', 'public', 'media');
const work = path.join(repo, 'site', '.media-work');
// media/run.sh serves the built UI (vite preview, which proxies the API to the
// engine) and says where.
const UI = process.env.ORK_UI_URL ?? 'http://127.0.0.1:4173';
const ENGINE = process.env.ORK_SERVER ?? 'http://127.0.0.1:8080';
const NS = 'default';
const ORKFLOWS = [
	'examples/each-fan-out.ork.yaml',
	'examples/notify-block.ork.yaml',
	'examples/order-saga.ork.yaml',
	'examples/approval-gate.ork.yaml',
	'site/media/orkflows/greet.ork.yaml',
	'site/media/orkflows/render-configs.ork.yaml',
];
// SITE_MEDIA_TAKES=e2e-demo,ui-editor redoes those recordings and
// SITE_MEDIA_SHOTS=tour-run,ui-await those screenshots, keeping every other item
// as it is. With neither set, a run redoes them all.
const onlyTakes = process.env.SITE_MEDIA_TAKES?.split(',').map((n) => n.trim()).filter(Boolean);
const onlyShots = process.env.SITE_MEDIA_SHOTS?.split(',').map((n) => n.trim()).filter(Boolean);
const partial = Boolean(onlyTakes?.length || onlyShots?.length);

const TOUR = { width: 1440, height: 1300 };
const WIDE = { width: 1280, height: 1500 };
const NARROW = { width: 1120, height: 1200 };
// The space a cut leaves around its cards, each side.
const PAD = 12;

const themes = ['light', 'dark'] as const;
type Theme = (typeof themes)[number];
const scales = [1, 2] as const;
type Scale = (typeof scales)[number];
interface View {
	theme: Theme;
	scale: Scale;
	page: Page;
}
/** One viewport's four pages: each theme at one and at two device pixels per CSS pixel. */
type Views = View[];
interface Clip {
	x: number;
	y: number;
	width: number;
	height: number;
}

function orkctl(...args: string[]): string {
	return execFileSync(path.join(repo, 'bin', 'orkctl'), ['--server', ENGINE, '-n', NS, ...args], {
		cwd: repo,
		encoding: 'utf8',
		stdio: ['ignore', 'pipe', 'pipe'],
	});
}

function submit(name: string, ...args: string[]): string {
	const out = JSON.parse(orkctl('run', 'submit', name, ...args, '--json'));
	const id = out.id ?? out.run?.id ?? out.runId;
	if (!id) throw new Error(`orkctl run submit ${name} --json printed no run id: ${JSON.stringify(out)}`);
	return id;
}

/** Wait until a run has ended, however it ended. */
function finished(id: string): void {
	try {
		orkctl('--timeout', '0', 'run', 'watch', id);
	} catch {
		// A run that fails makes `run watch` exit non-zero; that is not this script's failure.
	}
}

// The HTTP API order-saga and approval-gate call (media/api-stub.mjs): it listens
// on the default bridge's gateway, which an activity container reaches, and
// refuses one order's shipment.
const API_PORT = 18099;
let api = '';
const REFUSED_ORDER = 'order-1042';

/** What this script started, for a signal to stop: it skips main's finally. */
const started: ChildProcess[] = [];
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
	process.once(signal, () => {
		for (const child of started) child.kill('SIGTERM');
		process.exit(143);
	});
}

async function startApi(): Promise<ChildProcess> {
	const gateway = execFileSync('docker', ['network', 'inspect', 'bridge', '--format', '{{(index .IPAM.Config 0).Gateway}}'], { encoding: 'utf8' }).trim();
	api = `http://${gateway}:${API_PORT}`;
	const child = spawn('node', [path.join(repo, 'site', 'media', 'api-stub.mjs'), gateway, String(API_PORT), REFUSED_ORDER], { stdio: ['ignore', 'pipe', 'inherit'] });
	started.push(child);
	await new Promise((resolve, reject) => {
		child.stdout?.once('data', resolve);
		child.once('exit', (code) => reject(new Error(`the API stub exited ${code}`)));
	});
	return child;
}

/** A run's input as JSON: order-saga's, for an order the API takes or refuses. */
const sagaInput = (id: string) => ({
	api,
	notify_url: `${api}/notify`,
	order: { id, sku: 'sku-7', quantity: 2, amount: '129.00', address: '1 Main St' },
});
const submitSaga = (id: string) => submit('order-saga', '--input-json', JSON.stringify(sagaInput(id)));

/** approval-gate's input: a change to a service it asks an approval for. */
const gateInput = () => ({
	notify_url: `${api}/notify`,
	changes: [{ service: 'payments', setting: 'replicas', value: '4' }],
});

/** The history a failed run's page reads: a success before it, and a failure like it. */
function failureHistory(): void {
	const list = JSON.parse(orkctl('run', 'list', '--limit', '100', '--json'));
	const runs: { orkflowName?: string; status?: string }[] = list.runs ?? [];
	const ended = (status: string) => runs.some((r) => r.orkflowName === 'order-saga' && r.status === status);
	if (!ended('SUCCEEDED')) finished(submitSaga('order-1041'));
	if (!ended('FAILED')) finished(submitSaga(REFUSED_ORDER));
}

/**
 * One Orker for the pool `batch`, with room for four 1-cpu tasks. It is this
 * script's own: the stack's Orker declares no capacity, and a run of
 * render-configs must really wait. Stopped by its own PID in main's finally.
 */
async function startBatchOrker(): Promise<ChildProcess> {
	const root = path.join(work, 'orker-batch');
	mkdirSync(root, { recursive: true });
	const log = openSync(path.join(work, 'orker-batch.log'), 'a');
	const child = spawn(path.join(repo, 'bin', 'orker'), [], {
		cwd: repo,
		stdio: ['ignore', log, log],
		env: {
			...process.env,
			ORK_ARTIFACT_BACKEND: 's3',
			ORK_ORKER_ID: 'orker-batch',
			ORK_ORKER_POOL: 'batch',
			ORK_ORKER_WORKDIR: root,
			ORK_ORKER_CAPACITY_CPU: '4',
			ORK_ORKER_MAX_CPU: '1',
			ORK_ORKER_HEARTBEAT_INTERVAL: '1s',
		},
	});
	closeSync(log);
	started.push(child);
	const ended = () => child.exitCode !== null || child.signalCode !== null;
	const failed = () => new Error(`the batch Orker exited: see ${path.join(work, 'orker-batch.log')}`);
	try {
		const deadline = Date.now() + 60_000;
		while (Date.now() < deadline) {
			if (ended()) throw failed();
			const orkers: { orkerId?: string; pool?: string }[] = JSON.parse(orkctl('orker', 'list', '--json')).orkers ?? [];
			if (orkers.some((o) => o.orkerId === 'orker-batch' && o.pool === 'batch')) {
				// An id is locked host-wide: a leftover Orker of the same id answers
				// the list while this one exits on that lock.
				await delay(1000);
				if (ended()) throw failed();
				return child;
			}
			await delay(500);
		}
		throw new Error('the batch Orker did not join its pool within a minute');
	} catch (err) {
		await stopChild(child);
		throw err;
	}
}

/** Stop a process this script started, by its own PID. */
async function stopChild(child: ChildProcess): Promise<void> {
	if (child.exitCode !== null || child.signalCode !== null) return;
	const gone = new Promise((resolve) => child.once('exit', resolve));
	child.kill('SIGTERM');
	const force = setTimeout(() => child.kill('SIGKILL'), 45_000);
	await gone;
	clearTimeout(force);
}

/** The next free version of an Orkflow: a version is immutable, so every take of an edit applies its own. */
function nextVersion(name: string): { from: number; to: number; text: string } {
	const list = JSON.parse(orkctl('orkflow', 'versions', name, '--json'));
	const head: string = list.headVersion ?? list.versions?.[0]?.version;
	const m = /^(\d+\.\d+\.)(\d+)$/.exec(head ?? '');
	if (!m) throw new Error(`${name}'s newest version does not read: ${JSON.stringify(list).slice(0, 200)}`);
	return { from: Number(m[2]), to: Number(m[2]) + 1, text: `${m[1]}${Number(m[2]) + 1}` };
}

const shots: Record<string, Record<string, string | number>> = {};
const items: Record<string, Record<string, string | number>> = {};

const all = (views: Views, fn: (p: Page) => Promise<unknown>) => Promise.all(views.map((v) => fn(v.page)));

/** Two animation frames: whatever a scroll or a click changed has been laid out and painted. */
const settled = (p: Page) => p.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));

class ViewsDisagree extends Error {}

/**
 * Capture one screenshot in every view, cut to the clip each view reports,
 * and save each losslessly as it is: NAME-THEME.webp at one device pixel per
 * CSS pixel, NAME-THEME-2x.webp at two. The clips must agree, or the pairs
 * would not match.
 */
async function shoot(name: string, alt: string, caption: string, views: Views, clip: (p: Page) => Promise<Clip>): Promise<void> {
	const clips = await Promise.all(views.map((v) => clip(v.page)));
	const first = clips[0];
	for (const c of clips) {
		if (c.x !== first.x || c.y !== first.y || c.width !== first.width || c.height !== first.height) {
			throw new ViewsDisagree(`${name}: the views disagree on the cut: ${JSON.stringify(clips)}`);
		}
	}
	const vp = views[0].page.viewportSize()!;
	if (first.y < 0 || first.y + first.height > vp.height) {
		throw new Error(`${name}: a cut of ${first.height}px at ${first.y} does not fit a ${vp.height}px viewport`);
	}
	const pngs = await Promise.all(views.map((v, i) => v.page.screenshot({ clip: clips[i], animations: 'disabled', caret: 'hide' })));
	const { width, height } = first;
	const item: Record<string, string | number> = { alt, caption, width, height };
	for (const [i, v] of views.entries()) {
		const meta = await sharp(pngs[i]).metadata();
		if (meta.width !== width * v.scale || meta.height !== height * v.scale) {
			throw new Error(`${name} ${v.theme}@${v.scale}x: ${meta.width}×${meta.height}, want ${width * v.scale}×${height * v.scale}`);
		}
		const file = `${name}-${v.theme}${v.scale === 2 ? '-2x' : ''}.webp`;
		await sharp(pngs[i]).webp({ lossless: true, effort: 6 }).toFile(path.join(publicMedia, file));
		item[v.scale === 2 ? `${v.theme}2x` : v.theme] = file;
	}
	shots[name] = item;
	console.log(`  ${name} (${width}×${height})`);
}

/** The cut around some cards: the box that holds them all, with PAD around it. */
async function around(...parts: Locator[]): Promise<Clip> {
	const boxes = await Promise.all(
		parts.map(async (part) => {
			await part.waitFor();
			const box = await part.boundingBox();
			if (!box) throw new Error(`${part} has no box to cut around`);
			return box;
		}),
	);
	const x = Math.min(...boxes.map((b) => b.x));
	const y = Math.min(...boxes.map((b) => b.y));
	const right = Math.max(...boxes.map((b) => b.x + b.width));
	const bottom = Math.max(...boxes.map((b) => b.y + b.height));
	return { x: Math.round(x - PAD), y: Math.round(y - PAD), width: Math.round(right - x + PAD * 2), height: Math.round(bottom - y + PAD * 2) };
}

async function contexts(browser: Browser, size: { width: number; height: number }): Promise<{ views: Views; contexts: BrowserContext[] }> {
	const views: Views = [];
	const made: BrowserContext[] = [];
	for (const theme of themes) {
		for (const scale of scales) {
			const ctx = await browser.newContext({ viewport: size, deviceScaleFactor: scale, colorScheme: theme, reducedMotion: 'reduce' });
			made.push(ctx);
			views.push({ theme, scale, page: await ctx.newPage() });
		}
	}
	return { views, contexts: made };
}

const run = (id: string, query = '') => `${UI}/runs/${id}?ns=${NS}${query}`;
const orkflow = (name: string, query = '') => `${UI}/orkflows/${name}?ns=${NS}${query}`;

/** The run page's status pill reads this. */
const status = (p: Page, want: string, timeout = 180_000) => p.getByTestId('run-status').filter({ hasText: want }).waitFor({ timeout });

/** The Tasks card: its rows, a selected fan-out's items and the log pane. */
const tasksCard = (p: Page) => p.locator('section.card').filter({ has: p.getByTestId('task-list') });

// Live: the fan-out task is running with some of its items done and some
// still to run. (An item runs for a second or two, so the moment to catch is
// the task's, not one item's.)
const fanningOut = (p: Page) =>
	p.evaluate(() => {
		const status = document.querySelector('[data-testid="task-convert-status"]')?.textContent?.trim();
		const items = [...document.querySelectorAll<HTMLElement>('[data-testid^="task-convert-item-"][data-status]')].map((e) => e.dataset.status);
		return status === 'Running' && items.includes('SUCCEEDED') && items.some((s) => s === 'PENDING' || s === 'RUNNING');
	});

const waitingForRoom = (p: Page) => p.getByTestId('run-capacity-note').isVisible();

/**
 * Submit a run, open it in every view and capture it while `ready` holds, all
 * four views in the same state. A capture taken across the moment that state
 * ended, or a run that ended before it was caught, is thrown away and taken
 * again on a fresh run. Returns the run's id.
 */
async function catchLive(views: Views, start: () => string, ready: (p: Page) => Promise<boolean>, capture: () => Promise<void>): Promise<string> {
	for (let tries = 0; tries < 8; tries++) {
		const id = start();
		await all(views, (p) => p.goto(run(id)));
		const deadline = Date.now() + 120_000;
		let caught = false;
		while (Date.now() < deadline) {
			if ((await Promise.all(views.map((v) => ready(v.page)))).every(Boolean)) {
				caught = true;
				break;
			}
			const shown = (await views[0].page.getByTestId('run-status').innerText().catch(() => '')).trim();
			if (shown && shown !== 'Running' && shown !== 'Pending') break;
			await settled(views[0].page);
		}
		if (caught) {
			try {
				await capture();
				if ((await Promise.all(views.map((v) => ready(v.page)))).every(Boolean)) return id;
			} catch (err) {
				if (!(err instanceof ViewsDisagree)) throw err;
			}
		}
		finished(id);
	}
	throw new Error('could not capture the run in that state in every view at once');
}

/** The line of the editor that reads exactly `text` (leading spaces and all). */
const editorLine = (p: Page, text: string) => p.locator('.cm-line').filter({ hasText: new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) });

/** Scroll the editor until a line is drawn, and to the middle of its view. */
async function revealLine(p: Page, line: Locator): Promise<void> {
	const scroller = p.locator('.cm-scroller').first();
	await scroller.waitFor();
	await scroller.evaluate((e) => e.scrollTo({ top: 0 }));
	await settled(p);
	for (let i = 0; i < 40 && (await line.count()) === 0; i++) {
		await scroller.evaluate((e) => e.scrollBy({ top: e.clientHeight * 0.6 }));
		await settled(p);
	}
	await line.first().evaluate((e) => {
		const box = e.closest('.cm-scroller')!;
		box.scrollBy({ top: e.getBoundingClientRect().top - box.getBoundingClientRect().top - box.clientHeight / 2 });
	});
	await settled(p);
}

interface ShotGroup {
	name: string;
	size: { width: number; height: number };
	take: (views: Views) => Promise<void>;
}

/** A finished run of each-fan-out, for the screenshots that read one. */
let finishedFanOut = '';
function fanOutRun(): string {
	if (!finishedFanOut) {
		finishedFanOut = submit('each-fan-out');
		finished(finishedFanOut);
	}
	return finishedFanOut;
}

/**
 * The screenshots. The tour's three are the landing page's; the rest are the
 * docs', and docs/*.md embed some of them by their two-x file.
 */
const shotGroups: ShotGroup[] = [
	{
		name: 'tour-run',
		size: TOUR,
		take: async (views) => {
			const id = await catchLive(views, () => submit('render-configs'), waitingForRoom, () =>
				shoot(
					'tour-run',
					'The run page of render-configs: a note above the graph says items of render are waiting for capacity because every Orker of pool batch is full, and the graph shows services done, render running and manifest pending.',
					'A real run on a pool at its capacity: the note says what holds it up.',
					views,
					(p) => around(p.getByTestId('run-capacity-note'), p.getByTestId('run-drawing')),
				),
			);
			finished(id);
		},
	},
	{
		name: 'tour-failed',
		size: TOUR,
		take: async (views) => {
			failureHistory();
			const id = submitSaga(REFUSED_ORDER);
			finished(id);
			await all(views, (p) => p.goto(run(id)));
			await all(views, (p) => status(p, 'Failed'));
			await all(views, settled);
			await shoot(
				'tour-failed',
				'The failed run page of order-saga: its header, a banner naming the shipped task and its reason, and the graph with shipped failed and the release, refund, report and notify failure handlers run.',
				'A real failed run: the failed task, its reason and its failure handlers.',
				views,
				(p) => around(p.getByTestId('run-header'), p.getByTestId('run-failure-banner'), p.getByTestId('run-drawing')),
			);
		},
	},
	{
		name: 'tour-editor',
		// The editor grows with the window above its 24 rows; this is the height
		// at which it stands at those rows.
		size: { width: TOUR.width, height: 1072 },
		take: async (views) => {
			await all(views, (p) => p.goto(orkflow('each-fan-out', '&tab=edit')));
			await all(views, async (p) => {
				await p.getByTestId('editor-docs').waitFor();
				const needs = editorLine(p, '      - list');
				await revealLine(p, needs);
				// Eight lines up: the document's first line under its comment heads the cut.
				await p.locator('.cm-scroller').first().evaluate((e) => e.scrollBy({ top: -174 }));
				await settled(p);
				// By the mouse: a click through Playwright would first scroll the
				// editor sideways, a line being as wide as the longest.
				const box = await needs.first().boundingBox();
				if (!box) throw new Error('the needs line has no box');
				await p.mouse.click(box.x + 48, box.y + box.height / 2);
				await p.keyboard.press('End');
				await p.keyboard.type('s');
				await p.getByTestId('editor-yaml-first-problem').waitFor({ timeout: 30_000 });
				await p.getByTestId('editor-docs-keys').waitFor({ timeout: 30_000 });
				await p.mouse.move(2, 2);
			});
			await all(views, settled);
			await shoot(
				'tour-editor',
				'The Orkflow editor on each-fan-out: a needs entry that names no task is underlined, the strip under the code says to fix one problem and gives its line, and the panel beside the code lists the shell activity’s keys with their types and descriptions.',
				'The editor beside the keys of the activity the cursor is in.',
				views,
				(p) => around(p.locator('section.edcard'), p.getByTestId('editor-docs')),
			);
		},
	},
	{
		name: 'ui-orkflow',
		size: WIDE,
		take: async (views) => {
			await all(views, (p) => p.goto(orkflow('each-fan-out')));
			await all(views, (p) => p.getByTestId('orkflow-outputs').waitFor());
			await all(views, settled);
			await shoot(
				'ui-orkflow',
				'The each-fan-out Orkflow’s page in the OrkStack UI after an apply: its graph of list, convert and merge, its stored version, and its three tasks with their image digests.',
				'An applied Orkflow: an immutable version, every image pinned to a digest.',
				views,
				async (p) => {
					const top = await around(p.getByTestId('orkflow-name'), p.getByTestId('orkflow-drawing'), p.getByTestId('orkflow-sheet-versions'));
					const outputs = await p.getByTestId('orkflow-outputs').boundingBox();
					if (!outputs) throw new Error('the Orkflow page has no outputs section to cut above');
					return { ...top, height: Math.round(outputs.y - PAD - top.y) };
				},
			);
		},
	},
	{
		name: 'ui-run-live',
		size: NARROW,
		take: async (views) => {
			const id = await catchLive(views, () => submit('each-fan-out'), fanningOut, () =>
				shoot(
					'ui-run-live',
					'A run of each-fan-out while it runs: the graph with list done, convert running and merge pending, and the Tasks card with a row for each task and a grid of convert’s items.',
					'A real run of examples/each-fan-out.ork.yaml, caught while its fan-out runs.',
					views,
					(p) => around(p.getByTestId('run-drawing'), tasksCard(p)),
				),
			);
			finished(id);
		},
	},
	{
		name: 'ui-run-finished',
		size: WIDE,
		take: async (views) => {
			const id = fanOutRun();
			await all(views, (p) => p.goto(run(id, '&task=merge')));
			await all(views, (p) => status(p, 'Succeeded'));
			await all(views, async (p) => {
				await p.getByTestId('task-log-merge').getByText('a', { exact: true }).first().waitFor({ timeout: 30_000 });
				await p.mouse.move(2, 2);
			});
			await all(views, settled);
			await shoot(
				'ui-run-finished',
				'The merge task of a finished each-fan-out run selected on the run page: its row and log in the Tasks card, and beside them its card with its attempts, Orker, pool, what it reserved, its timing and its phases.',
				'A finished run’s task: its log, attempts, timing and phases. Inspect, on the same card, lists its artifacts.',
				views,
				(p) => around(tasksCard(p), p.locator('#item-card')),
			);
		},
	},
	{
		name: 'ui-fanout',
		size: NARROW,
		take: async (views) => {
			const id = fanOutRun();
			await all(views, (p) => p.goto(run(id, '&task=convert&item=2')));
			await all(views, (p) => status(p, 'Succeeded'));
			await all(views, (p) => p.getByTestId('task-log-convert').getByText('converted').first().waitFor({ timeout: 30_000 }));
			await all(views, async (p) => {
				await p.mouse.move(2, 2);
				await settled(p);
			});
			await shoot(
				'ui-fanout',
				'The convert task’s fan-out in the Tasks card: a grid of four items, convert[2] selected, and its log below.',
				'Each fan-out item is its own durable activity, with its own attempts and logs.',
				views,
				(p) => around(tasksCard(p)),
			);
		},
	},
	{
		name: 'ui-await',
		size: WIDE,
		take: async (views) => {
			const id = submit('approval-gate', '--input-json', JSON.stringify(gateInput()));
			await all(views, (p) => p.goto(run(id)));
			await all(views, (p) => p.getByTestId('task-approval-status').filter({ hasText: 'Awaiting' }).waitFor({ timeout: 120_000 }));
			await all(views, (p) => p.getByTestId('task-approval-resolve').click());
			await all(views, (p) => p.getByTestId('await-resolve-dialog').waitFor());
			await all(views, (p) => p.getByTestId('declared-input-approved').check());
			await all(views, (p) => p.getByTestId('declared-input-approver').fill('example-approver'));
			await all(views, settled);
			await shoot(
				'ui-await',
				'An awaited task’s Resolve dialog over the run page: approved checked and the approver filled in, ready to complete.',
				'An awaited task waits for a person or a system; resolving it starts the next task.',
				views,
				(p) => around(p.getByTestId('await-resolve-dialog')),
			);
			const resolver = views[0].page;
			await resolver.getByTestId('await-resolve-submit').click();
			await status(resolver, 'Succeeded', 120_000);
		},
	},
];

async function screenshots(browser: Browser): Promise<void> {
	const unknown = onlyShots?.filter((n) => !shotGroups.some((g) => g.name === n));
	if (unknown?.length) throw new Error(`SITE_MEDIA_SHOTS names no screenshot ${unknown.join(', ')}; there are ${shotGroups.map((g) => g.name).join(', ')}`);
	for (const group of shotGroups) {
		if (partial && !onlyShots?.includes(group.name)) continue;
		const made = await contexts(browser, group.size);
		try {
			await group.take(made.views);
		} finally {
			for (const c of made.contexts) await c.close();
		}
	}
}

async function main() {
	mkdirSync(assets, { recursive: true });
	mkdirSync(publicMedia, { recursive: true });
	mkdirSync(work, { recursive: true });

	let batch: ChildProcess | undefined;
	let httpApi: ChildProcess | undefined;
	let server: ReturnType<typeof startBrowserServer> | undefined;
	let browser: Browser | undefined;
	let complete = false;
	try {
		batch = await startBatchOrker();
		httpApi = await startApi();
		server = startBrowserServer(`site-media-${process.pid}`);
		// An apply of bytes already stored, against the same image builds, is a
		// no-op. After the images are rebuilt it is refused (a version is immutable
		// and pins its images), and the capture stops with the server's error:
		// capture on a fresh stack then.
		for (const file of ORKFLOWS) orkctl('apply', file);

		browser = await chromium.connect(server.wsEndpoint, { exposeNetwork: '<loopback>' });
		await screenshots(browser);

		// The recordings, each in both themes (takes, below).
		const unknown = onlyTakes?.filter((n) => !takes.some((t) => t.name === n));
		if (unknown?.length) throw new Error(`SITE_MEDIA_TAKES names no recording ${unknown.join(', ')}; there are ${takes.map((t) => t.name).join(', ')}`);
		for (const take of takes) {
			if (partial && !onlyTakes?.includes(take.name)) continue;
			let seconds = 0;
			for (const theme of themes) {
				const length = await record(browser, take, theme);
				if (theme === 'light') seconds = length;
			}
			items[take.name] = {
				alt: take.alt,
				caption: take.caption,
				...(take.title ? { title: take.title, duration: `${Math.floor(Math.round(seconds) / 60)}:${String(Math.round(seconds) % 60).padStart(2, '0')}` } : {}),
				width: REC.width,
				height: REC.height,
				mp4: `${take.name}-light.mp4`,
				poster: `${take.name}-light.webp`,
				captions: `${take.name}-light.vtt`,
				mp4Dark: `${take.name}-dark.mp4`,
				posterDark: `${take.name}-dark.webp`,
				captionsDark: `${take.name}-dark.vtt`,
			};
		}
		complete = true;
	} finally {
		await browser?.close().catch(() => {});
		if (server) stopBrowserServer(server.container);
		if (batch) await stopChild(batch);
		if (httpApi) await stopChild(httpApi);
		writeManifest(complete, server?.image);
	}
}

/**
 * The manifest, written whatever happened: a failed run has already replaced
 * the files of what it finished. The terminal recordings (cli-*) are
 * media/finish.mjs's and stay. Every other item is this script's, so one it no
 * longer takes goes, unless this run redid only some or stopped early, which
 * keeps everything else.
 */
function writeManifest(complete: boolean, browserImage: string | undefined): void {
	const keepAll = partial || !complete;
	const previous = (() => {
		try {
			return JSON.parse(readFileSync(path.join(assets, 'manifest.json'), 'utf8'));
		} catch {
			return { items: {} };
		}
	})();
	const kept = Object.fromEntries(Object.entries(previous.items ?? {}).filter(([name]) => keepAll || name.startsWith('cli-')));
	const commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
	writeFileSync(
		path.join(assets, 'manifest.json'),
		JSON.stringify(
			{
				$comment: 'Written by make site-media (site/media/). Do not edit.',
				// A partial run redoes some items of an earlier full one.
				sourceCommit: keepAll ? (previous.sourceCommit ?? commit) : commit,
				orkflows: ORKFLOWS,
				browserImage: browserImage ?? previous.browserImage,
				items: { ...kept, ...items, ...shots },
				...(previous.vhsImage ? { vhsImage: previous.vhsImage } : {}),
			},
			null,
			'\t',
		) + '\n',
	);
}

// ---- The recordings: the UI driven by a stagehand (media/demo.ts) at
// 1280×800 CSS pixels, captured at two device pixels per CSS pixel, and paced
// for a reader: each screen that matters is held about three seconds, every
// click is announced by the cursor gliding to it, and a caption names each
// step. The steps are the recording's captions too, timed to the take.
const REC = { width: 1280, height: 800 };

interface Script {
	name: string;
	/** The name the landing page prints above a recording, with its length. */
	title?: string;
	alt: string;
	caption: string;
	/** Load the first page; the camera starts after it. */
	open: (p: Page) => Promise<void>;
	act: (t: Take, p: Page) => Promise<void>;
}

const takes: Script[] = [
	// ---- The landing page's three.
	{
		name: 'video-task-waits',
		title: 'A task waits for room, then runs',
		alt: 'A screen recording of render-configs on a pool with room for four 1-cpu tasks: four items run and the rest wait, the note above the graph says the pool is full, a card beside the tasks says every cpu of the pool is reserved, and each waiting item starts as a running one ends.',
		caption: 'A full pool holds items back; each one starts the moment an Orker has room.',
		open: async (p) => {
			await p.goto(orkflow('render-configs'));
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'One config per service', 'render-configs renders one config file per service, on a pool with room for four at a time.');
			await t.hold(1600);
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await t.rest();
			const note = p.getByTestId('run-capacity-note');
			await note.waitFor({ timeout: 120_000 });
			await t.hold(600);
			await t.say('2', 'The pool is full, so the rest wait', 'The pool is full, so the other items wait for capacity. The note above the graph says why.');
			await t.glide(note, true);
			t.poster();
			await t.hold(READ);
			const waits = p.getByTestId('run-waits-for');
			await waits.waitFor({ timeout: 60_000 });
			await t.say('3', 'Every cpu of the pool is reserved', 'The card beside the tasks says what it waits for: every cpu of pool batch is reserved, by the items that are running.');
			await t.scrollTo(waits, 72);
			await t.rest();
			await p.getByTestId('run-waits-reserved').filter({ hasText: '4 of 4' }).waitFor({ timeout: 30_000 });
			await t.hold(READ + 800);
			await t.scrollTo(tasksCard(p), 72);
			await t.say('4', 'As an item ends, the next one starts', 'An item finishes and frees its reservation, and a waiting item starts at once.');
			await status(p, 'Succeeded');
			await t.say('5', 'It waited, then ran', 'Nothing was retried and nothing was lost: each item waited for room, then ran.');
			await t.hold(READ);
		},
	},
	{
		name: 'video-yaml-to-run',
		title: 'From YAML to a running Orkflow',
		alt: 'A screen recording of the Orkflow editor on greet: a key is added with the suggestions, the engine flags a needs entry that names no task and the fix clears it, the panel beside the code lists the shell activity’s keys, a new version is applied, a plan shows which tasks would run, and the run is submitted and followed until it succeeds.',
		caption: 'Edit, apply, plan and run, then watch the graph fill in.',
		open: async (p) => {
			// Each take makes the same edit, so each starts from the same document,
			// applied again as the next version: a version is immutable.
			const file = path.join(work, 'greet.ork.yaml');
			const source = readFileSync(path.join(repo, 'site', 'media', 'orkflows', 'greet.ork.yaml'), 'utf8');
			writeFileSync(file, source.replace(/^version: .*$/m, `version: "${nextVersion('greet').text}"`));
			orkctl('apply', file);
			await p.goto(orkflow('greet', '&tab=edit'));
			await p.getByTestId('editor-docs').waitFor();
			await p.locator('.cm-line').first().waitFor();
		},
		act: async (t, p) => {
			const version = nextVersion('greet');
			await t.say('1', 'Ctrl Space suggests keys and values', 'Edit an Orkflow in the editor. Ctrl Space suggests keys and values.');
			await revealLine(p, editorLine(p, '      - hello'));
			await t.hold(1200);
			await t.pressLine(editorLine(p, '    uses: shell').last(), 300);
			await t.rest();
			await t.key('End');
			await t.key('Enter');
			await t.type('net');
			await t.key('Control+Space', 900);
			await t.key('Enter', 500);
			await t.key('Control+Space', 900);
			await t.key('ArrowDown', 400);
			await t.key('Enter', 700);
			await t.say('2', 'The engine checks as you type', 'The engine checks the document as you type. A problem is underlined; fix it to apply.');
			await t.pressLine(editorLine(p, '      - hello'), 200);
			await t.key('End');
			await t.type('x');
			const problem = p.getByTestId('editor-yaml-first-problem');
			await problem.waitFor({ timeout: 30_000 });
			await t.scrollWindow(120);
			await t.hold(READ);
			await t.pressLine(editorLine(p, '      - hellox'), 200);
			await t.rest();
			await t.key('End');
			await t.key('Backspace');
			await problem.waitFor({ state: 'detached', timeout: 30_000 });
			await t.say('3', 'The activity’s keys, beside the code', 'The panel beside the code shows the keys of the activity the cursor is in.');
			await t.glide(p.getByTestId('editor-docs-keys'));
			await t.hold(READ);
			await t.say('4', 'A new version, then a plan', 'Apply version stores an immutable version and its digest. A plan shows which tasks would run.');
			await revealLine(p, p.locator('.cm-line').filter({ hasText: /^version: / }));
			await t.pressLine(p.locator('.cm-line').filter({ hasText: /^version: / }), 200);
			await t.rest();
			await t.key('End');
			await t.key('ArrowLeft');
			for (let i = 0; i < String(version.from).length; i++) await t.key('Backspace', 80);
			await t.type(String(version.to));
			await t.hold(700);
			await t.press(p.getByTestId('editor-apply'));
			await p.getByTestId('orkflow-submit-run').waitFor({ timeout: 30_000 });
			await t.hold(1200);
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-plan'));
			await p.getByTestId('run-plan-summary').waitFor({ timeout: 30_000 });
			await t.hold(READ);
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await t.say('5', 'Watch the graph fill in', 'Submit a run, and watch the graph fill in as each task starts and ends.');
			await t.rest();
			await status(p, 'Succeeded');
			await t.hold(1200);
			t.poster();
			await t.hold(READ);
		},
	},
	{
		name: 'video-why-failed',
		title: 'Find why a run failed',
		alt: 'A screen recording of a failed run of order-saga: the banner names shipped and its reason, View stderr opens the task’s own output, the side cards say how it failed and how often, the graph shows its failure handlers ran, and Run again opens with the run’s inputs.',
		caption: 'Open the failed task, read its stderr and how it failed, then run it again.',
		open: async (p) => {
			failureHistory();
			const id = submitSaga(REFUSED_ORDER);
			finished(id);
			await p.goto(run(id));
			await status(p, 'Failed');
			await p.getByTestId('run-failure-banner').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'The banner names the task and the reason', 'order-saga failed. The banner names the task and the reason, as a code and in words.');
			await t.glide(p.getByTestId('run-failure-banner'), true);
			t.poster();
			await t.hold(READ);
			await t.say('2', 'View stderr opens its own output', 'View stderr opens the failed task’s own output: the API answered 503.');
			await t.press(p.getByTestId('run-failure-view-stderr'), 0);
			await t.rest();
			await p.getByTestId('task-log-shipped').getByText('the shipment was answered HTTP 503').first().waitFor({ timeout: 30_000 });
			await t.hold(READ);
			await t.say('3', 'How it failed, and how often', 'How it failed lists what ran and what never started. Seen before counts failures like it.');
			await t.scrollTo(p.getByTestId('run-how-failed'), 72);
			await t.glide(p.getByTestId('run-how-failed'));
			await t.hold(READ);
			await t.say('4', 'The handlers ran; the run still failed', 'The failure handlers released and refunded the order. The run still ends Failed.');
			await t.scrollWindow(0);
			await t.glide(p.getByTestId('run-dag-node-release'));
			await t.hold(READ);
			await t.say('5', 'Fix the cause, then Run again', 'Fix the cause, then Run again.');
			await t.press(p.getByTestId('run-again'));
			await p.getByTestId('run-again-dialog').waitFor();
			await t.hold(READ);
		},
	},
	// ---- The docs'.
	{
		name: 'e2e-demo',
		alt: 'A screen recording of the OrkStack UI: the each-fan-out example is applied from the Apply page, its run is planned and submitted, the run page follows it live until it succeeds, and the merge task’s merged.txt artifact is shown with its provenance.',
		caption: 'Apply, plan and submit, watch, find the artifact: one run of examples/each-fan-out.ork.yaml on the local stack.',
		open: async (p) => {
			await p.goto(`${UI}/apply?ns=${NS}`);
			await p.getByTestId('apply-import').waitFor({ state: 'attached' });
		},
		act: async (t, p) => {
			await t.say('1', 'Apply an Orkflow');
			await t.hold(1200);
			await t.glide(p.getByText('Import YAML'));
			await p.getByTestId('apply-import').setInputFiles(path.join(repo, 'examples', 'each-fan-out.ork.yaml'));
			await t.hold(READ);
			await t.press(p.getByTestId('apply-submit'));
			await p.getByTestId('orkflow-submit-run').waitFor();
			await t.hold(READ);
			await t.say('2', 'Plan it, then submit a run');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-plan'));
			await p.getByTestId('run-plan-summary').waitFor({ timeout: 30_000 });
			await t.hold(READ + 400);
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await t.rest();
			await t.say('3', 'Watch it run, live');
			await status(p, 'Succeeded');
			await t.hold(2600);
			await t.say('4', 'Open what it wrote');
			await t.press(p.getByTestId('task-deep-link-merge'));
			await t.press(p.getByTestId('task-inspect-toggle-merge'));
			await p.getByTestId('task-artifacts-merge').waitFor();
			await t.scrollTo(p.getByTestId('task-artifacts-merge'), 260);
			await t.press(p.getByTestId('artifact-provenance-toggle-merge-merged.txt'));
			await t.hold(READ + 1000);
		},
	},
	{
		name: 'ui-editor',
		alt: 'A screen recording of the Orkflow editor: a task’s needs is changed to name a task that does not exist, the engine’s finding appears at that line and in the strip under the code, the problems list opens, and the fix clears it.',
		caption: 'The editor asks the engine as you type: an unknown task in needs is flagged at its line, and fixing it clears the problem.',
		open: async (p) => {
			await p.goto(orkflow('each-fan-out', '&tab=edit'));
			await p.getByTestId('editor-docs').waitFor();
			await revealLine(p, editorLine(p, '      - list'));
		},
		act: async (t, p) => {
			await t.say('1', 'Name a task that does not exist');
			await t.hold(1200);
			await t.pressLine(editorLine(p, '      - list'), 200);
			await t.rest();
			await t.key('End');
			await t.type('s');
			const problem = p.getByTestId('editor-yaml-first-problem');
			await problem.waitFor({ timeout: 30_000 });
			await t.say('2', 'The engine flags it, at its line');
			await t.scrollWindow(120);
			await t.hold(READ);
			await t.say('3', 'The list says what is wrong');
			await t.press(p.getByTestId('editor-problems-chip'));
			await p.getByTestId('editor-validation-issue').first().waitFor({ timeout: 30_000 });
			await t.hold(READ);
			await t.say('4', 'Fix it, and the problem clears');
			await t.pressLine(editorLine(p, '      - lists'), 200);
			await t.rest();
			await t.key('End');
			await t.key('Backspace');
			await problem.waitFor({ state: 'detached', timeout: 30_000 });
			await t.hold(READ);
		},
	},
	{
		name: 'ui-fanout-run',
		alt: 'A screen recording of a run of each-fan-out: list finishes, convert runs its four items two at a time, merge follows, and one item is opened to show its log and its card.',
		caption: 'A fan-out runs one durable item per element, two at a time here; each item has its own attempts and log.',
		open: async (p) => {
			await p.goto(orkflow('each-fan-out'));
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('', 'One task, fanned out over a list');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await t.rest();
			await p.getByTestId('task-convert-status').filter({ hasText: 'Running' }).waitFor({ timeout: 120_000 });
			await t.say('', 'Each item is its own durable activity, two at a time');
			await t.scrollTo(tasksCard(p), 72);
			await p.getByTestId('task-convert-status').filter({ hasText: 'Succeeded' }).waitFor({ timeout: 180_000 });
			// Once the run has ended its page stops redrawing the items.
			await status(p, 'Succeeded');
			await t.hold(1600);
			await t.say('', 'Open one: its attempts, its timing, its log');
			await t.press(p.getByTestId('task-deep-link-convert'));
			await t.press(p.getByTestId('task-convert-item-2'));
			await p.getByTestId('task-convert-item-panel').waitFor();
			await p.getByTestId('task-log-convert').getByText('converted').first().waitFor({ timeout: 30_000 });
			await t.hold(READ + 800);
		},
	},
	{
		name: 'ui-await-run',
		alt: 'A screen recording of an awaited task: approval-gate is submitted with a change to the payments service, the run waits at approval, the resolve dialog is opened, approved is checked and an approver typed, and once it is completed the next task runs and the run succeeds.',
		caption: 'An awaited task holds the run until a person or a system resolves it; completing it starts the next task.',
		open: async (p) => {
			await p.goto(orkflow('approval-gate'));
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'Submit a change that needs an approval');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-input-json-mode'), 300);
			await p.getByTestId('run-submit-inputs-json').fill(JSON.stringify(gateInput(), null, 2));
			await t.hold(READ);
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await t.rest();
			await p.getByTestId('task-approval-status').filter({ hasText: 'Awaiting' }).waitFor({ timeout: 120_000 });
			await t.say('2', 'It waits, with no container, until resolved');
			await t.scrollTo(tasksCard(p), 72);
			await t.hold(READ);
			await t.say('3', 'Resolve it with the values it asks for');
			await t.press(p.getByTestId('task-approval-resolve'));
			await p.getByTestId('await-resolve-dialog').waitFor();
			await t.press(p.getByTestId('declared-input-approved'));
			await t.press(p.getByTestId('declared-input-approver'), 200);
			await t.type('example-approver');
			await t.hold(1800);
			await t.press(p.getByTestId('await-resolve-submit'), 0);
			await t.rest();
			await t.say('4', 'The next task starts');
			await status(p, 'Succeeded', 120_000);
			await t.hold(READ);
		},
	},
	{
		name: 'ui-failure',
		alt: 'A screen recording of order-saga submitted with an order whose shipment the API refuses: shipped fails, the release, refund, report and notify failure handlers run, and the run ends Failed with shipped’s reason, whose failure block and log are then shown.',
		caption: 'The API refuses the shipment: shipped fails, the failure handlers release and refund the order, and the run still ends Failed with shipped’s reason.',
		open: async (p) => {
			await p.goto(orkflow('order-saga'));
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'An order the API will refuse to ship');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-input-json-mode'), 300);
			await p.getByTestId('run-submit-inputs-json').fill(JSON.stringify(sagaInput(REFUSED_ORDER), null, 2));
			await t.hold(READ);
			await t.press(p.getByTestId('run-submit-submit'), 0);
			await p.getByTestId('run-drawing').waitFor();
			await t.rest();
			await p.getByTestId('task-ship-status').filter({ hasText: 'Running' }).waitFor({ timeout: 120_000 });
			await t.say('2', 'The shipment is refused, and shipped fails');
			await p.getByTestId('task-shipped-status').filter({ hasText: 'Failed' }).waitFor({ timeout: 180_000 });
			await t.say('3', 'Failure handlers run, and the run still fails');
			await status(p, 'Failed');
			await t.hold(READ + 400);
			await t.say('4', 'The failed task, with its error and log');
			await t.scrollTo(p.getByTestId('task-shipped'), 72);
			await p.getByTestId('task-log-shipped').getByText('the shipment was answered HTTP 503').first().waitFor({ timeout: 30_000 });
			await t.hold(READ + 800);
		},
	},
];

/** One take of a script in one theme, encoded into public/media. Returns its length in seconds. */
async function record(browser: Browser, script: Script, theme: Theme): Promise<number> {
	const ctx = await browser.newContext({ viewport: REC, deviceScaleFactor: 2, colorScheme: theme });
	await stage(ctx);
	const p = await ctx.newPage();
	await script.open(p);
	const base = `${script.name}-${theme}`;
	const take = new Take(p, path.join(work, base), REC);
	try {
		await take.roll();
		try {
			await script.act(take, p);
		} catch (err) {
			// What the page showed when the take failed, for whoever reads the error.
			const shot = path.join(process.env.TMPDIR ?? tmpdir(), `site-media-${base}-failed.png`);
			await p.screenshot({ path: shot }).catch(() => {});
			console.error(`  ${base} failed; its last screen is ${shot}`);
			throw err;
		} finally {
			// The script's own failure is the one to report; the camera's
			// comes second.
			await take.cut().catch((err) => console.error(`  ${base}: the camera failed too:`, err));
		}
	} finally {
		await ctx.close();
	}
	return encode(take, base);
}

/**
 * A take, encoded: a lossless master, then H.264 MP4 at 1600 pixels wide, which
 * keeps the UI's text sharp at about 60% of the bytes of the capture's own size,
 * its poster frame, and its captions.
 * (A VP9 WebM of these takes is no smaller and takes twice as long.)
 */
function encode(take: Take, base: string): number {
	const out = (ext: string) => path.join(publicMedia, `${base}.${ext}`);
	const film = take.film();
	rmSync(out('webm'), { force: true });
	const size = ['-vf', 'scale=1600:-2:flags=lanczos'];
	const master = path.join(work, `${base}.mkv`);
	ffmpeg(...film.inputs, '-filter_complex', film.filter, '-map', '[v]', '-t', film.seconds.toFixed(3), '-c:v', 'libx264rgb', '-qp', '0', '-preset', 'ultrafast', master);
	// A screen recording is mostly still frames: a keyframe every 30 s, and
	// none at a page change. CRF 24 keeps text sharp; a take that comes out
	// over the site's video budget (scripts/check-tokens.mjs, 2.5 MiB) is
	// encoded again a step coarser, until it fits.
	for (let crf = 24; ; crf++) {
		ffmpeg('-i', master, ...size, '-c:v', 'libx264', '-crf', String(crf), '-preset', 'medium', '-tune', 'animation', '-x264-params', 'keyint=900:scenecut=0', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', out('mp4'));
		const bytes = statSync(out('mp4')).size;
		if (bytes <= 2.4 * 1024 * 1024) break;
		if (crf >= 30) throw new Error(`${base}.mp4 is ${(bytes / 1048576).toFixed(1)} MiB even at CRF 30: shorten the take`);
	}
	const at = take.posterSeconds();
	ffmpeg(...(at === undefined ? ['-sseof', '-0.2'] : ['-ss', at.toFixed(3)]), '-i', master, ...size, '-frames:v', '1', '-c:v', 'libwebp', '-quality', '82', out('webp'));
	writeFileSync(out('vtt'), take.captions());
	rmSync(master, { force: true });
	console.log(`  ${base} (${film.seconds.toFixed(1)} s, ${(statSync(out('mp4')).size / 1048576).toFixed(2)} MiB)`);
	return film.seconds;
}

// Screenshots nothing shows any more.
if (!partial) {
	for (const stale of ['ui-run-hero', 'ui-run-page']) {
		for (const theme of themes) for (const suffix of ['', '-2x']) rmSync(path.join(publicMedia, `${stale}-${theme}${suffix}.webp`), { force: true });
	}
}
await main();
