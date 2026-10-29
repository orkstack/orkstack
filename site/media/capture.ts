// The site's screenshots and its end-to-end recording, taken from the real
// stack by a script, never by hand (make site-media runs it through
// media/run.sh, which first checks the stack is up).
//
// It applies the landing page's example Orkflow and the awaited-approval
// example into the `examples` namespace, runs them, and captures the UI in
// both themes. Then it records one short video: apply from the UI, submit,
// watch the run, open its artifact. Every frame shows example data from
// examples/ and nothing else.
//
// A screenshot is shown at exactly its own size, so it is never resampled
// (ThemeShot.astro, sync-docs.mjs): each is captured twice per theme, at one
// and at two device pixels per CSS pixel, and saved losslessly, and a page
// serves the pair as a srcset. Two viewports, matched to where the site shows
// them:
//   - the landing page's, LANDING.width wide: its drawing frames are that
//     wide inside main's 90rem at the site's reading scale (base.css), so the
//     run page fills a frame one to one;
//   - the docs', DOCS.width wide: the UI's narrowest two-column layout, cut
//     to its main column (the drawing, a task, a dialog), which fits the docs
//     column (starlight.css, --sl-content-width) one to one.
//
// The browser is the UI e2e suite's own: Playwright's browser server in the
// image ui/e2e/browser-image.txt pins by digest, reached over its connection
// with the host's loopback exposed.
import { chromium, type Browser, type BrowserContext, type Locator, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import sharp from 'sharp';
import { startBrowserServer, stopBrowserServer } from '../../ui/e2e/browser-server';
import { ffmpeg, READ, stage, Take } from './demo';

const repo = path.resolve(import.meta.dirname, '..', '..');
const assets = path.join(repo, 'site', 'src', 'assets', 'media');
const publicMedia = path.join(repo, 'site', 'public', 'media');
const work = path.join(repo, 'site', '.media-work');
// media/run.sh serves the built UI and says where.
const UI = process.env.ORK_UI_URL ?? 'http://127.0.0.1:5173';
const ENGINE = process.env.ORK_SERVER ?? 'http://127.0.0.1:8080';
const NS = 'examples';
const FANOUT = 'examples/each-fan-out.ork.yaml';
const AWAIT = 'examples/awaited-approval.ork.yaml';
const BRANCH = 'examples/branch-and-recover.ork.yaml';
// SITE_MEDIA_TAKES=e2e-demo,ui-editor redoes those recordings alone, keeping
// every screenshot and the other recordings as they are.
const partial = Boolean(process.env.SITE_MEDIA_TAKES?.trim());

const LANDING = { width: 1616, height: 1010 };
const DOCS = { width: 1280, height: 900 };
// The space a docs cut leaves around the UI's main column, each side.
const PAD = 12;
// The UI's sticky top bar (--spacing-bar): a cut below the page's top is
// scrolled to sit under it.
const BAR = 44;

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
	});
}

function submit(name: string): string {
	const out = JSON.parse(orkctl('run', 'submit', name, '--json'));
	const id = out.id ?? out.run?.id ?? out.runId;
	if (!id) throw new Error(`orkctl run submit ${name} --json printed no run id: ${JSON.stringify(out)}`);
	return id;
}

const shots: Record<string, Record<string, string | number>> = {};
const items: Record<string, Record<string, string | number>> = {};

const all = (views: Views, fn: (p: Page) => Promise<unknown>) => Promise.all(views.map((v) => fn(v.page)));

/** Two animation frames: whatever a scroll or a click changed has been laid out and painted. */
const settled = (p: Page) => p.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));

/**
 * Capture one screenshot in every view, cut to the clip each view reports,
 * and save each losslessly as it is: NAME-THEME.webp at one device pixel per
 * CSS pixel, NAME-THEME-2x.webp at two. The four clips must agree, or the
 * pairs would not match.
 */
async function shoot(name: string, alt: string, caption: string, views: Views, clip?: (p: Page) => Promise<Clip>): Promise<void> {
	const clips = await Promise.all(views.map((v) => (clip ? clip(v.page) : Promise.resolve(undefined))));
	const first = clips[0];
	for (const c of clips) {
		if (first && c && (c.x !== first.x || c.y !== first.y || c.width !== first.width || c.height !== first.height)) {
			throw new Error(`${name}: the views disagree on the cut: ${JSON.stringify(clips)}`);
		}
	}
	const pngs = await Promise.all(
		views.map((v, i) => v.page.screenshot({ clip: clips[i], animations: 'disabled', caret: 'hide' })),
	);
	const vp = views[0].page.viewportSize()!;
	const width = first?.width ?? vp.width;
	const height = first?.height ?? vp.height;
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

/**
 * The cut around a part of the run page's main column: the column's width
 * and PAD either side, from the part's top to its bottom with PAD above and
 * below. The part is scrolled to sit under the sticky top bar, and must end
 * above the fixed title block at the bottom of the viewport.
 */
async function columnCut(p: Page, part: Locator): Promise<Clip> {
	await part.scrollIntoViewIfNeeded();
	const top = await part.evaluate((e) => e.getBoundingClientRect().top + window.scrollY);
	await p.evaluate((y) => window.scrollTo(0, y), Math.max(0, top - BAR - PAD * 2));
	await settled(p);
	const box = await part.boundingBox();
	const column = await p.getByTestId('run-drawing').boundingBox();
	const footer = await p.getByTestId('title-block').boundingBox();
	if (!box || !column || !footer) throw new Error('the run page has no main column or title block to measure');
	const clip = {
		x: Math.round(column.x - PAD),
		y: Math.round(box.y - PAD),
		width: Math.round(column.width + PAD * 2),
		height: Math.round(box.height + PAD * 2),
	};
	if (clip.y < BAR || clip.y + clip.height > footer.y) {
		throw new Error(`a cut of ${clip.height}px does not fit between the top bar and the title block (${JSON.stringify(clip)})`);
	}
	return clip;
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

// Live: the fan-out task is running with some of its items done and some
// still to run. (An item runs for a second or two, so the moment to catch is
// the task's, not one item's.)
const live = (p: Page) =>
	p.evaluate(() => {
		const status = document.querySelector('[data-testid="task-convert-status"]')?.textContent?.trim();
		const items = [...document.querySelectorAll<HTMLElement>('[data-testid^="task-convert-item-"][data-status]')].map(
			(e) => e.dataset.status,
		);
		return status === 'RUNNING' && items.includes('SUCCEEDED') && items.some((s) => s === 'PENDING' || s === 'RUNNING');
	});

/**
 * Submit a run of each-fan-out, open it in every view and capture it while
 * its fan-out runs, all four views in the same state. A capture taken across
 * the moment the last item finishes, or a run that ended before it was
 * caught, is thrown away and taken again on a fresh run. Returns the run's id.
 */
async function catchLive(views: Views, capture: () => Promise<void>): Promise<string> {
	for (let tries = 0; tries < 8; tries++) {
		const id = submit('each-fan-out');
		await all(views, (p) => p.goto(`${UI}/runs/${id}?ns=${NS}`));
		const deadline = Date.now() + 60_000;
		let caught = false;
		while (Date.now() < deadline) {
			if ((await Promise.all(views.map((v) => live(v.page)))).every(Boolean)) {
				caught = true;
				break;
			}
			const status = (await views[0].page.getByTestId('run-status').innerText().catch(() => '')).trim();
			if (status && status !== 'RUNNING' && status !== 'PENDING') break;
			await settled(views[0].page);
		}
		if (!caught) continue;
		await capture();
		if ((await Promise.all(views.map((v) => live(v.page)))).every(Boolean)) return id;
	}
	throw new Error('could not capture the fan-out running in every view at once');
}

/**
 * The screenshots: the landing page's, of the run page one to one in its
 * frames, and the docs', of parts of the UI one to one in the docs column.
 */
async function screenshots(browser: Browser): Promise<void> {
	// ---- The landing page: the run page, one to one in its frames.
	const landing = await contexts(browser, LANDING);
	const lv = landing.views;
	// The run page compiles a route on its first visit in some setups; a
	// first run, opened once, keeps that out of the live capture's window.
	const warm = submit('each-fan-out');
	await all(lv, (p) => p.goto(`${UI}/runs/${warm}?ns=${NS}`));
	await all(lv, (p) => p.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 }));

	// The first viewport's view: the run page's top, while its fan-out runs,
	// cut under its heading, its drawing and the aside's timing register.
	await catchLive(lv, () =>
		shoot(
			'ui-run-hero',
			'A run of each-fan-out in the OrkStack UI while it runs: the DAG of list, convert and merge, with list done, convert running and merge waiting, beside the run’s version and timing.',
			'A real run of examples/each-fan-out.ork.yaml, caught while its fan-out runs.',
			lv,
			async (p) => {
				const drawing = await p.getByTestId('run-drawing').boundingBox();
				const timing = await p.getByTestId('task-summary').boundingBox();
				if (!drawing || !timing) throw new Error('the run page has no drawing or timing register to cut under');
				const bottom = Math.max(drawing.y + drawing.height, timing.y + timing.height);
				return { x: 0, y: 0, width: LANDING.width, height: Math.round(bottom + PAD * 2) };
			},
		),
	);

	// The same run, finished: the whole viewport, as a visitor sees it.
	await all(lv, (p) => p.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 }));
	await all(lv, (p) => p.reload());
	await all(lv, (p) => p.getByTestId('run-status').waitFor());
	await all(lv, settled);
	await shoot(
		'ui-run-page',
		'The finished run of each-fan-out in the OrkStack UI: list, convert and merge all SUCCEEDED, the leaf task’s values, and the run’s versions, timing and reproduce commands in the aside.',
		'The same Orkflow’s run, finished.',
		lv,
	);
	for (const c of landing.contexts) await c.close();

	// ---- The docs: parts of the run page, one to one in the docs column.
	const docs = await contexts(browser, DOCS);
	const dv = docs.views;

	// The Orkflow's own page once it is applied: its graph, its versions, and
	// its tasks with each image pinned to a digest.
	await all(dv, (p) => p.goto(`${UI}/orkflows/each-fan-out?ns=${NS}`));
	await all(dv, (p) => p.getByTestId('orkflow-outputs').waitFor());
	await all(dv, settled);
	await shoot(
		'ui-orkflow',
		'The each-fan-out Orkflow’s page in the OrkStack UI after an apply: its graph of list, convert and merge, its stored version, and its three tasks with their image digests.',
		'An applied Orkflow: an immutable version, every image pinned to a digest.',
		dv,
		async (p) => {
			const outputs = await p.getByTestId('orkflow-outputs').boundingBox();
			if (!outputs) throw new Error('the Orkflow page has no outputs section to cut above');
			return { x: 0, y: 0, width: DOCS.width, height: Math.round(outputs.y - PAD) };
		},
	);

	// The run as a graph, while its fan-out runs.
	await catchLive(dv, () =>
		shoot(
			'ui-run-live',
			'The run’s graph in the OrkStack UI while it runs: list done, convert running its items, merge waiting.',
			'A real run of examples/each-fan-out.ork.yaml, caught while its fan-out runs.',
			dv,
			(p) => columnCut(p, p.getByTestId('run-drawing')),
		),
	);
	await all(dv, (p) => p.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 }));
	await all(dv, (p) => p.reload());
	await all(dv, (p) => p.getByTestId('run-status').waitFor());

	// The finished run's last task, inspected: its attempt, its timing,
	// its log and its artifacts.
	await all(dv, (p) => p.getByTestId('task-inspect-toggle-merge').click());
	await all(dv, (p) => p.getByTestId('task-artifacts-merge').waitFor());
	await shoot(
		'ui-run-finished',
		'The merge task of a finished each-fan-out run, inspected in the run page: SUCCEEDED, its attempt and timing, its log and its artifacts, merged.txt among them.',
		'The same Orkflow’s run, finished: a task’s attempts, timing, log and artifacts.',
		dv,
		(p) => columnCut(p, p.getByTestId('task-merge')),
	);

	// One fan-out item: its attempts and its logs.
	await all(dv, (p) => p.getByTestId('task-convert-item-2').click());
	await all(dv, (p) => p.getByTestId('task-convert-item-panel').waitFor());
	await all(dv, (p) => p.getByTestId('task-log-toggle-convert').click());
	await all(dv, (p) => p.getByTestId('task-log-convert').getByText('converted').first().waitFor({ timeout: 30_000 }));
	await shoot(
		'ui-fanout',
		'The convert task’s fan-out in the run page: four items, item 2 opened with its attempt, its timing, its log line and its artifacts.',
		'Each fan-out item is its own durable activity, with its own attempts and logs.',
		dv,
		(p) => columnCut(p, p.getByTestId('task-convert')),
	);

	// An awaited task, being approved: the dialog, with the page around it.
	const awaiting = submit('awaited-approval');
	await all(dv, (p) => p.goto(`${UI}/runs/${awaiting}?ns=${NS}`));
	await all(dv, (p) => p.getByTestId('task-approval-status').filter({ hasText: 'AWAITING' }).waitFor({ timeout: 120_000 }));
	await all(dv, (p) => p.getByTestId('task-approval-resolve').click());
	await all(dv, (p) => p.getByTestId('await-resolve-dialog').waitFor());
	await all(dv, (p) => p.getByTestId('declared-input-approved').check());
	await all(dv, (p) => p.getByTestId('declared-input-approver').fill('example-approver'));
	await all(dv, settled);
	await shoot(
		'ui-await',
		'An awaited task in the run page, with the dialog that resolves it: approved checked, the approver filled in, ready to complete.',
		'An awaited task waits for a person or a system; resolving it starts the next task.',
		dv,
		async (p) => {
			// A band of the dimmed page around it says it is a dialog; a wider
			// one only shows the page's words cut off at the edges.
			const box = await p.getByTestId('await-resolve-dialog').boundingBox();
			if (!box) throw new Error('the resolve dialog has no box');
			const x = Math.max(0, Math.round(box.x - PAD));
			const y = Math.max(0, Math.round(box.y - PAD));
			return {
				x,
				y,
				width: Math.min(DOCS.width - x, Math.round(box.width + PAD * 2)),
				height: Math.min(DOCS.height - y, Math.round(box.height + PAD * 2)),
			};
		},
	);
	const resolver = dv[0].page;
	await resolver.getByTestId('await-resolve-submit').click();
	await resolver.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 120_000 });
	for (const c of docs.contexts) await c.close();
}

async function main() {
	mkdirSync(assets, { recursive: true });
	mkdirSync(publicMedia, { recursive: true });
	mkdirSync(work, { recursive: true });

	// The namespace and the Orkflows. An apply of bytes already stored,
	// against the same image builds, is a no-op. After the images are rebuilt
	// it is refused (a version is immutable and pins its images), and the
	// capture stops with the server's error: capture on a fresh stack then.
	try {
		orkctl('namespace', 'get', NS);
	} catch {
		orkctl('namespace', 'create', NS, '--description', 'The examples/ Orkflows, as the docs run them.');
	}
	orkctl('apply', FANOUT);
	orkctl('apply', AWAIT);
	orkctl('apply', BRANCH);

	const server = startBrowserServer(`site-media-${process.pid}`);
	let browser: Browser | undefined;
	try {
		browser = await chromium.connect(server.wsEndpoint, { exposeNetwork: '<loopback>' });

		if (!partial) await screenshots(browser);

		// The recordings, each in both themes (takes, below).
		const only = process.env.SITE_MEDIA_TAKES?.split(',').map((n) => n.trim()).filter(Boolean);
		const unknown = only?.filter((n) => !takes.some((t) => t.name === n));
		if (unknown?.length) throw new Error(`SITE_MEDIA_TAKES names no recording ${unknown.join(', ')}; there are ${takes.map((t) => t.name).join(', ')}`);
		for (const take of takes) {
			if (only && !only.includes(take.name)) continue;
			for (const theme of themes) await record(browser, take, theme);
			items[take.name] = {
				alt: take.alt,
				caption: take.caption,
				width: REC.width,
				height: REC.height,
				mp4: `${take.name}-light.mp4`,
				poster: `${take.name}-light.webp`,
				mp4Dark: `${take.name}-dark.mp4`,
				posterDark: `${take.name}-dark.webp`,
			};
		}
	} finally {
		await browser?.close().catch(() => {});
		stopBrowserServer(server.container);
	}

	// The terminal recordings (cli-*) are media/finish.mjs's and stay; every
	// other item is this script's, so one it no longer takes goes (unless
	// this run redid only some recordings, which keeps everything else).
	const previous = (() => {
		try {
			return JSON.parse(readFileSync(path.join(assets, 'manifest.json'), 'utf8'));
		} catch {
			return { items: {} };
		}
	})();
	const kept = Object.fromEntries(Object.entries(previous.items ?? {}).filter(([name]) => partial || name.startsWith('cli-')));
	const commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
	writeFileSync(
		path.join(assets, 'manifest.json'),
		JSON.stringify(
			{
				$comment: 'Written by make site-media (site/media/). Do not edit.',
				// A partial run redoes some recordings of an earlier full one.
				sourceCommit: partial ? (previous.sourceCommit ?? commit) : commit,
				orkflows: [FANOUT, AWAIT, BRANCH],
				browserImage: server.image,
				items: { ...kept, ...items, ...shots },
				...(previous.vhsImage ? { vhsImage: previous.vhsImage } : {}),
			},
			null,
			'\t',
		) + '\n',
	);
}

// ---- The recordings: the UI driven by a stagehand (media/demo.ts) at
// 1280×800 CSS pixels, the UI's narrowest two-column layout, captured at two
// device pixels per CSS pixel, and paced for a reader: each screen that
// matters is held about three seconds, every click is announced by the
// cursor gliding to it, and a caption names each step.
const REC = { width: 1280, height: 800 };

interface Script {
	name: string;
	alt: string;
	caption: string;
	/** Load the first page; the camera starts after it. */
	open: (p: Page) => Promise<void>;
	act: (t: Take, p: Page) => Promise<void>;
}

const status = (p: Page, want: string, timeout = 180_000) =>
	p.getByTestId('run-status').filter({ hasText: want }).waitFor({ timeout });

/** The run page's drawing scrolls inside its frame: scroll it smoothly to its end. */
async function scrollDrawing(p: Page): Promise<void> {
	await p.getByTestId('run-drawing').evaluate((root) => {
		const box = [root, ...root.querySelectorAll<HTMLElement>('*')].find((e) => e.scrollHeight > e.clientHeight + 4 && getComputedStyle(e).overflowY !== 'visible');
		box?.scrollTo({ top: box.scrollHeight, behavior: 'smooth' });
	});
	await p.waitForTimeout(1200);
}

/** The line of the editor that reads exactly `text` (leading spaces and all). */
const editorLine = (p: Page, text: string) =>
	p.locator('.cm-line').filter({ hasText: new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) });

const takes: Script[] = [
	{
		name: 'e2e-demo',
		alt: 'A screen recording of the OrkStack UI: the each-fan-out example is applied from the Apply page, its run is planned and submitted, the run page follows it live until it succeeds, and the merge task’s merged.txt artifact is opened with its provenance.',
		caption: 'Apply, plan and submit, watch, open the artifact: one run of examples/each-fan-out.ork.yaml on the local stack.',
		open: async (p) => {
			await p.goto(`${UI}/apply?ns=${NS}`);
			await p.getByTestId('apply-examples').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'Apply an Orkflow');
			await t.hold(1200);
			await t.scrollTo(p.getByTestId('apply-examples'), 160);
			await t.press(p.getByTestId('apply-example-use-each-fan-out'));
			await t.scrollWindow(0);
			await t.hold(READ);
			await t.press(p.getByTestId('apply-submit'));
			await p.getByTestId('orkflow-submit-run').waitFor();
			await t.hold(READ);
			await t.say('2', 'Plan it, then submit a run');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-plan'));
			await p.getByText('Nothing was started').waitFor();
			await t.hold(READ + 400);
			await t.press(p.getByTestId('run-submit-submit'));
			await t.say('3', 'Watch it run, live');
			await status(p, 'SUCCEEDED');
			await t.hold(2600);
			await t.say('4', 'Open what it wrote');
			await t.press(p.getByTestId('task-inspect-toggle-merge'));
			await t.scrollTo(p.getByTestId('task-artifacts-merge'), 260);
			await t.press(p.getByTestId('artifact-provenance-toggle-merge-merged.txt'));
			await t.hold(READ + 1000);
		},
	},
	{
		name: 'ui-editor',
		alt: 'A screen recording of the Apply page’s YAML editor: a task’s needs is changed to name a task that does not exist, the engine’s diagnostic appears at that line and in the problems list, and the fix clears it.',
		caption: 'The editor asks the engine as you type: an unknown task in needs is flagged at its line, and fixing it clears the problem.',
		open: async (p) => {
			await p.goto(`${UI}/apply?ns=${NS}`);
			await p.getByTestId('apply-examples').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'Start from an example');
			await t.scrollTo(p.getByTestId('apply-examples'), 160);
			await t.press(p.getByTestId('apply-example-use-each-fan-out'));
			await t.scrollWindow(0);
			const needs = editorLine(p, '      - list');
			await t.say('2', 'Name a task that does not exist');
			await t.press(needs, 200);
			await p.keyboard.press('End');
			await t.type('s');
			const issue = p.getByTestId('apply-validation-issue').first();
			await issue.waitFor({ timeout: 30_000 });
			await t.say('3', 'The engine flags it, at its line');
			await t.hold(READ);
			await t.glide(issue, true);
			await t.hold(READ);
			await t.say('4', 'Fix it, and the problem clears');
			await t.press(editorLine(p, '      - lists'), 200);
			await p.keyboard.press('End');
			await p.keyboard.press('Backspace');
			await issue.waitFor({ state: 'detached', timeout: 30_000 });
			await t.hold(READ);
		},
	},
	{
		name: 'ui-fanout-run',
		alt: 'A screen recording of a run of each-fan-out: list finishes, convert runs its four items two at a time, merge follows, and one item is opened to show its attempt, timing and log.',
		caption: 'A fan-out runs one durable item per element, two at a time here; each item has its own attempts and log.',
		open: async (p) => {
			await p.goto(`${UI}/orkflows/each-fan-out?ns=${NS}`);
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('', 'One task, fanned out over a list');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-submit'));
			// The convert task's place is known once list has ended and its
			// values have taken their room above it; the page is barely taller
			// than the window then, so the scroll stops where the page ends,
			// with the items in view.
			await p.getByTestId('task-convert-status').filter({ hasText: 'RUNNING' }).waitFor({ timeout: 120_000 });
			await p.getByTestId('task-values-list').waitFor({ timeout: 30_000 });
			await t.hold(400);
			await t.say('', 'Each item is its own durable activity, two at a time');
			await t.scrollTo(p.getByTestId('task-convert'), 120);
			await p.getByTestId('task-convert-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 });
			// Once the run has ended its page stops redrawing the items.
			await status(p, 'SUCCEEDED');
			await t.hold(1600);
			await t.say('', 'Open one: its attempts, its timing, its log');
			await t.press(p.getByTestId('task-convert-item-2'));
			await p.getByTestId('task-convert-item-panel').waitFor();
			await t.press(p.getByTestId('task-log-toggle-convert'));
			await p.getByTestId('task-log-convert').getByText('converted').first().waitFor({ timeout: 30_000 });
			await t.hold(READ + 800);
		},
	},
	{
		name: 'ui-await-run',
		alt: 'A screen recording of an awaited task: the run is submitted and its callback token shown once, the run waits at approval, the resolve dialog is opened, approved is checked and an approver typed, and once it is completed the next task runs and the run succeeds.',
		caption: 'An awaited task holds the run until a person or a system resolves it; completing it starts the next task. Its callback token is shown once, at submit.',
		open: async (p) => {
			await p.goto(`${UI}/orkflows/awaited-approval?ns=${NS}`);
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'Submit a run that needs an approval');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.press(p.getByTestId('run-submit-submit'));
			await p.getByTestId('await-callbacks-continue').waitFor();
			await t.say('2', 'Its callback token is shown once');
			await t.hold(READ);
			await t.press(p.getByTestId('await-callbacks-continue'));
			await p.getByTestId('task-approval-status').filter({ hasText: 'AWAITING' }).waitFor({ timeout: 120_000 });
			await t.say('3', 'It waits, with no container, until resolved');
			await t.scrollTo(p.getByTestId('task-approval'), 120);
			await t.hold(READ);
			await t.say('4', 'Resolve it with the values it asks for');
			await t.press(p.getByTestId('task-approval-resolve'));
			await p.getByTestId('await-resolve-dialog').waitFor();
			await t.press(p.getByTestId('declared-input-approved'));
			await t.press(p.getByTestId('declared-input-approver'), 200);
			await t.type('example-approver');
			await t.hold(1800);
			await t.press(p.getByTestId('await-resolve-submit'));
			await t.say('5', 'The next task starts');
			await status(p, 'SUCCEEDED', 120_000);
			await t.hold(READ);
		},
	},
	{
		name: 'ui-failure',
		alt: 'A screen recording of branch-and-recover submitted with fail_deploy checked in its input form: deploy fails on both of its attempts, the rollback and notify failure handlers run, and the run ends FAILED with deploy’s reason, whose two attempts are then inspected.',
		caption: 'Submitted with fail_deploy set: deploy fails twice, the failure handlers run, and the run still ends FAILED with deploy’s reason.',
		open: async (p) => {
			await p.goto(`${UI}/orkflows/branch-and-recover?ns=${NS}`);
			await p.getByTestId('orkflow-submit-run').waitFor();
		},
		act: async (t, p) => {
			await t.say('1', 'Typed inputs, as a form');
			await t.press(p.getByTestId('orkflow-submit-run'));
			await t.hold(1400);
			await t.press(p.getByTestId('declared-input-fail_deploy'));
			await t.hold(1200);
			await t.press(p.getByTestId('run-submit-submit'));
			await t.say('2', 'deploy fails, and is retried');
			await p.getByTestId('task-deploy').waitFor();
			await t.glide(p.getByTestId('run-drawing'));
			await p.getByTestId('task-deploy-status').filter({ hasText: 'RUNNING' }).waitFor({ timeout: 120_000 });
			await scrollDrawing(p);
			await p.getByTestId('task-deploy-status').filter({ hasText: 'FAILED' }).waitFor({ timeout: 120_000 });
			await t.say('3', 'Failure handlers run, and the run still fails');
			await status(p, 'FAILED', 120_000);
			await t.hold(READ + 400);
			await t.say('4', 'Every attempt, with its error');
			await t.press(p.getByTestId('task-inspect-toggle-deploy'));
			await t.scrollTo(p.getByTestId('task-deploy-attempts'), 200);
			await t.hold(READ + 800);
		},
	},
];

/** One take of a script in one theme, encoded into public/media. */
async function record(browser: Browser, script: Script, theme: Theme): Promise<void> {
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
		} finally {
			// The script's own failure is the one to report; the camera's
			// comes second.
			await take.cut().catch((err) => console.error(`  ${base}: the camera failed too:`, err));
		}
	} finally {
		await ctx.close();
	}
	encode(take, base);
}

/**
 * A take, encoded: a lossless master, then H.264 MP4 at the capture's own
 * size (two device pixels per CSS pixel), and its last frame as the poster.
 * (A VP9 WebM of these takes is no smaller and takes twice as long.)
 */
function encode(take: Take, base: string): void {
	const out = (ext: string) => path.join(publicMedia, `${base}.${ext}`);
	const film = take.film();
	rmSync(out('webm'), { force: true });
	const master = path.join(work, `${base}.mkv`);
	ffmpeg(...film.inputs, '-filter_complex', film.filter, '-map', '[v]', '-t', film.seconds.toFixed(3), '-c:v', 'libx264rgb', '-qp', '0', '-preset', 'ultrafast', master);
	// A screen recording is mostly still frames: a keyframe every 30 s, and
	// none at a page change. CRF 24 keeps text sharp; a take that comes out
	// over the site's video budget (scripts/check-tokens.mjs, 2.5 MiB) is
	// encoded again a step coarser, until it fits.
	for (let crf = 24; ; crf++) {
		ffmpeg('-i', master, '-c:v', 'libx264', '-crf', String(crf), '-preset', 'medium', '-tune', 'animation', '-x264-params', 'keyint=900:scenecut=0', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', out('mp4'));
		const size = statSync(out('mp4')).size;
		if (size <= 2.4 * 1024 * 1024) break;
		if (crf >= 30) throw new Error(`${base}.mp4 is ${(size / 1048576).toFixed(1)} MiB even at CRF 30: shorten the take`);
	}
	ffmpeg('-sseof', '-0.2', '-i', master, '-frames:v', '1', '-c:v', 'libwebp', '-quality', '82', out('webp'));
	console.log(`  ${base} (${film.seconds.toFixed(1)} s)`);
}

// Screenshots used to live in src/assets/media, where Astro re-encoded them;
// they are served as they are now, from public/media.
for (const stale of ['ui-orkflow', 'ui-run-hero', 'ui-run-finished', 'ui-fanout', 'ui-await']) {
	for (const theme of themes) rmSync(path.join(assets, `${stale}-${theme}.webp`), { force: true });
}
await main();
