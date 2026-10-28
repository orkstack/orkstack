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
import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import sharp from 'sharp';
import { startBrowserServer, stopBrowserServer } from '../../ui/e2e/browser-server';

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
const items: Record<string, Record<string, string>> = {};

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

async function main() {
	mkdirSync(assets, { recursive: true });
	mkdirSync(publicMedia, { recursive: true });
	mkdirSync(work, { recursive: true });

	// The namespace and the two Orkflows. An apply of bytes already stored,
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

	const server = startBrowserServer(`site-media-${process.pid}`);
	let browser: Browser | undefined;
	try {
		browser = await chromium.connect(server.wsEndpoint, { exposeNetwork: '<loopback>' });

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

		// The end-to-end recording, in each theme: apply from the UI, submit,
		// watch the run, open the merged artifact's provenance. It feeds the
		// README's animation (media/run.sh).
		for (const theme of themes) {
			const dir = path.join(work, `video-${theme}`);
			const rec = await browser.newContext({
				viewport: { width: 1280, height: 800 },
				colorScheme: theme,
				recordVideo: { dir, size: { width: 1280, height: 800 } },
			});
			const p = await rec.newPage();
			await p.goto(`${UI}/apply?ns=${NS}`);
			await p.getByTestId('apply-example-use-each-fan-out').click();
			await p.waitForTimeout(800);
			await p.getByTestId('apply-submit').click();
			await p.getByTestId('orkflow-name').waitFor();
			await p.waitForTimeout(800);
			await p.getByTestId('orkflow-submit-run').click();
			await p.getByTestId('run-submit-submit').click();
			await p.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 });
			await p.waitForTimeout(1000);
			await p.getByTestId('task-inspect-toggle-merge').click();
			await p.getByTestId('task-artifacts-merge').scrollIntoViewIfNeeded();
			await p.waitForTimeout(600);
			await p.getByTestId('artifact-provenance-toggle-merge-merged.txt').click();
			await p.waitForTimeout(2500);
			const video = p.video();
			await rec.close();
			// Over a browser-server connection the recording lives on the
			// server; saveAs copies it here (path() is local-only).
			const raw = path.join(work, `video-${theme}.webm`);
			await video!.saveAs(raw);
			encode(raw, `e2e-demo-${theme}`);
		}
		items['e2e-demo'] = {
			alt: 'A screen recording: the each-fan-out example applied from the UI’s Apply page, a run submitted and watched to the end, and its merged artifact opened.',
			caption: 'Apply, submit, watch, open the artifact: one run of examples/each-fan-out.ork.yaml on the local stack.',
			webm: 'e2e-demo-light.webm',
			mp4: 'e2e-demo-light.mp4',
			poster: 'e2e-demo-light.webp',
			webmDark: 'e2e-demo-dark.webm',
			mp4Dark: 'e2e-demo-dark.mp4',
			posterDark: 'e2e-demo-dark.webp',
		};
	} finally {
		await browser?.close().catch(() => {});
		stopBrowserServer(server.container);
	}

	// The terminal recordings (cli-*) are media/finish.mjs's and stay; every
	// other item is this script's, so one it no longer takes goes.
	const previous = (() => {
		try {
			return JSON.parse(readFileSync(path.join(assets, 'manifest.json'), 'utf8'));
		} catch {
			return { items: {} };
		}
	})();
	const kept = Object.fromEntries(Object.entries(previous.items ?? {}).filter(([name]) => name.startsWith('cli-')));
	const commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
	writeFileSync(
		path.join(assets, 'manifest.json'),
		JSON.stringify(
			{
				$comment: 'Written by make site-media (site/media/). Do not edit.',
				sourceCommit: commit,
				orkflows: [FANOUT, AWAIT],
				browserImage: server.image,
				items: { ...kept, ...items, ...shots },
				...(previous.vhsImage ? { vhsImage: previous.vhsImage } : {}),
			},
			null,
			'\t',
		) + '\n',
	);
}

/** Re-encode a recording to VP9 WebM and H.264 MP4, and a poster frame. */
function encode(raw: string, base: string): void {
	const out = (ext: string) => path.join(publicMedia, `${base}.${ext}`);
	const ff = (...args: string[]) => execFileSync('ffmpeg', ['-y', '-loglevel', 'error', ...args]);
	ff('-i', raw, '-c:v', 'libvpx-vp9', '-b:v', '0', '-crf', '40', '-row-mt', '1', '-an', out('webm'));
	ff('-i', raw, '-c:v', 'libx264', '-crf', '28', '-preset', 'slow', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-an', out('mp4'));
	const posterPng = path.join(work, `${base}.png`);
	ff('-sseof', '-1.5', '-i', raw, '-frames:v', '1', posterPng);
	execFileSync('node', ['-e', `require('sharp')(${JSON.stringify(posterPng)}).webp({quality:82}).toFile(${JSON.stringify(out('webp'))})`], {
		cwd: path.join(repo, 'site'),
	});
	renameSync(raw, raw + '.done');
	console.log(`  ${base}`);
}

// Screenshots used to live in src/assets/media, where Astro re-encoded them;
// they are served as they are now, from public/media.
for (const stale of ['ui-orkflow', 'ui-run-hero', 'ui-run-finished', 'ui-fanout', 'ui-await']) {
	for (const theme of themes) rmSync(path.join(assets, `${stale}-${theme}.webp`), { force: true });
}
await main();
