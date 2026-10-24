// The site's screenshots and its end-to-end recording, taken from the real
// stack by a script, never by hand (make site-media runs it through
// media/run.sh, which first checks the stack is up).
//
// It applies the landing page's example Orkflow and the awaited-approval
// example into the `examples` namespace, runs them, and captures the UI in
// both themes: the run page while the fan-out is running, the same run
// finished, one fan-out item with its attempts and logs, an awaited task
// being approved, and the Orkflow's page. Then it records one short video:
// apply from the UI, submit, watch the run, open its artifact. Every frame
// shows example data from examples/ and nothing else.
//
// The browser is the UI e2e suite's own: Playwright's browser server in the
// image ui/e2e/browser-image.txt pins by digest, reached over its connection
// with the host's loopback exposed.
import { chromium, type Browser, type BrowserContext, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import sharp from 'sharp';
import { startBrowserServer, stopBrowserServer } from '../../ui/e2e/browser-server';

const repo = path.resolve(import.meta.dirname, '..', '..');
const assets = path.join(repo, 'site', 'src', 'assets', 'media');
const publicMedia = path.join(repo, 'site', 'public', 'media');
const work = path.join(repo, 'site', '.media-work');
const UI = process.env.ORK_UI_URL ?? 'http://127.0.0.1:5173';
const ENGINE = process.env.ORK_SERVER ?? 'http://127.0.0.1:8080';
const NS = 'examples';
const FANOUT = 'examples/each-fan-out.ork.yaml';
const AWAIT = 'examples/awaited-approval.ork.yaml';
const themes = ['light', 'dark'] as const;
type Theme = (typeof themes)[number];

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

const manifest: { items: Record<string, Record<string, string>> } = { items: {} };

/** Save a screenshot as lossy WebP at the size the site serves it. */
async function save(name: string, theme: Theme, png: Buffer): Promise<string> {
	const file = `${name}-${theme}.webp`;
	await sharp(png).resize({ width: 2400, withoutEnlargement: true }).webp({ quality: 86, effort: 6 }).toFile(path.join(assets, file));
	return file;
}

async function shotPair(
	name: string,
	alt: string,
	caption: string,
	pages: Record<Theme, Page>,
	take: (p: Page) => Promise<Buffer>,
): Promise<void> {
	const [light, dark] = await Promise.all(themes.map((t) => take(pages[t])));
	manifest.items[name] = {
		alt,
		caption,
		light: await save(name, 'light', light),
		dark: await save(name, 'dark', dark),
	};
	console.log(`  ${name}`);
}

async function contexts(browser: Browser): Promise<Record<Theme, BrowserContext>> {
	const make = (colorScheme: Theme) =>
		browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, colorScheme, reducedMotion: 'reduce' });
	return { light: await make('light'), dark: await make('dark') };
}

async function main() {
	mkdirSync(assets, { recursive: true });
	mkdirSync(publicMedia, { recursive: true });
	rmSync(work, { recursive: true, force: true });
	mkdirSync(work, { recursive: true });

	// The namespace and the two Orkflows. An apply of bytes already stored is
	// a no-op, so this is safe to repeat.
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
		const ctx = await contexts(browser);
		const pages = { light: await ctx.light.newPage(), dark: await ctx.dark.newPage() } as Record<Theme, Page>;
		const both = (fn: (p: Page) => Promise<unknown>) => Promise.all(themes.map((t) => fn(pages[t])));

		// The Orkflow's own page, after apply.
		await both((p) => p.goto(`${UI}/orkflows/each-fan-out?ns=${NS}`));
		await both((p) => p.getByTestId('orkflow-name').waitFor());
		await shotPair(
			'ui-orkflow',
			'The each-fan-out Orkflow in the OrkStack UI: its stored version, digest and tasks, with the button that submits a run.',
			'The Orkflow after apply: an immutable version, pinned by its digest.',
			pages,
			(p) => p.screenshot(),
		);

		// A run, caught while its fan-out is running.
		const live = submit('each-fan-out');
		await both((p) => p.goto(`${UI}/runs/${live}?ns=${NS}`));
		// Both themes must show the fan-out running, in the same capture: the
		// alt text says so. A pair taken across the moment an item finishes is
		// thrown away and taken again, on a fresh run if this one has ended.
		const running = (p: Page) => p.locator('[data-testid^="task-convert-item-"][data-status="RUNNING"]');
		let livePair: Buffer[] | undefined;
		for (let tries = 0; tries < 4 && !livePair; tries++) {
			if (tries > 0 && (await pages.light.getByTestId('run-status').innerText()).trim() !== 'RUNNING') {
				const again = submit('each-fan-out');
				await both((p) => p.goto(`${UI}/runs/${again}?ns=${NS}`));
			}
			await both((p) => running(p).first().waitFor({ timeout: 120_000 }));
			const pair = await Promise.all(themes.map((t) => pages[t].screenshot()));
			const still = await Promise.all(themes.map(async (t) => (await running(pages[t]).count()) > 0));
			if (still.every(Boolean)) livePair = pair;
		}
		if (!livePair) throw new Error('could not capture the fan-out running in both themes at once');
		manifest.items['ui-run-hero'] = {
			alt: 'A run of each-fan-out in the OrkStack UI: the DAG of list, convert and merge, with convert’s items running, beside the run’s version and timing.',
			caption: 'A real run of examples/each-fan-out.ork.yaml, caught while its fan-out runs.',
			light: await save('ui-run-hero', 'light', livePair[0]),
			dark: await save('ui-run-hero', 'dark', livePair[1]),
		};
		console.log('  ui-run-hero');
		manifest.items['ui-run-live'] = { ...manifest.items['ui-run-hero'] };

		// The same run, finished.
		await both((p) => p.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 180_000 }));
		await both((p) => p.reload());
		await both((p) => p.getByTestId('run-status').waitFor());
		await shotPair(
			'ui-run-finished',
			'The finished run of each-fan-out: every task SUCCEEDED, the fan-out’s four items settled, and the run’s timing in the aside.',
			'The same Orkflow’s run, finished: list, four convert items, merge.',
			pages,
			(p) => p.screenshot(),
		);

		// One fan-out item: its attempts and its logs.
		await both((p) => p.getByTestId('task-convert-item-2').click());
		await both((p) => p.getByTestId('task-convert-item-panel').waitFor());
		await both((p) => p.getByTestId('task-log-toggle-convert').click());
		await both((p) => p.getByTestId('task-log-convert').getByText('converted').first().waitFor({ timeout: 30_000 }));
		await both((p) => p.getByTestId('task-convert-item-panel').scrollIntoViewIfNeeded());
		await shotPair(
			'ui-fanout',
			'One item of the convert fan-out opened in the run page: its attempt, its timing and its log lines.',
			'Each fan-out item is its own durable activity, with its own attempts and logs.',
			pages,
			(p) => p.screenshot(),
		);

		// An awaited task, being approved.
		const awaiting = submit('awaited-approval');
		await both((p) => p.goto(`${UI}/runs/${awaiting}?ns=${NS}`));
		await both((p) => p.getByTestId('task-approval-status').filter({ hasText: 'AWAITING' }).waitFor({ timeout: 120_000 }));
		await both((p) => p.getByTestId('task-approval-resolve').click());
		await both((p) => p.getByTestId('await-resolve-dialog').waitFor());
		await both((p) => p.getByTestId('declared-input-approved').check());
		await both((p) => p.getByTestId('declared-input-approver').fill('example-approver'));
		await shotPair(
			'ui-await',
			'An awaited task in the run page, with the dialog that resolves it: approved checked, the approver filled in, ready to complete.',
			'An awaited task waits for a person or a system; resolving it starts the next task.',
			pages,
			(p) => p.screenshot(),
		);
		await pages.light.getByTestId('await-resolve-submit').click();
		await pages.light.getByTestId('run-status').filter({ hasText: 'SUCCEEDED' }).waitFor({ timeout: 120_000 });
		await ctx.light.close();
		await ctx.dark.close();

		// The end-to-end recording, in each theme: apply from the UI, submit,
		// watch the run, open the merged artifact's provenance.
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
		manifest.items['e2e-demo'] = {
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

	const previous = (() => {
		try {
			return JSON.parse(readFileSync(path.join(assets, 'manifest.json'), 'utf8'));
		} catch {
			return { items: {} };
		}
	})();
	const commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repo, encoding: 'utf8' }).trim();
	writeFileSync(
		path.join(assets, 'manifest.json'),
		JSON.stringify(
			{
				$comment: 'Written by make site-media (site/media/). Do not edit.',
				sourceCommit: commit,
				orkflows: [FANOUT, AWAIT],
				browserImage: server.image,
				items: { ...previous.items, ...manifest.items },
			},
			null,
			'\t',
		) + '\n',
	);
	rmSync(work, { recursive: true, force: true });
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

await main();
