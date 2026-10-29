// The recordings' camera and stagehand (make site-media, media/capture.ts).
//
// A recording is the page's own pixels at two device pixels per CSS pixel,
// lossless: the viewport captured over and over, each capture kept with the
// instant it was taken, and a capture identical to the one before dropped
// (the earlier frame lasts longer). Chrome's screencast and Playwright's
// recordVideo both draw in CSS pixels, and recordVideo encodes VP8 at
// 1 Mbit/s in real time, so text through them comes out soft.
//
// A capture at scale 2 is a fresh raster, a few a second while the page
// moves, so the cursor is not part of the page: the stagehand records where
// it glides and clicks, and the encode draws it over the frames at 30 a
// second, in the theme's own ink and paper (ffmpeg's overlay). What the
// stagehand does to the page is real: it moves Playwright's mouse along the
// same path, so the UI's hover states show, and it clicks what the cursor
// points at. The caption naming each step is drawn by the page, with the
// UI's tokens, in the top layer above its dialogs.
import type { BrowserContext, Locator, Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import sharp from 'sharp';

/** The pauses a reader needs: to find what changed, and to read a screen. */
export const BEAT = 700;
export const READ = 3200;

/** ffmpeg, quiet, failing loudly. */
export const ffmpeg = (...args: string[]) => execFileSync('ffmpeg', ['-y', '-hide_banner', '-loglevel', 'error', ...args]);

/** The step caption, added to every document the context opens. */
export async function stage(ctx: BrowserContext): Promise<void> {
	await ctx.addInitScript(() => {
		const install = () => {
			if (document.getElementById('demo-caption')) return;
			const style = document.createElement('style');
			style.textContent = `
				#demo-caption { margin: 0; inset: auto; overflow: visible; position: fixed; left: 50%;
					bottom: calc(var(--spacing-titleblock, 52px) + 20px); transform: translateX(-50%);
					pointer-events: none; transition: opacity 240ms;
					padding: 10px 18px; border: var(--rule-region, 2px) solid var(--color-ink);
					background: var(--color-paper); color: var(--color-ink);
					font: 600 15px/1.3 var(--font-sans); letter-spacing: 0.01em; white-space: nowrap; }
				#demo-caption b { font: 600 12px/1.3 var(--font-mono); color: var(--color-ink-muted); margin-right: 10px; }
				#demo-caption:empty { opacity: 0; }`;
			document.head.append(style);
			const caption = document.createElement('div');
			caption.id = 'demo-caption';
			caption.setAttribute('popover', 'manual');
			caption.setAttribute('aria-hidden', 'true');
			document.body.append(caption);
			const raise = () => {
				if (caption.matches(':popover-open')) caption.hidePopover();
				caption.showPopover();
			};
			raise();
			// A modal dialog opened later sits above it; showing it again puts
			// it back on top of the top layer.
			new MutationObserver(raise).observe(document.body, { subtree: true, attributeFilter: ['open'] });
			const saved = (window as unknown as Record<string, unknown>).__demoCaption as string | undefined;
			if (saved) caption.innerHTML = saved;
		};
		if (document.body) install();
		else addEventListener('DOMContentLoaded', install);
	});
}

interface Frame {
	file: string;
	/** When it was captured, in seconds. */
	t: number;
}
interface Glide {
	t0: number;
	t1: number;
	x0: number;
	y0: number;
	x1: number;
	y1: number;
}

/** One recording of one page: the camera, the cursor's path, and the steps. */
export class Take {
	private frames: Frame[] = [];
	private writes: Promise<void>[] = [];
	private glides: Glide[] = [];
	private clicks: number[] = [];
	private at = { x: 0, y: 0 };
	private running = false;
	private loop?: Promise<void>;
	private fault?: unknown;
	/** When the last capture that began after it was asked for began (s). */
	private steadyAt = 0;
	private ended = 0;
	private art: { cursor: string; ring: string } = { cursor: '', ring: '' };

	constructor(
		readonly page: Page,
		private readonly dir: string,
		private readonly size: { width: number; height: number },
	) {
		this.at = { x: size.width * 0.62, y: size.height * 0.55 };
	}

	/** Draw the cursor and click ring in the page's theme, then start the camera. */
	async roll(): Promise<void> {
		rmSync(this.dir, { recursive: true, force: true });
		mkdirSync(this.dir, { recursive: true });
		this.art = { cursor: path.join(this.dir, 'cursor.png'), ring: path.join(this.dir, 'ring.png') };
		// The art alone, on a transparent page: the UI paints its paper on the
		// root and the body, which would otherwise fill the art's corners.
		await this.page.evaluate(() => {
			const box = document.createElement('div');
			box.id = 'demo-art';
			box.setAttribute('popover', 'manual');
			box.style.cssText = 'margin:0;padding:6px;border:0;inset:auto;left:0;top:0;position:fixed;background:transparent;display:flex;gap:6px';
			box.innerHTML =
				'<svg id="demo-art-cursor" width="24" height="24" viewBox="0 0 24 24"><path d="M4 2.5 L19.5 12.3 L12.2 13.4 L8.6 20.6 Z" fill="var(--color-paper)" stroke="var(--color-ink)" stroke-width="1.6" stroke-linejoin="round"/></svg>' +
				'<svg id="demo-art-ring" width="36" height="36" viewBox="0 0 36 36"><circle cx="18" cy="18" r="15" fill="none" stroke="var(--color-blue-strong)" stroke-width="2.5" opacity="0.85"/></svg>';
			const clear = document.createElement('style');
			clear.id = 'demo-art-clear';
			clear.textContent = 'html, body { background: transparent !important; } html { visibility: hidden; } #demo-art { visibility: visible; }';
			document.head.append(clear);
			document.body.append(box);
			box.showPopover();
		});
		await this.page.locator('#demo-art-cursor').screenshot({ path: this.art.cursor, omitBackground: true });
		await this.page.locator('#demo-art-ring').screenshot({ path: this.art.ring, omitBackground: true });
		await this.page.evaluate(() => {
			document.getElementById('demo-art')?.remove();
			document.getElementById('demo-art-clear')?.remove();
		});
		for (const file of [this.art.cursor, this.art.ring]) {
			const { hasAlpha } = await sharp(file).metadata();
			const corner = await sharp(file).ensureAlpha().extract({ left: 0, top: 0, width: 1, height: 1 }).raw().toBuffer();
			if (!hasAlpha || corner[3] !== 0) throw new Error(`${file} is not drawn on a transparent ground`);
		}
		await this.page.mouse.move(this.at.x, this.at.y);

		const cdp = await this.page.context().newCDPSession(this.page);
		this.running = true;
		let last = '';
		const viewport = async () => {
			const { cssVisualViewport: vp } = await cdp.send('Page.getLayoutMetrics');
			return { x: vp.pageX, y: vp.pageY };
		};
		this.loop = (async () => {
			while (this.running) {
				const before = Date.now() / 1000;
				// A clip is in document coordinates, so it follows the window's
				// scroll: a capture during which the window scrolled is cut from
				// two places, and is dropped.
				const from = await viewport();
				const clip = { ...from, width: this.size.width, height: this.size.height, scale: 2 };
				const { data } = await cdp.send('Page.captureScreenshot', { format: 'png', optimizeForSpeed: true, clip });
				const to = await viewport();
				if (to.x !== from.x || to.y !== from.y) continue;
				this.steadyAt = before;
				if (data === last) continue;
				last = data;
				const file = path.join(this.dir, `f${String(this.frames.length).padStart(6, '0')}.png`);
				this.frames.push({ file, t: (before + Date.now() / 1000) / 2 });
				this.writes.push(writeFile(file, Buffer.from(data, 'base64')));
			}
		})()
			.catch((err) => {
				this.fault = err;
				this.running = false;
			})
			.finally(() => cdp.detach().catch(() => {}));
		// The first frame is on disk before anything happens.
		while (this.frames.length === 0) {
			if (this.fault) throw this.fault;
			await this.page.waitForTimeout(50);
		}
	}

	/** Wait until the camera has taken a picture that began after now. */
	private async exposed(): Promise<void> {
		const asked = Date.now() / 1000;
		while (this.steadyAt < asked) {
			if (this.fault) throw this.fault;
			await this.page.waitForTimeout(25);
		}
	}

	/** Name the step on screen ("2", "Submit a run"), or clear it. */
	async say(step: string, words = ''): Promise<void> {
		await this.page.evaluate(
			([s, t]) => {
				const esc = (v: string) => v.replace(/&/g, '&amp;').replace(/</g, '&lt;');
				const html = t ? `${s ? `<b>${esc(s)}</b>` : ''}${esc(t)}` : '';
				(window as unknown as Record<string, unknown>).__demoCaption = html;
				const el = document.getElementById('demo-caption');
				if (el) el.innerHTML = html;
			},
			[step, words],
		);
	}

	async hold(ms: number): Promise<void> {
		await this.page.waitForTimeout(ms);
	}

	/**
	 * Scroll the window to y in a few eased steps, each one photographed: a
	 * capture takes a few hundred milliseconds at scale 2, so the camera sets
	 * the pace rather than a smooth scroll it would only catch mid-way.
	 */
	async scrollWindow(y: number): Promise<void> {
		const from = await this.page.evaluate(() => window.scrollY);
		const to = await this.page.evaluate((v) => Math.max(0, Math.min(v, document.documentElement.scrollHeight - innerHeight)), y);
		if (Math.abs(to - from) < 2) return;
		const steps = 3;
		for (let i = 1; i <= steps; i++) {
			const s = i / steps;
			const e = s * s * (3 - 2 * s);
			await this.page.evaluate((v) => window.scrollTo({ top: v, behavior: 'instant' }), Math.round(from + (to - from) * e));
			await this.exposed();
		}
		await this.page.waitForTimeout(300);
	}

	/** Scroll the window so a target's top sits `margin` below the viewport's top. */
	async scrollTo(target: Locator, margin = 72): Promise<void> {
		const top = await target.evaluate((e) => e.getBoundingClientRect().top + window.scrollY);
		await this.scrollWindow(top - margin);
		await this.page.waitForTimeout(500);
	}

	/** The target's box once it has stopped moving (a dialog opening, a scroll settling). */
	private async settledBox(target: Locator) {
		await target.waitFor({ state: 'visible' });
		let before = await target.boundingBox();
		for (let i = 0; i < 40; i++) {
			await this.page.waitForTimeout(60);
			const now = await target.boundingBox();
			if (before && now && before.x === now.x && before.y === now.y && before.width === now.width && before.height === now.height) return now;
			before = now;
		}
		if (!before) throw new Error(`the stagehand cannot see ${target}`);
		return before;
	}

	/**
	 * Glide the cursor to a target: its middle, or 48 CSS pixels into a wide
	 * one, and return that point in the viewport. A target out of view is
	 * scrolled to first. With `under`, the cursor only points: its tip rests
	 * just below the target's start, so it covers none of the words it points
	 * at.
	 */
	async glide(target: Locator, under = false): Promise<{ x: number; y: number }> {
		let box = await this.settledBox(target);
		if (box.y < 56 || box.y + box.height > this.size.height - 64) {
			const top = await target.evaluate((e) => e.getBoundingClientRect().top + window.scrollY);
			await this.scrollWindow(top - (this.size.height - box.height) / 2);
			// A target inside a scrolling box of its own comes into view there.
			await target.scrollIntoViewIfNeeded();
			box = await this.settledBox(target);
		}
		const aim = (b: { x: number; y: number; width: number; height: number }) =>
			under ? { x: b.x + 10, y: b.y + b.height + 3 } : { x: b.x + Math.min(b.width / 2, 48), y: b.y + b.height / 2 };
		let point = aim(box);
		await this.glideTo(point.x, point.y);
		// It may have moved while the cursor travelled: follow it.
		const now = await this.settledBox(target);
		if (Math.abs(now.x - box.x) > 1 || Math.abs(now.y - box.y) > 1) {
			point = aim(now);
			await this.glideTo(point.x, point.y);
		}
		return point;
	}

	async glideTo(x: number, y: number): Promise<void> {
		const distance = Math.hypot(x - this.at.x, y - this.at.y);
		const ms = Math.min(1100, Math.max(380, distance * 1.4));
		const t0 = Date.now() / 1000;
		const steps = Math.max(8, Math.round(ms / 40));
		for (let i = 1; i <= steps; i++) {
			const s = i / steps;
			const e = s * s * (3 - 2 * s);
			await this.page.mouse.move(this.at.x + (x - this.at.x) * e, this.at.y + (y - this.at.y) * e);
			await this.page.waitForTimeout(ms / steps);
		}
		this.glides.push({ t0, t1: Date.now() / 1000, x0: this.at.x, y0: this.at.y, x1: x, y1: y });
		this.at = { x, y };
		await this.page.waitForTimeout(240);
	}

	/**
	 * Glide to a target and click it where the cursor points, then give the
	 * reader a beat. The click is Playwright's, so it waits for the target to
	 * be enabled and to receive it; its position is relative to the target's
	 * padding box, so the border is taken off the point the cursor shows.
	 */
	async press(target: Locator, after = BEAT): Promise<void> {
		const { x, y } = await this.glide(target);
		const box = await target.boundingBox();
		const edge = await target.evaluate((e) => ({ left: e.clientLeft, top: e.clientTop }));
		if (!box) throw new Error(`the stagehand cannot see ${target}`);
		this.clicks.push(Date.now() / 1000);
		await target.click({ position: { x: x - box.x - edge.left, y: y - box.y - edge.top } });
		await this.page.waitForTimeout(after);
	}

	/** Type at a typist's pace into what has focus. */
	async type(text: string): Promise<void> {
		await this.page.keyboard.type(text, { delay: 90 });
	}

	/** Stop the camera; a failure it met on the way is thrown here. */
	async cut(): Promise<void> {
		this.running = false;
		await this.loop;
		this.ended = Date.now() / 1000;
		await Promise.all(this.writes);
		if (this.fault) throw this.fault;
	}

	/**
	 * The frames as an ffmpeg concat list, each lasting until the next, and
	 * the filter that draws the cursor and its click rings over them at 30
	 * frames a second, in device pixels and in RGB (the encode converts once).
	 * Inputs: 0 the list, 1 the cursor, 2 the ring.
	 */
	film(): { inputs: string[]; filter: string; seconds: number } {
		const { frames, ended } = this;
		if (frames.length === 0 || !ended) throw new Error('cut the take before filming it');
		// Each glide nests two levels of ffmpeg's expression, which parses at
		// most about a hundred.
		if (this.glides.length > 40) throw new Error(`a take of ${this.glides.length} glides is too many for one cursor expression (at most 40)`);
		const start = frames[0].t;
		const lines = ['ffconcat version 1.0'];
		for (const [i, f] of frames.entries()) {
			const next = i + 1 < frames.length ? frames[i + 1].t : ended;
			lines.push(`file '${path.basename(f.file)}'`, `duration ${Math.max(0.001, next - f.t).toFixed(4)}`);
		}
		lines.push(`file '${path.basename(frames[frames.length - 1].file)}'`);
		const list = path.join(this.dir, 'frames.ffconcat');
		writeFileSync(list, lines.join('\n') + '\n');

		// The cursor's position as a function of t, in device pixels; the
		// arrow's tip is 4,2.5 CSS pixels into its art.
		const tip = { x: 4 * 2, y: 2.5 * 2 };
		const rel = (t: number) => (t - start).toFixed(3);
		// Before the first glide the cursor rests where it started; each glide
		// is an eased line, and its end holds until the next one starts.
		const chain = (k: 'x' | 'y') => {
			const gs = this.glides;
			let expr = ((gs.length ? gs[gs.length - 1][`${k}1`] : this.at[k]) * 2).toFixed(1);
			for (let i = gs.length - 1; i >= 0; i--) {
				const g = gs[i];
				const a = rel(g.t0);
				const b = rel(g.t1);
				const p0 = (g[`${k}0`] * 2).toFixed(1);
				const p1 = (g[`${k}1`] * 2).toFixed(1);
				const s = `clip((t-${a})/(${b}-${a}),0,1)`;
				expr = `if(lt(t,${b}),${p0}+(${p1}-${p0})*${s}*${s}*(3-2*${s}),${expr})`;
				expr = `if(lt(t,${a}),${p0},${expr})`;
			}
			return expr;
		};
		const x = `${chain('x')}-${tip.x}`;
		const y = `${chain('y')}-${tip.y}`;
		const rings = this.clicks.map((c) => `between(t,${rel(c)},${rel(c + 0.36)})`).join('+') || '0';
		const filter = [
			`[0:v]fps=30,format=rgb24[bg]`,
			`[bg][2:v]overlay=x='${chain('x')}-36':y='${chain('y')}-36':eval=frame:format=rgb:shortest=1:enable='${rings}'[r]`,
			`[r][1:v]overlay=x='${x}':y='${y}':eval=frame:format=rgb:shortest=1,format=rgb24[v]`,
		].join(';');
		return {
			inputs: ['-f', 'concat', '-safe', '0', '-i', list, '-loop', '1', '-i', this.art.cursor, '-loop', '1', '-i', this.art.ring],
			filter,
			seconds: ended - start,
		};
	}
}
