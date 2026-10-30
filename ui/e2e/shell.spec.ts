import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
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
import { editorReady } from './yaml';

// The drawing office's shell (UI revamp, stage 1), against a real engine and
// Orker on real Temporal and MinIO:
//
//   modes      Build is current on the dashboard, Runs, an Orkflow and Apply;
//              Operate on every admin page, with its strip; Build's three
//              daily surfaces stay reachable from Operate; links carry ns;
//   palette    Ctrl+K (⌘K on Apple platforms, and only that there) opens it
//              from a page and from inside a text field; the keyboard moves
//              the active option; an admin page, an Orkflow and a run open
//              from it; an unknown run id lands on the run page's own
//              not-found state with no GetRun from the palette; Close and
//              Escape close it and focus returns; it says no banned word;
//   overlays   the shortcut opens nothing over an open dialog with a typed
//              form, or over an open Select, and works once they close;
//   theme      the first frame of a dark-OS visit is already dark; Light
//              survives a reload; System follows the OS live;
//   collapse   at 390 px no page scrolls sideways, and the menu holds the
//              Primary navigation and the namespace picker — once;
//   weight     a first load fetches at most two font files, both latin,
//              within 60 KB, and a stylesheet within 14 KB gzipped;
//   specimen, forced colours, the title block's default cells, the skip
//   link, and axe (WCAG 2.2 AA) over the shell's own regions.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18911';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-shell-${STAMP}`;
const BUCKET = ownBucket('shell');
const FLOW = `shell-hello-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'shell')
	: path.join(uiDir, 'test-results', 'shell');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// The words no surface of the shell may say (ui/test/paletteVocabulary.test.ts
// holds the same list over the sources).
const BANNED = /\b(queues?|sessions?|generations?|checkpoints?|task queue|workflow id|signals?|timers?|child workflow|marker|search attribute|rev|revisions?|started by|sheets?|title block|drawing|superseded)\b/i;

let runId = '';


test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
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
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('shell') });
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: [
			'apiVersion: orkstack/v1',
			'kind: Orkflow',
			`name: ${FLOW}`,
			'version: "1.0.0"',
			'description: The shell spec fixture.',
			'tasks:',
			'  - id: hello',
			'    uses: shell',
			'    network: none',
			'    with: {op: exec, command: [echo, ok]}',
			''
		].join('\n')
	});
	runId = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: FLOW })).run.id;
	await waitFor(
		`run ${runId} succeeded`,
		async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status === 'STATUS_SUCCEEDED',
		180_000
	);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}; ${FLOW} ran as ${runId}`);
});

test.afterAll(async () => {
	await killStarted();
	try {
		removeBucket(BUCKET, note);
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

const primary = (page: Page) => page.locator('nav[aria-label="Primary"]');

async function openPalette(page: Page, key = 'Control+k'): Promise<void> {
	await page.keyboard.press(key);
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await expect(page.getByTestId('command-palette-input')).toBeFocused();
}

test('modes: Build is current on its four pages, Operate on every admin page, and the daily surfaces stay reachable', async ({ page }) => {
	for (const [path, build] of [
		['/', 'page'],
		['/runs', 'true'],
		[`/orkflows/${FLOW}`, 'true'],
		['/apply', 'true']
	]) {
		await page.goto(`${UI_ORIGIN}${path}${path.includes('?') ? '&' : '?'}ns=default`);
		await expect(page.getByTestId('nav-dashboard')).toHaveAttribute('aria-current', build);
		await expect(page.getByTestId('nav-admin')).not.toHaveAttribute('aria-current', /.+/);
		await expect(page.getByTestId('admin-nav')).toHaveCount(0);
	}
	await expect(primary(page).getByRole('link', { name: 'Apply Orkflow', exact: true })).toHaveAttribute('aria-current', 'page');

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('nav-admin')).toHaveAttribute('aria-current', 'true');
	await expect(page.getByTestId('nav-dashboard')).not.toHaveAttribute('aria-current', /.+/);
	await expect(page.getByTestId('admin-nav')).toBeVisible();
	await expect(page.getByTestId('admin-tab-namespaces')).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId('admin-nav').getByRole('link')).toHaveCount(12);
	await page.goto(`${UI_ORIGIN}/admin?ns=default`);
	await expect(page.getByTestId('nav-admin')).toHaveAttribute('aria-current', 'page');

	// From Operate, Runs, Orkflows and Apply are one click away and keep ns.
	await expect(primary(page)).toHaveCount(1);
	for (const [name, href] of [
		['Runs', '/runs?ns=default'],
		['Orkflows', '/orkflows?ns=default'],
		['Apply Orkflow', '/apply?ns=default']
	]) {
		await expect(primary(page).getByRole('link', { name, exact: true })).toHaveAttribute('href', href);
	}
	await primary(page).getByRole('link', { name: 'Runs', exact: true }).click();
	await expect(page).toHaveURL(/\/runs\?ns=default/);
	await expect(page.getByTestId('nav-dashboard')).toHaveAttribute('aria-current', 'true');
	await page.screenshot({ path: out('screenshot-modes-runs.png') });
	note('modes: Build current on /, /runs, an Orkflow and /apply; Operate on /admin and /admin/namespaces with its 12-tab strip; Runs, Orkflows and Apply reachable from Operate with ns');
});

test('palette: opens from a page and from a text field, moves, and opens a page, an Orkflow and a run', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await expect(page.getByTestId('command-palette-open')).toContainText('Ctrl K');
	await openPalette(page);
	const input = page.getByTestId('command-palette-input');
	const first = await input.getAttribute('aria-activedescendant');
	expect(first).toMatch(/^command-palette-option-/);
	await page.keyboard.press('ArrowDown');
	await expect(input).not.toHaveAttribute('aria-activedescendant', first!);
	// The rendered list says none of the words the shell may not.
	const listed = await page.getByTestId('command-palette-list').innerText();
	expect(BANNED.test(listed), listed).toBe(false);
	await page.screenshot({ path: out('screenshot-palette-open.png') });

	await input.fill('namespaces');
	await expect(page.getByTestId('command-palette-option-page-operate-namespaces')).toHaveAttribute('aria-selected', 'true');
	await page.keyboard.press('Enter');
	await expect(page).toHaveURL(/\/admin\/namespaces\?ns=default/);
	await expect(page.getByTestId('command-palette')).toHaveCount(0);

	// From inside a text field: Apply's YAML editor.
	await page.goto(`${UI_ORIGIN}/apply?ns=default`);
	await editorReady(page, 'apply-yaml');
	await page.getByTestId('apply-yaml').click();
	await openPalette(page);
	await input.fill(FLOW);
	await expect(page.getByTestId(`command-palette-option-orkflow-${FLOW}`)).toBeVisible();
	await page.getByTestId(`command-palette-option-orkflow-${FLOW}`).click();
	await expect(page).toHaveURL(new RegExp(`/orkflows/${FLOW}\\?ns=default`));
	await expect(page.getByTestId('orkflow-name')).toContainText(FLOW);

	// A run, by its id: navigated to, never looked up.
	let getRuns = 0;
	page.on('request', (r) => {
		if (r.url().includes('RunService/GetRun')) getRuns++;
	});
	await openPalette(page);
	await input.fill(runId);
	await expect(page.getByTestId('command-palette-option-open-run-query')).toBeVisible();
	expect(getRuns).toBe(0);
	await page.keyboard.press('Enter');
	await expect(page.getByTestId('run-id')).toHaveText(runId);
	note(`palette: opened Namespaces, ${FLOW} (from inside the Apply editor) and ${runId}; no GetRun before the run page loaded`);
});

test('palette: an unknown run id lands on the run page’s not-found state, with no GetRun from the palette', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	let getRuns = 0;
	page.on('request', (r) => {
		if (r.url().includes('RunService/GetRun')) getRuns++;
	});
	await openPalette(page);
	await page.getByTestId('command-palette-input').fill('ork-no-such-run');
	await expect(page.getByTestId('command-palette-option-open-run-query')).toContainText('ork-no-such-run');
	// The remote search has had its debounce and answered before Enter.
	await expect(page.getByTestId('command-palette-status')).not.toHaveText('Searching…');
	expect(getRuns).toBe(0);
	await page.keyboard.press('Enter');
	await expect(page).toHaveURL(/\/runs\/ork-no-such-run\?ns=default/);
	await expect(page.getByTestId('engine-error')).toBeVisible();
	note('palette: an unknown id navigated to /runs/ork-no-such-run and the run page said so (engine-error)');
});

test('palette: Escape closes and returns the focus; Close closes on a touch screen', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const opener = page.getByTestId('command-palette-open');
	await opener.click();
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
	await expect(opener).toBeFocused();
	// The shortcut toggles it shut too.
	await openPalette(page);
	await page.keyboard.press('Control+k');
	await expect(page.getByTestId('command-palette')).toHaveCount(0);

	await page.setViewportSize({ width: 390, height: 844 });
	await opener.click();
	await expect(page.getByTestId('command-palette')).toBeVisible();
	const close = page.getByTestId('command-palette-close');
	const box = await close.boundingBox();
	expect((box?.width ?? 0) >= 24 && (box?.height ?? 0) >= 24).toBe(true);
	await page.screenshot({ path: out('screenshot-palette-390.png') });
	await close.click();
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
});

test('palette on an Apple platform: ⌘K opens it, and Ctrl+K is left to the text field', async ({ browser }) => {
	const context = await browser.newContext();
	await context.addInitScript(() => {
		Object.defineProperty(Navigator.prototype, 'platform', { get: () => 'MacIntel' });
		Object.defineProperty(Navigator.prototype, 'userAgentData', { get: () => undefined });
	});
	const page = await context.newPage();
	await page.goto(`${UI_ORIGIN}/apply?ns=default`);
	await expect(page.getByTestId('command-palette-open')).toContainText('⌘K');
	await editorReady(page, 'apply-yaml');
	const yaml = page.getByTestId('apply-yaml');
	await yaml.click();
	await page.evaluate(() => {
		(window as unknown as { __ctrlK: boolean | null }).__ctrlK = null;
		document.querySelector('[data-testid="apply-yaml"]')!.addEventListener('keydown', (e) => {
			const k = e as KeyboardEvent;
			if (k.ctrlKey && k.key === 'k') (window as unknown as { __ctrlK: boolean }).__ctrlK = k.defaultPrevented;
		});
	});
	await page.keyboard.press('Control+k');
	await expect.poll(() => page.evaluate(() => (window as unknown as { __ctrlK: boolean | null }).__ctrlK)).toBe(false);
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
	await expect(yaml).toBeFocused();
	await page.keyboard.press('Meta+k');
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await context.close();
	note('Apple platform: ⌘K opened the palette; Ctrl+K inside the editor reached it unprevented and opened nothing');
});

test('the shortcut opens nothing over an open dialog with a typed form, or an open Select', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${FLOW}?ns=default`);
	await page.getByTestId('orkflow-submit-run').click();
	const dialog = page.getByTestId('orkflow-submit-dialog');
	await expect(dialog).toBeVisible();
	await page.getByTestId('run-submit-input-json-mode').click();
	const json = page.getByTestId('run-submit-inputs-json');
	await json.fill('{"draft": "kept"}');
	await json.focus();
	await page.keyboard.press('Control+k');
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
	await expect(dialog).toBeVisible();
	await expect(json).toHaveValue('{"draft": "kept"}');
	await expect(json).toBeFocused();
	await page.keyboard.press('Escape');
	await expect(dialog).toHaveCount(0);
	await openPalette(page);
	await page.keyboard.press('Escape');

	await page.getByTestId('namespace-picker').click();
	await expect(page.getByTestId('namespace-picker-listbox')).toBeVisible();
	await page.keyboard.press('Control+k');
	await expect(page.getByTestId('command-palette')).toHaveCount(0);
	await expect(page.getByTestId('namespace-picker-listbox')).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(page.getByTestId('namespace-picker-listbox')).toHaveCount(0);
	await openPalette(page);
	note('overlays: Ctrl+K opened nothing over the submit dialog (its typed JSON and focus kept) or the open namespace listbox, and opened the palette once each closed');
});

test('theme: a dark OS paints dark from the first frame; Light survives a reload; System follows the OS live', async ({ browser }) => {
	const context = await browser.newContext({ colorScheme: 'dark' });
	await context.addInitScript(() => {
		requestAnimationFrame(() => {
			(window as unknown as { __firstFrame: unknown }).__firstFrame = {
				theme: document.documentElement.getAttribute('data-theme'),
				background: getComputedStyle(document.body).backgroundColor
			};
		});
	});
	const page = await context.newPage();
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const first = await page.evaluate(() => (window as unknown as { __firstFrame: { theme: string; background: string } }).__firstFrame);
	expect(first.theme).toBe('dark');
	expect(first.background).toBe('rgb(19, 22, 28)');
	await expect(page.getByTestId('theme-system')).toHaveAttribute('aria-pressed', 'true');
	await page.screenshot({ path: out('screenshot-runs-dark.png') });

	await page.getByTestId('theme-light').click();
	await page.reload();
	await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
	await expect(page.getByTestId('theme-light')).toHaveAttribute('aria-pressed', 'true');

	await page.getByTestId('theme-system').click();
	await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
	await page.emulateMedia({ colorScheme: 'light' });
	await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
	await page.emulateMedia({ colorScheme: 'dark' });
	await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
	await context.close();
	note(`theme: first frame ${JSON.stringify(first)}; Light kept across a reload; System followed the OS both ways`);
});

test('collapse: at 390 px nothing scrolls sideways, and the menu holds the navigation and the picker once', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	for (const path of ['/runs', `/runs/${runId}`, '/admin/namespaces']) {
		await page.goto(`${UI_ORIGIN}${path}?ns=default`);
		await expect(page.getByTestId('title-block')).toBeVisible();
		if (path.startsWith('/runs/')) await expect(page.getByTestId('run-status')).toBeVisible();
		expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), path).toBe(true);
	}
	await page.getByRole('button', { name: 'Menu' }).click();
	const drawer = page.getByTestId('menu-drawer');
	await expect(drawer).toBeVisible();
	await expect(page.locator('nav[aria-label="Primary"]')).toHaveCount(1);
	await expect(drawer.locator('nav[aria-label="Primary"]')).toHaveCount(1);
	await expect(page.getByTestId('namespace-picker')).toHaveCount(1);
	await expect(drawer.getByTestId('namespace-picker')).toBeVisible();
	await expect(drawer.getByRole('link', { name: 'Runs', exact: true })).toBeVisible();
	await page.screenshot({ path: out('screenshot-menu-390.png') });
	await drawer.getByRole('link', { name: 'Runs', exact: true }).click();
	await expect(page).toHaveURL(/\/runs\?ns=default/);
	await expect(drawer).toHaveCount(0);
	note('collapse: /runs, a run page and /admin/namespaces fit 390 px; the menu held the one Primary nav and the one picker');
});

test('weight: a first load fetches two latin font files within 60 KB and a stylesheet within 14 KB gzipped', async ({ page }) => {
	const fonts: { url: string; bytes: number }[] = [];
	const styles: { url: string; gzip: number; raw: number }[] = [];
	page.on('response', async (r) => {
		const url = r.url();
		if (/\.woff2(\?|$)/.test(url)) fonts.push({ url, bytes: (await r.body()).length });
		else if (/\.css(\?|$)/.test(url)) {
			const body = await r.body();
			styles.push({ url, raw: body.length, gzip: gzipSync(body).length });
		}
	});
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await page.waitForLoadState('networkidle');
	note(`weight: fonts ${JSON.stringify(fonts.map((f) => [path.basename(f.url), f.bytes]))}; styles ${JSON.stringify(styles.map((s) => [path.basename(s.url), s.raw, s.gzip]))}`);
	expect(fonts.length).toBeLessThanOrEqual(2);
	for (const f of fonts) expect(path.basename(f.url)).toMatch(/-latin-wght-normal\./);
	expect(fonts.reduce((n, f) => n + f.bytes, 0)).toBeLessThanOrEqual(60_000);
	expect(styles.length).toBeGreaterThan(0);
	expect(styles.reduce((n, s) => n + s.gzip, 0)).toBeLessThanOrEqual(14_000);
});

test('specimen and forced colours: the mono tells O from 0, and stamps keep a frame and their words', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	await page.evaluate(() => {
		const strip = document.createElement('p');
		strip.dataset.testid = 'shell-specimen';
		strip.className = 'font-mono text-lg text-ink bg-paper p-4';
		strip.textContent = 'O0 Il1 ork-1792204627744865045 9f2c71e0';
		document.querySelector('main')!.prepend(strip);
	});
	await page.getByTestId('shell-specimen').screenshot({ path: out('screenshot-specimen-mono.png') });

	await page.emulateMedia({ forcedColors: 'active' });
	const stamp = page.locator(`[data-testid="run-row-${runId}"] .stamp`).first();
	await expect(stamp).toHaveText('SUCCEEDED');
	const frame = await stamp.evaluate((el) => getComputedStyle(el).borderTopWidth);
	expect(frame).not.toBe('0px');
	await page.screenshot({ path: out('screenshot-forced-colors.png') });
	await page.emulateMedia({ forcedColors: 'none' });
	note(`specimen written; under forced colours the SUCCEEDED stamp kept its word and a ${frame} frame`);
});

test('title block: default cells, stacked label over an 18 px value, no run id on the list, and a skip link first', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const block = page.getByRole('contentinfo', { name: 'Page summary' });
	await expect(block).toBeVisible();
	await expect(block).toHaveAttribute('data-testid', 'title-block');
	await expect(page.getByTestId('title-block-page')).toContainText('Runs');
	await expect(page.getByTestId('title-block-namespace')).toContainText('default');
	const label = await page.getByTestId('title-block-page').locator('dt').boundingBox();
	const value = await page.getByTestId('title-block-page').locator('dd').boundingBox();
	expect((label?.y ?? 0) < (value?.y ?? 0)).toBe(true);
	expect(await page.getByTestId('title-block-page').locator('dd').evaluate((el) => getComputedStyle(el).fontSize)).toBe('18px');
	expect(await block.innerText()).not.toContain(runId);
	expect(BANNED.test(await block.innerText())).toBe(false);

	await page.goto(`${UI_ORIGIN}/admin/pools?ns=default`);
	await expect(page.getByTestId('title-block-scope')).toContainText('This namespace');

	await page.keyboard.press('Tab');
	const skip = page.getByRole('link', { name: 'Skip to content' });
	await expect(skip).toBeFocused();
	await page.keyboard.press('Enter');
	await expect(page.locator('main#content')).toBeFocused();
	note('title block: PAGE and NAMESPACE by default (SCOPE on Operate pages), 18 px values under their labels, no run id; the skip link is the first stop');
});

async function axeShell(page: Page, label: string): Promise<void> {
	const builder = new AxeBuilder({ page })
		.withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa'])
		.include('header')
		.include('[data-testid="title-block"]');
	if (await page.getByTestId('admin-nav').count()) builder.include('[data-testid="admin-nav"]');
	if (await page.getByTestId('command-palette').count()) builder.include('[data-testid="command-palette"]');
	if (await page.getByTestId('menu-drawer').count()) builder.include('[data-testid="menu-drawer"]');
	const results = await builder.analyze();
	const found = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
	note(`axe ${label}: ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
	expect(found, label).toEqual([]);
}

test('axe: the shell has no WCAG 2.2 AA violation, in both themes, open palette and menu included', async ({ page }) => {
	test.setTimeout(5 * 60_000);
	for (const theme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: theme });
		for (const path of ['/runs', `/runs/${runId}`, '/admin/namespaces']) {
			await page.setViewportSize({ width: 1280, height: 720 });
			await page.goto(`${UI_ORIGIN}${path}?ns=default`);
			await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
			if (path.startsWith('/runs/')) await expect(page.getByTestId('run-status')).toBeVisible();
			await axeShell(page, `${theme} ${path}`);
			const name = path === '/runs' ? 'runs' : path.startsWith('/runs/') ? 'run' : 'namespaces';
			await page.screenshot({ path: out(`screenshot-${name}-${theme}-desktop.png`) });
			await openPalette(page);
			await axeShell(page, `${theme} ${path} with the palette open`);
			await page.keyboard.press('Escape');
			await page.setViewportSize({ width: 390, height: 844 });
			await page.screenshot({ path: out(`screenshot-${name}-${theme}-390.png`) });
			await page.getByRole('button', { name: 'Menu' }).click();
			await expect(page.getByTestId('menu-drawer')).toBeVisible();
			await axeShell(page, `${theme} ${path} with the menu open at 390 px`);
			await page.keyboard.press('Escape');
			await expect(page.getByTestId('menu-drawer')).toHaveCount(0);
		}
	}
});
