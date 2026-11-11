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
//   modes      the sidebar's current link is the page you are on (Overview,
//              Runs, Orkflows, Apply, and one of the Operate group's twelve on
//              each admin page); Build's three daily surfaces stay reachable
//              from Operate; links carry ns;
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
//   collapse   at 390 px no page scrolls sideways, and the sidebar drawer
//              holds the Main navigation and the namespace picker — once;
//   weight     a first load fetches at most two font files, both latin,
//              within 60 KB, and a stylesheet within 14 KB gzipped;
//   specimen, forced colours, the breadcrumb, the skip link, and axe
//   (WCAG 2.2 AA) over the shell's own regions.
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
const BANNED = /\b(queues?|sessions?|generations?|checkpoints?|task queue|workflow id|signals?|timers?|child workflow|marker|search attribute|rev|revisions?|sheets?|title block|drawing|superseded)\b/i;

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

const sidebar = (page: Page) => page.locator('nav[aria-label="Main"]');

async function openPalette(page: Page, key = 'Control+k'): Promise<void> {
	await page.keyboard.press(key);
	await expect(page.getByTestId('command-palette')).toBeVisible();
	await expect(page.getByTestId('command-palette-input')).toBeFocused();
}

test('modes: the sidebar marks the page you are on, Operate has its twelve links, and the daily surfaces stay reachable', async ({ page }) => {
	for (const [path, current] of [
		['/', 'nav-dashboard'],
		['/runs', 'nav-runs'],
		[`/orkflows/${FLOW}`, 'nav-orkflows'],
		// Create Orkflow is a page of Orkflows, and the sidebar draws no link of its own for it.
		['/orkflows/+new', 'nav-orkflows']
	]) {
		await page.goto(`${UI_ORIGIN}${path}?ns=default`);
		await expect(page.getByTestId(current)).toHaveAttribute('aria-current', 'page');
		// One page is current, and none of the Operate links is it.
		await expect(sidebar(page).locator('a[aria-current="page"]')).toHaveCount(1);
		await expect(page.getByTestId('nav-admin')).not.toHaveAttribute('aria-current', /.+/);
		await expect(page.getByTestId('admin-nav').locator('a[aria-current="page"]')).toHaveCount(0);
	}

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('admin-tab-namespaces')).toHaveAttribute('aria-current', 'page');
	await expect(sidebar(page).locator('a[aria-current="page"]')).toHaveCount(1);
	await expect(page.getByTestId('nav-dashboard')).not.toHaveAttribute('aria-current', /.+/);
	await expect(page.getByTestId('nav-admin')).not.toHaveAttribute('aria-current', /.+/);
	await expect(page.getByTestId('admin-nav')).toBeVisible();
	await expect(page.getByTestId('admin-nav').getByRole('link')).toHaveCount(12);
	await page.goto(`${UI_ORIGIN}/admin?ns=default`);
	await expect(page.getByTestId('nav-admin')).toHaveAttribute('aria-current', 'page');

	// From Operate, Runs and Orkflows are one click away and keep ns; the
	// sidebar has no Apply link: Create Orkflow is a button of the Orkflows page.
	await expect(sidebar(page)).toHaveCount(1);
	for (const [id, href] of [
		['nav-runs', '/runs?ns=default'],
		['nav-orkflows', '/orkflows?ns=default']
	]) {
		await expect(page.getByTestId(id)).toHaveAttribute('href', href);
	}
	await expect(page.getByTestId('nav-apply')).toHaveCount(0);
	await page.getByTestId('nav-runs').click();
	await expect(page).toHaveURL(/\/runs\?ns=default/);
	await expect(page.getByTestId('nav-runs')).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId('nav-admin')).not.toHaveAttribute('aria-current', /.+/);
	await page.screenshot({ path: out('screenshot-modes-runs.png') });
	note('modes: the sidebar marks Overview, Runs, an Orkflow and Apply on their pages; an Operate link on each admin page, 12 in all; Runs, Orkflows and Apply reachable from Operate with ns');
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
	await page.goto(`${UI_ORIGIN}/orkflows/+new?ns=default`);
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
	// The plate carries its "run" key and a short form beside the whole id.
	await expect(page.getByTestId('run-id')).toContainText(runId);
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
	await page.goto(`${UI_ORIGIN}/orkflows/+new?ns=default`);
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
	// The dark ground is the page-background token's own value, not a literal.
	expect(first.background).toBe(await page.evaluate(() => {
		const probe = document.createElement('span');
		probe.style.color = 'var(--color-bg)';
		document.body.appendChild(probe);
		const c = getComputedStyle(probe).color;
		probe.remove();
		return c;
	}));
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

test('collapse: at 390 px nothing scrolls sideways, and the sidebar drawer holds the navigation and the picker once', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	for (const path of ['/runs', `/runs/${runId}`, '/admin/namespaces']) {
		await page.goto(`${UI_ORIGIN}${path}?ns=default`);
		await expect(page.getByTestId('breadcrumb')).toBeVisible();
		if (path.startsWith('/runs/')) await expect(page.getByTestId('run-status')).toBeVisible();
		expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), path).toBe(true);
	}
	const menu = page.getByTestId('menu-open');
	await expect(menu).toHaveAttribute('aria-expanded', 'false');
	await menu.click();
	await expect(menu).toHaveAttribute('aria-expanded', 'true');
	// The drawer IS the sidebar: the one Main navigation and the one picker.
	const drawer = page.getByTestId('sidebar');
	await expect(drawer).toBeVisible();
	await expect(page.locator('nav[aria-label="Main"]')).toHaveCount(1);
	await expect(page.getByTestId('namespace-picker')).toHaveCount(1);
	await expect(drawer.getByTestId('namespace-picker')).toBeVisible();
	await expect(drawer.getByTestId('nav-runs')).toBeVisible();
	await page.screenshot({ path: out('screenshot-menu-390.png') });
	await drawer.getByTestId('nav-runs').click();
	await expect(page).toHaveURL(/\/runs\?ns=default/);
	await expect(drawer).toBeHidden();
	await expect(menu).toHaveAttribute('aria-expanded', 'false');
	note('collapse: /runs, a run page and /admin/namespaces fit 390 px; the drawer held the one Main nav and the one picker, and closed on a navigation');
});

test('weight: a first load fetches two latin font files within 89 KB and a stylesheet within 20 KB gzipped', async ({ page }) => {
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
	// Harbour's budgets (phase-2 brief §13): Inter and JetBrains Mono, latin,
	// 48.3 + 40.4 KB; the stylesheet grew by the Harbour primitives (17.1 KB
	// measured 19.3 KB at the change), held here with a margin of under 1 KB.
	expect(fonts.reduce((n, f) => n + f.bytes, 0)).toBeLessThanOrEqual(89_000);
	expect(styles.length).toBeGreaterThan(0);
	expect(styles.reduce((n, s) => n + s.gzip, 0)).toBeLessThanOrEqual(20_000);
});

test('specimen and forced colours: the mono tells O from 0, and status pills keep their mark and their words', async ({ page }) => {
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
	const stamp = page.getByTestId(`run-row-status-${runId}`);
	await expect(stamp).toHaveText('Succeeded');
	// A pill no longer has a border of its own; the frame it keeps in forced
	// colours is the outline of its drawn mark (the pill's ::before).
	const frame = await stamp.evaluate((el) => {
		const mark = getComputedStyle(el, '::before');
		return { style: mark.outlineStyle, width: mark.outlineWidth };
	});
	expect(frame.style).not.toBe('none');
	expect(frame.width).not.toBe('0px');
	await page.screenshot({ path: out('screenshot-forced-colors.png') });
	await page.emulateMedia({ forcedColors: 'none' });
	note(`specimen written; under forced colours the Succeeded pill kept its word and a ${frame.width} outline on its mark`);
});

test('breadcrumb: the namespace, the page, then where on it; no whole run id on the list; and a skip link first', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs?ns=default`);
	const crumbs = page.getByRole('navigation', { name: 'Breadcrumb' });
	await expect(crumbs).toBeVisible();
	await expect(crumbs).toHaveAttribute('data-testid', 'breadcrumb');
	await expect(crumbs.getByRole('link').first()).toHaveText('default');
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('Runs');
	expect(await crumbs.innerText()).not.toContain(runId);
	expect(BANNED.test(await crumbs.innerText())).toBe(false);

	// On a run, the last crumb is its Orkflow; the run's id is on the plate below.
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('breadcrumb-current')).toHaveText(FLOW);
	expect(await crumbs.innerText()).not.toContain(runId);
	expect(BANNED.test(await crumbs.innerText())).toBe(false);

	// The scope of an Operate page is said in its own sentence now (the title block's SCOPE cell is gone).
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=default`);
	await expect(page.getByTestId('breadcrumb-current')).toHaveText('Pools');
	await expect(page.getByTestId('pools-scope-caption')).toContainText('Where a task can run');

	await page.keyboard.press('Tab');
	const skip = page.getByRole('link', { name: 'Skip to content' });
	await expect(skip).toBeFocused();
	await page.keyboard.press('Enter');
	await expect(page.locator('main#content')).toBeFocused();
	note('breadcrumb: default / Runs, the short run id on a run, Operate pages state their scope in prose; the skip link is the first stop');
});

async function axeShell(page: Page, label: string): Promise<void> {
	const builder = new AxeBuilder({ page })
		.withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa'])
		.include('header')
		.include('[data-testid="sidebar"]');
	if (await page.getByTestId('command-palette').count()) builder.include('[data-testid="command-palette"]');
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
			await page.getByTestId('menu-open').click();
			await expect(page.getByTestId('sidebar')).toBeVisible();
			// Axe reads it only once the drawer has finished sliding in.
			await expect(page.getByTestId('sidebar')).toBeInViewport({ ratio: 1 });
			await axeShell(page, `${theme} ${path} with the menu open at 390 px`);
			await page.keyboard.press('Escape');
			await expect(page.getByTestId('sidebar')).toBeHidden();
		}
	}
});
