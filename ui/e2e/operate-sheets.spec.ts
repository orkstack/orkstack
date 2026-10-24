import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
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
	waitFor,
	waitForEngine
} from './harness';

// The Operate sheets (UI revamp, stage 4), against a real engine and Orker
// on real Temporal and MinIO:
//
//   sheets   each of the twelve pages under /admin is one sheet: the strip
//            on top with its own tab current, one h1 in the page's head, the
//            scope said once as a NOTE, and the title block's SCOPE naming
//            how far the page reaches;
//   strip    at 390 px the strip scrolls inside its own frame — never the
//            page — and the tab of the page you are on is brought into it;
//   attention a held namespace is red attention in the register (the row
//            on the red tint, the hold in red ink after its mark), and its
//            budget reads as a figure;
//   screens  every Operate page in both themes, at 1440 and 390 px, with
//            axe (WCAG 2.2 AA) at 1440 and nothing past the right edge at
//            390.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '18981';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const UI_PORT = '14981';
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-operate-sheets-${STAMP}`;
const BUCKET = ownBucket('operate-sheets');
const HELD = `sheets-held-${STAMP}`;
const HOLD_REASON = 'partner CI loop, INC-4412';
const FLOW = `sheets-flow-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'operate-sheets')
	: path.join(uiDir, 'test-results', 'operate-sheets');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);


const flow = () =>
	[
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		`name: ${FLOW}`,
		'version: "1.0.0"',
		'tasks:',
		'  - id: only',
		'    uses: shell',
		'    network: none',
		'    timeout_seconds: 120',
		'    with: {op: exec, command: ["echo", "ok"]}',
		''
	].join('\n');

/** Each Operate page: its strip id, path, h1, the testid that says it has rendered, and its SCOPE. */
const SHEETS: { id: string; path: string; h1: string; ready: string; scope: string }[] = [
	{ id: 'overview', path: '/admin', h1: 'Operate', ready: 'system-info', scope: 'Deployment-wide' },
	{ id: 'namespaces', path: '/admin/namespaces', h1: 'Namespaces', ready: 'namespace-list', scope: 'Deployment-wide' },
	{ id: 'pools', path: '/admin/pools', h1: 'Pools', ready: 'pool-list', scope: 'This namespace' },
	{ id: 'releases', path: '/admin/releases', h1: 'Releases', ready: 'releases-scope', scope: 'Deployment-wide' },
	{ id: 'activities', path: '/admin/activities', h1: 'Activities', ready: 'activity-list', scope: 'Deployment-wide' },
	{ id: 'webhooks', path: '/admin/webhooks', h1: 'Webhooks', ready: 'webhook-list', scope: 'Deployment-wide' },
	{ id: 'automation', path: '/admin/automation', h1: 'Automation', ready: 'automation-yaml', scope: 'This namespace' },
	{ id: 'retention', path: '/admin/retention', h1: 'Maximum run retention', ready: 'retention-info', scope: 'Deployment-wide' },
	{ id: 'usage', path: '/admin/usage', h1: 'Usage', ready: 'usage-table', scope: 'Deployment-wide' },
	{ id: 'cache', path: '/admin/cache', h1: 'Cache', ready: 'cache-window', scope: 'This namespace' },
	{ id: 'image-policy', path: '/admin/image-policy', h1: 'Image policy', ready: 'image-policy-info', scope: 'Deployment-wide' },
	{ id: 'audit', path: '/admin/audit', h1: 'Audit', ready: 'audit-table', scope: 'All namespaces' }
];

/** The scope NOTE each page says its reach in (the strip's overview has none: it is an index). */
const SCOPE_NOTES: Record<string, string> = {
	namespaces: 'namespaces-scope-caption',
	pools: 'pools-scope-caption',
	releases: 'releases-scope',
	activities: 'activities-scope-caption',
	webhooks: 'webhooks-scope',
	automation: 'automation-scope',
	retention: 'retention-scope-caption',
	usage: 'usage-admin-scope',
	cache: 'cache-scope',
	'image-policy': 'image-policy-scope-caption',
	audit: 'audit-scope'
};

const url = (p: string) => `${UI_ORIGIN}${p}?ns=default`;

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [Number(ENGINE_PORT), Number(UI_PORT)]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();
	ensureBucket(BUCKET);
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_S3_BUCKET: BUCKET,
		ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`,
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('operate-sheets') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: UI_PORT, ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/admin`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: flow() });
	await rpc('NamespaceService/CreateNamespace', {
		namespace: {
			name: HELD,
			description: 'closed to new runs',
			hold: { reason: HOLD_REASON },
			budget: { cpuSeconds: '3600', gibSeconds: '7200', period: '86400s', enforce: true }
		}
	});
	await rpc('WebhookService/RegisterWebhook', { url: 'http://127.0.0.1:9/hooks/orkstack', orkflowName: FLOW });
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);
	note(`orkflow ${FLOW}; namespace ${HELD} held ("${HOLD_REASON}") with an enforced budget; one webhook scoped to ${FLOW}`);
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

async function axe(page: Page, label: string): Promise<void> {
	const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
	const found = result.violations.map((v) => `${v.id} (${v.nodes.length}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(', ')})`);
	note(`axe ${label}: ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
	expect(found).toEqual([]);
}

// What runs past the right edge, named, so a failure says where: only what
// no scrolling frame of its own (a register, the strip) contains.
async function pastRightEdge(page: Page): Promise<string[]> {
	return page.evaluate(() => {
		if (document.documentElement.scrollWidth <= window.innerWidth) return [];
		const contained = (el: Element): boolean => {
			for (let p = el.parentElement; p && p !== document.body; p = p.parentElement) {
				if (['auto', 'scroll', 'hidden', 'clip'].includes(getComputedStyle(p).overflowX)) return true;
			}
			return false;
		};
		const loose = [...document.querySelectorAll('body *')].filter(
			(el) => el.getBoundingClientRect().right > window.innerWidth + 0.5 && !contained(el)
		);
		return [
			`document ${document.documentElement.scrollWidth} px wide`,
			...loose
				.slice(0, 8)
				.map(
					(el) =>
						`${el.tagName.toLowerCase()}${el.getAttribute('data-testid') ? `[${el.getAttribute('data-testid')}]` : ''}.${el.className.toString().slice(0, 60)} → ${Math.round(el.getBoundingClientRect().right)}`
				)
		];
	});
}

test('sheets: every Operate page is one sheet — its tab current, one h1, its scope said once, SCOPE in the title block', async ({ page }) => {
	for (const sheet of SHEETS) {
		await page.goto(url(sheet.path));
		await expect(page.getByTestId(sheet.ready)).toBeVisible();
		await expect(page.getByTestId(`admin-tab-${sheet.id}`)).toHaveAttribute('aria-current', 'page');
		await expect(page.getByTestId('admin-nav').locator('[aria-current="page"]')).toHaveCount(1);
		const h1 = page.locator('main h1');
		await expect(h1).toHaveCount(1);
		await expect(h1).toHaveText(sheet.h1);
		// The strip is the sheet's first row: it comes before the page's head.
		const order = await page.evaluate(() => {
			const strip = document.querySelector('[data-testid="admin-nav"]')!;
			const head = document.querySelector('main h1')!;
			return strip.compareDocumentPosition(head) & Node.DOCUMENT_POSITION_FOLLOWING ? 'strip first' : 'head first';
		});
		expect(order).toBe('strip first');
		const scopeNote = SCOPE_NOTES[sheet.id];
		if (scopeNote) {
			await expect(page.getByTestId(scopeNote)).toBeVisible();
			await expect(page.locator('.note', { has: page.getByTestId(scopeNote) }).getByText('Note', { exact: true })).toBeVisible();
		}
		await expect(page.getByTestId('title-block-page')).toContainText(sheet.id === 'overview' ? 'Operate overview' : sheet.h1 === 'Maximum run retention' ? 'Retention' : sheet.h1);
		await expect(page.getByTestId('title-block-scope')).toContainText(sheet.scope);
		note(`sheet ${sheet.path}: h1 "${sheet.h1}", tab admin-tab-${sheet.id} current, SCOPE ${sheet.scope}${scopeNote ? `, NOTE ${scopeNote}` : ''}`);
	}
	// The overview indexes the other eleven, one link each, with what each governs and its reach.
	await page.goto(url('/admin'));
	const rows = page.getByTestId('operate-index').locator('tbody tr');
	await expect(rows).toHaveCount(11);
	for (const row of await rows.all()) await expect(row.getByRole('link')).toHaveCount(1);
	await expect(page.getByTestId('admin-card-cache')).toHaveAttribute('href', '/admin/cache?ns=default');
});

test('strip: at 390 px it scrolls inside its own frame, never the page, and brings the current tab into view', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	await page.goto(url('/admin/namespaces'));
	await expect(page.getByTestId('namespace-list')).toBeAttached();
	const strip = page.getByTestId('admin-nav');
	const before = await strip.evaluate((el) => ({ scroll: el.scrollWidth, client: el.clientWidth, overflowX: getComputedStyle(el).overflowX }));
	expect(before.scroll, 'the twelve tabs are wider than 390 px').toBeGreaterThan(before.client);
	expect(before.overflowX).toBe('auto');
	expect(await pastRightEdge(page), 'the page itself never scrolls sideways').toEqual([]);

	// Scrolling the strip moves its tabs, not the page.
	await strip.evaluate((el) => el.scrollTo({ left: el.scrollWidth }));
	await expect(page.getByTestId('admin-tab-audit')).toBeInViewport();
	expect(await page.evaluate(() => window.scrollX)).toBe(0);

	// The page you are on is brought into the strip's frame.
	await page.goto(url('/admin/audit'));
	await expect(page.getByTestId('audit-scope')).toBeVisible();
	const audit = await page.evaluate(() => {
		const nav = document.querySelector('[data-testid="admin-nav"]')!.getBoundingClientRect();
		const tab = document.querySelector('[data-testid="admin-tab-audit"]')!.getBoundingClientRect();
		return { inside: tab.left >= nav.left - 0.5 && tab.right <= nav.right + 0.5, scrollLeft: document.querySelector('[data-testid="admin-nav"]')!.scrollLeft };
	});
	expect(audit.inside, `admin-tab-audit inside the strip (scrollLeft ${audit.scrollLeft})`).toBe(true);
	expect(audit.scrollLeft).toBeGreaterThan(0);
	await page.screenshot({ path: out('screenshot-strip-audit-390.png') });
	note(`strip at 390 px: ${before.scroll} px of tabs in a ${before.client} px frame; on /admin/audit the strip scrolled ${audit.scrollLeft} px to show its tab; the page never scrolled sideways`);
	await page.setViewportSize({ width: 1280, height: 720 });
});

test('namespaces: a held namespace is red attention in the register, and its budget a figure', async ({ page }) => {
	await page.goto(url('/admin/namespaces'));
	const row = page.getByTestId(`namespace-row-${HELD}`);
	await expect(row).toHaveAttribute('data-held', 'true');
	const hold = page.getByTestId(`namespace-hold-${HELD}`);
	await expect(hold).toContainText(HOLD_REASON);
	await expect(hold).toHaveClass(/severity-error/);
	const redTint = await tokenColor(page, '--color-red-tint');
	const ground = await row.evaluate((el) => getComputedStyle(el).backgroundColor);
	expect(ground).toBe(redTint);
	const budget = page.getByTestId(`namespace-budget-${HELD}`);
	const face = await budget.evaluate((el) => ({ family: getComputedStyle(el).fontFamily, numeric: getComputedStyle(el).fontVariantNumeric }));
	expect(face.family).toContain('Spline Sans Mono');
	expect(face.numeric).toContain('tabular-nums');
	const accepting = page.getByTestId('namespace-row-default');
	await expect(accepting).toHaveAttribute('data-held', 'false');
	expect(await accepting.evaluate((el) => getComputedStyle(el).backgroundColor)).not.toBe(redTint);
	note(`namespaces: ${HELD} held on ${ground} (the red tint), hold "${await hold.textContent()}", budget "${(await budget.textContent())?.trim()}" in ${face.family.split(',')[0]}`);
});

test('screens: every Operate page in both themes, at 1440 and 390 px, with axe', async ({ page }) => {
	test.setTimeout(10 * 60_000);
	// Open what each page's components draw only on request, so the screen shows them.
	async function expand(id: string): Promise<void> {
		if (id === 'pools') {
			await page.getByTestId('pool-orkers-toggle-main').click();
			await expect(page.getByTestId('pool-orkers-main')).toBeVisible();
			await expect(page.getByTestId('pool-orkers-loading-main')).toHaveCount(0);
		}
		if (id === 'activities') {
			await page.getByTestId('activity-interface-file').click();
			await expect(page.getByTestId('activity-interface-panel-file')).toBeVisible();
		}
	}
	for (const theme of ['light', 'dark'] as const) {
		await page.setViewportSize({ width: 1440, height: 900 });
		await page.goto(url('/admin'));
		await page.getByTestId(`theme-${theme}`).click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
		for (const sheet of SHEETS) {
			await page.goto(url(sheet.path));
			await expect(page.getByTestId(sheet.ready)).toBeVisible();
			await expand(sheet.id);
			await page.screenshot({ path: out(`screenshot-${sheet.id}-${theme}-1440.png`), fullPage: true });
			await axe(page, `${sheet.path} (${theme}, 1440)`);
		}
		await page.setViewportSize({ width: 390, height: 844 });
		for (const sheet of SHEETS) {
			await page.goto(url(sheet.path));
			await expect(page.getByTestId(sheet.ready)).toBeAttached();
			await expect(page.getByTestId('title-block')).toBeVisible();
			expect(await pastRightEdge(page), `${sheet.path} at 390 px: nothing runs past the right edge`).toEqual([]);
			await page.screenshot({ path: out(`screenshot-${sheet.id}-${theme}-390.png`), fullPage: true });
		}
		note(`screens (${theme}): ${SHEETS.length} Operate pages at 1440 (axe clean) and 390 px (no horizontal overflow)`);
	}
	await page.setViewportSize({ width: 1280, height: 720 });
	await page.goto(url('/admin'));
	await page.getByTestId('theme-system').click();
});
