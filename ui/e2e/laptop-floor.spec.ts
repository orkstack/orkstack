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
	temporalNamespace,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

// The design floor (1280x800, the smallest 15-inch laptop the UI is drawn
// for), against a real engine and Orker:
//
//   namespace  the breadcrumb is where a page says its namespace: no page's
//              own content repeats it, and the crumb leads back to Runs;
//   floor      every page at 1280x800, in both themes: nothing past the right
//              edge, no register that must scroll sideways to be read, and
//              nothing axe's WCAG rules find;
//   versions   a derived version is short everywhere and whole in its tooltip;
//   lists      Runs and Running now set a version under its Orkflow's name, the
//              Orkflows list sets labels there, and the Namespaces list fits;
//   dashboard  counts, then registers side by side, then the week's charts;
//   links      a row's name opens its edit, at an address of its own;
//   sidebar    the Runs glyph, and a count that follows a run that ends;
//   wide       at 1920, 2560 and 3440 wide the content column fills the width,
//              no page scrolls sideways, and running text and forms keep their
//              measure; a thousand-task run page fills it too.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and ports;
// it ends only the processes it started.
const ENGINE_PORT = '19021';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = temporalNamespace('laptop-floor');
const BUCKET = ownBucket('laptop-floor');
// A name no other word on any page can contain, so a page that still prints
// its namespace is found by the name alone.
const NS = `floor-ns-${STAMP}`;
const FLOW = `floor-flow-${STAMP}`;
const FAILING = `floor-failing-${STAMP}`;
const LONG = `floor-long-${STAMP}`;
const DERIVED = `floor-derived-${STAMP}`;
const HEX_FLOW = `floor-hex-${STAMP}`;
const HEX_VERSION = 'b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'laptop-floor')
	: path.join(uiDir, 'test-results', 'laptop-floor');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const url = (p: string): string => `${ENGINE}${p}${p.includes('?') ? '&' : '?'}ns=${NS}`;

let runId = '';
let longRun = '';
let derivedVersion = '';
let webhookId = '';

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	test.setTimeout(10 * 60_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
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
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('laptop-floor') });
	await waitForEngine(ENGINE);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS } });
	await waitFor('the namespace reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', { namespace: NS });
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: `floor-held-${STAMP}`, allowedPools: ['main'], defaultPool: 'main', maxRunning: 5, description: 'A held namespace on the floor.' }
	});
	webhookId = (
		await rpc<{ webhook: { id: string } }>('WebhookService/RegisterWebhook', { url: 'https://hooks.example.invalid/orkstack/a-fairly-long-path', events: ['RUN_FAILED'] })
	).webhook.id;
	await rpc('OrkflowService/ApplyOrkflow', {
		namespace: NS,
		yaml: [
			'apiVersion: orkstack/v1',
			'kind: Orkflow',
			`name: ${FLOW}`,
			'version: "1.0.0"',
			'description: The laptop floor fixture.',
			'labels:',
			'  team: floor',
			'tasks:',
			'  - id: hello',
			'    uses: shell',
			'    network: none',
			'    with: {op: exec, command: [echo, ok]}',
			''
		].join('\n')
	});
	runId = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { namespace: NS, orkflowName: FLOW })).run.id;
	await waitFor(
		`run ${runId} succeeded`,
		async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { namespace: NS, runId })).run?.status === 'STATUS_SUCCEEDED',
		180_000
	);
	const applyFlow = (name: string, command: string, version?: string) =>
		rpc<{ orkflow: { version: string } }>('OrkflowService/ApplyOrkflow', {
			namespace: NS,
			yaml: [
				'apiVersion: orkstack/v1',
				'kind: Orkflow',
				`name: ${name}`,
				...(version ? [`version: "${version}"`] : []),
				'tasks:',
				'  - id: only',
				'    uses: shell',
				'    network: none',
				`    with: {op: exec, command: [sh, -c, "${command}"]}`,
				''
			].join('\n')
		});
	await applyFlow(FAILING, 'exit 1', '1.0.0');
	await applyFlow(LONG, 'sleep 600', '1.0.0');
	derivedVersion = (await applyFlow(DERIVED, 'echo derived')).orkflow.version;
	await applyFlow(HEX_FLOW, 'echo hex', HEX_VERSION);
	const failed = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { namespace: NS, orkflowName: FAILING })).run.id;
	await waitFor(`run ${failed} failed`, async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { namespace: NS, runId: failed })).run?.status === 'STATUS_FAILED', 180_000);
	await rpc('RunService/SubmitRun', { namespace: NS, orkflowName: DERIVED });
	longRun = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { namespace: NS, orkflowName: LONG })).run.id;
	await waitFor(`run ${longRun} is running`, async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { namespace: NS, runId: longRun })).run?.status === 'STATUS_RUNNING', 180_000);
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}; ${FLOW} ran as ${runId} in ${NS}`);
});

test.afterAll(async () => {
	if (longRun) await rpc('RunService/CancelRun', { namespace: NS, runId: longRun, reason: 'the spec is done' }).catch(() => undefined);
	await killStarted();
	try {
		removeBucket(BUCKET, note);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

/** Every page that is about one namespace, with what says it has drawn. */
const namespaced = (): { path: string; ready: string }[] => [
	{ path: '/', ready: 'dashboard-heading' },
	{ path: '/runs', ready: 'run-list' },
	{ path: `/runs/${runId}`, ready: 'run-status' },
	{ path: '/orkflows', ready: 'orkflow-list' },
	{ path: '/orkflows/+new', ready: 'apply-validate' },
	{ path: `/orkflows/${FLOW}`, ready: 'orkflow-name' },
	{ path: `/orkflows/${FLOW}?tab=versions`, ready: 'orkflow-name' },
	{ path: `/orkflows/${FLOW}?tab=schedules`, ready: 'schedules-scope' },
	{ path: `/orkflows/${FLOW}?tab=triggers`, ready: 'triggers-scope' },
	{ path: `/orkflows/${FLOW}?tab=edit`, ready: 'orkflow-name' },
	{ path: '/admin/pools', ready: 'pools-scope-caption' },
	{ path: '/admin/cache', ready: 'breadcrumb-current' },
	{ path: '/admin/automation', ready: 'automation-scope' }
];

test('namespace: the breadcrumb says it, and no page repeats it in its own content', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	const repeats: string[] = [];
	for (const { path: p, ready } of namespaced()) {
		await page.goto(url(p));
		await expect(page.getByTestId(ready).first()).toBeVisible();
		await expect(page.getByTestId('breadcrumb').getByRole('link').first()).toHaveText(NS);
		const text = await page.locator('main#content').innerText();
		// The automation document's own `namespace:` key is the document, not the page talking.
		for (const line of text.split('\n')) if (line.includes(NS) && !line.startsWith('namespace: ')) repeats.push(`${p}: ${line}`);
	}
	expect(repeats, 'a page repeats the namespace in its own content').toEqual([]);
	note('the namespace is in the breadcrumb, and in no page content of ' + namespaced().length + ' pages');
});

test('breadcrumb: the namespace is a link to Runs, named as a namespace, and the crumbs are chevrons, not slashes', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url(`/orkflows/${FLOW}`));
	const crumbs = page.getByTestId('breadcrumb');
	const first = crumbs.getByRole('link').first();
	await expect(first).toHaveAttribute('href', new RegExp(`^/runs\\?ns=${NS}`));
	await expect(first).toHaveAccessibleName(`Namespace ${NS}`);
	expect(await crumbs.innerText()).not.toContain('/');
	await expect(crumbs.locator('svg')).toHaveCount(2);
	for (const theme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: theme });
		await crumbs.screenshot({ path: out(`screenshot-breadcrumb-${theme}.png`) });
	}
	await page.emulateMedia({ colorScheme: 'light' });
});

/** Every page of the app, with what says it has drawn. */
const floorPages = (): { path: string; ready: string; name: string }[] => [
	...namespaced().map((p) => ({ ...p, name: p.path.replace(runId, 'run').replace(FLOW, 'flow').replace(/[^a-z]+/g, '-').replace(/^-|-$/g, '') || 'dashboard' })),
	{ path: `/runs/${runId}/compare?to=${runId}`, ready: 'breadcrumb-current', name: 'compare' },
	{ path: '/admin', ready: 'system-info', name: 'admin' },
	{ path: '/admin/namespaces', ready: 'namespace-list', name: 'admin-namespaces' },
	{ path: `/admin/namespaces/${NS}`, ready: 'namespace-facts', name: 'admin-namespace' },
	{ path: '/admin/releases', ready: 'releases-scope', name: 'admin-releases' },
	{ path: '/admin/activities', ready: 'activity-list', name: 'admin-activities' },
	{ path: '/admin/webhooks', ready: 'webhooks-scope', name: 'admin-webhooks' },
	{ path: '/admin/retention', ready: 'retention-info', name: 'admin-retention' },
	{ path: '/admin/usage', ready: 'usage-table', name: 'admin-usage' },
	{ path: '/admin/image-policy', ready: 'image-policy-info', name: 'admin-image-policy' },
	{ path: '/admin/audit', ready: 'audit-table', name: 'admin-audit' }
];

/** What runs past the page's right edge, and every register that scrolls sideways in its frame. */
async function overflows(page: Page): Promise<string[]> {
	return page.evaluate(() => {
		const found: string[] = [];
		const root = document.documentElement;
		if (root.scrollWidth > root.clientWidth) found.push(`the page itself: ${root.scrollWidth} > ${root.clientWidth}`);
		for (const table of document.querySelectorAll<HTMLElement>('main table')) {
			const frame = table.parentElement;
			// A chart's table for assistive technology sits in a one-pixel frame and is never read sideways.
			if (frame && frame.clientWidth > 1 && frame.scrollWidth > frame.clientWidth + 1) {
				found.push(`${table.dataset.testid ?? 'a table'}: ${frame.scrollWidth} > ${frame.clientWidth}`);
			}
		}
		return found;
	});
}

for (const theme of ['light', 'dark'] as const) {
	test(`floor: every page at 1280x800 in ${theme}, nothing past the edge, no register scrolling sideways, no axe violation`, async ({ page }) => {
		test.setTimeout(5 * 60_000);
		await page.setViewportSize({ width: 1280, height: 800 });
		await page.emulateMedia({ colorScheme: theme });
		const found: string[] = [];
		for (const { path: p, ready, name } of floorPages()) {
			await page.goto(url(p));
			await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
			await expect(page.getByTestId(ready).first()).toBeVisible();
			for (const what of await overflows(page)) found.push(`${p}: ${what}`);
			const axe = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze();
			for (const v of axe.violations) found.push(`${p}: axe ${v.id}: ${v.nodes.map((n) => n.target.join(' ')).join(', ')}`);
			await page.screenshot({ path: out(`screenshot-${name}-${theme}.png`) });
		}
		note(`floor ${theme}: ${found.length} overflow(s)${found.length ? ': ' + found.join('; ') : ''}`);
		expect(found).toEqual([]);
	});
}

/** The top and bottom of two elements, for "this one is under that one". */
async function stacked(page: Page, upper: ReturnType<Page['locator']>, lower: ReturnType<Page['locator']>): Promise<void> {
	const [a, b] = await Promise.all([upper.boundingBox(), lower.boundingBox()]);
	expect(a, 'the upper element is drawn').not.toBeNull();
	expect(b, 'the lower element is drawn').not.toBeNull();
	expect(b!.y).toBeGreaterThanOrEqual(a!.y + a!.height - 1);
	expect(Math.abs(b!.x - a!.x)).toBeLessThan(2);
}

test('versions: a derived version is short on every page, whole in its tooltip and in its address', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	expect(derivedVersion).toMatch(/^[0-7][0-9a-hjkmnp-tv-z]{25}$/);
	const uuid = `…${derivedVersion.slice(-7)}`;
	const hex = HEX_VERSION.slice(0, 8);

	await page.goto(url('/orkflows'));
	const row = page.getByTestId(`orkflow-row-${DERIVED}`);
	await expect(row).toContainText(uuid);
	await expect(row.locator(`[data-tip="${derivedVersion}"]`)).toHaveText(uuid);
	await expect(page.getByTestId(`orkflow-row-${HEX_FLOW}`)).toContainText(hex);
	await expect(page.getByTestId(`orkflow-row-${FLOW}`)).toContainText('1.0.0');

	for (const [name, full, short] of [[DERIVED, derivedVersion, uuid], [HEX_FLOW, HEX_VERSION, hex]] as const) {
		for (const tab of ['', '?tab=versions', '?tab=schedules', '?tab=triggers']) {
			await page.goto(url(`/orkflows/${name}${tab}`));
			await expect(page.getByTestId('orkflow-version')).toContainText(short);
			await expect(page.getByTestId('orkflow-version').locator('.plate-v')).toHaveAttribute('data-tip', full);
			const text = await page.locator('main#content').innerText();
			expect(text.includes(full), `/orkflows/${name}${tab} prints the whole version`).toBe(false);
		}
	}

	// A shortened version reaches a keyboard: its plate is a tab stop, and focus shows the whole value.
	await page.goto(url(`/orkflows/${DERIVED}`));
	await page.getByTestId('orkflow-version').locator('.plate-v').focus();
	await expect(page.locator('.tt')).toHaveText(derivedVersion);
	await page.goto(url('/orkflows'));
	await page.getByTestId(`orkflow-row-${DERIVED}`).locator(`[data-tip="${derivedVersion}"]`).focus();
	await expect(page.locator('.tt')).toHaveText(derivedVersion);

	// A version has no copy button, and the run's plate is the same form with its link whole.
	await page.goto(url('/runs'));
	await expect(page.getByTestId('run-list')).toContainText(uuid);
	expect((await page.locator('main#content').innerText()).includes(derivedVersion)).toBe(false);
	await page.locator(`tr:has-text("${DERIVED}") a[href^="/runs/"]`).first().click();
	await expect(page.getByTestId('run-version')).toContainText(uuid);
	await expect(page.getByTestId('run-version').getByRole('button')).toHaveCount(0);
	await expect(page.getByTestId('run-version-link')).toHaveAttribute('href', new RegExp(`version=${derivedVersion}`));
	await expect(page.getByTestId('run-version-link')).toHaveAttribute('aria-label', new RegExp(derivedVersion));
	await page.getByTestId('run-version-link').hover();
	await expect(page.locator('.tt')).toHaveText(derivedVersion);
	await page.screenshot({ path: out('screenshot-version-tooltip.png') });
	note(`versions: ${derivedVersion} shows as ${uuid}, ${HEX_VERSION} as ${hex}`);
});

test('lists: a run\'s version sits under its Orkflow\'s name, on Runs and in Running now', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url(`/runs?filter=${encodeURIComponent(`orkflow_name=${LONG}`)}`));
	const cell = page.locator('td.wf').first();
	await stacked(page, cell.getByText(LONG, { exact: true }), cell.locator('.plate'));
	await cell.scrollIntoViewIfNeeded();
	await page.screenshot({ path: out('screenshot-runs-version-under-name.png') });
	await page.goto(url('/'));
	const running = page.getByTestId(`running-${longRun}`);
	await stacked(page, running.getByText(LONG, { exact: true }), running.locator('.plate'));
});

test('lists: an Orkflow\'s labels sit under its name in the navy tag, and the list has no Tasks or Labels column', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url('/orkflows'));
	await expect(page.getByTestId('orkflow-list').locator('thead th')).toHaveText(['Name', 'Version', 'Digest', 'Created']);
	const labels = page.getByTestId(`orkflow-labels-${FLOW}`);
	const tag = labels.locator('.tag');
	await expect(tag).toHaveText('team=floor');
	const description = page.getByTestId(`orkflow-row-${FLOW}`).getByText('The laptop floor fixture.');
	expect((await tag.boundingBox())!.y).toBeGreaterThanOrEqual((await description.boundingBox())!.y);
	const sizes = await page.evaluate(() => {
		const size = (el: Element | null) => parseFloat(getComputedStyle(el!).fontSize);
		return { tag: size(document.querySelector('[data-testid^="orkflow-labels-"] .tag')), name: size(document.querySelector('[data-testid^="orkflow-row-"] a')) };
	});
	expect(sizes.tag).toBeLessThan(sizes.name);
});

test('namespaces: the list fits the floor, and a name opens that namespace at an address of its own', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url('/admin/namespaces'));
	await expect(page.getByTestId('namespace-list').locator('thead th')).toHaveText(['Name', 'Hold', 'Max running', 'Run retention', 'Pools', 'Telemetry', 'Actions']);
	expect(await overflows(page)).toEqual([]);
	const row = page.getByTestId(`namespace-row-${NS}`);
	await expect(row.getByRole('button', { name: 'Edit', exact: true })).toHaveCount(0);
	await expect(page.getByTestId(`namespace-pools-${NS}`)).toContainText('main default');
	await expect(page.getByTestId(`namespace-telemetry-sink-${NS}`)).toContainText('inherited');
	await page.screenshot({ path: out('screenshot-namespaces-list.png') });

	await page.getByTestId(`namespace-edit-${NS}`).click();
	await expect(page).toHaveURL(new RegExp(`/admin/namespaces/${NS}\\?`));
	await expect(page.getByTestId('breadcrumb-current')).toHaveText(NS);
	await expect(page.getByTestId(`admin-tab-namespaces`)).toHaveAttribute('aria-current', 'page');
	await expect(page.getByTestId(`namespace-budget-${NS}`)).toHaveText('unbounded');
	await expect(page.getByTestId(`namespace-allowed-secrets-${NS}`)).toHaveText('unrestricted');
	await expect(page.getByTestId(`namespace-require-signed-by-${NS}`)).toHaveText('—');
	await expect(page.getByTestId(`namespace-updated-${NS}`)).not.toBeEmpty();
	await page.screenshot({ path: out('screenshot-namespace-page.png') });

	// The address is the state: a reload reopens it, and a save lands back on its row.
	await page.reload();
	await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toBeVisible();
	await page.getByTestId(`ns-edit-description-${NS}`).fill('Edited on the floor.');
	await page.getByTestId(`ns-edit-save-${NS}`).click();
	await expect(page.getByTestId(`namespace-row-${NS}`)).toContainText('Edited on the floor.');
	await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toHaveCount(0);

	// A namespace nobody has says so, and offers the way back.
	await page.goto(url('/admin/namespaces/no-such-namespace'));
	await expect(page.getByTestId('namespace-detail-error')).toBeVisible();
});

test('links: a webhook and a trigger open their edit from their name, at an address of their own', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url('/admin/webhooks'));
	await page.getByTestId(`edit-webhook-${webhookId}`).click();
	await expect(page.getByTestId('webhook-edit-dialog')).toBeVisible();
	await expect(page).toHaveURL(new RegExp(`edit=${webhookId}`));
	// Reloaded, the address opens the same edit; closed, it is gone from the address.
	await page.reload();
	await expect(page.getByTestId('webhook-edit-dialog')).toBeVisible();
	await page.screenshot({ path: out('screenshot-webhook-edit-address.png') });
	await page.keyboard.press('Escape');
	await expect(page.getByTestId('webhook-edit-dialog')).toHaveCount(0);
	await expect(page).not.toHaveURL(/edit=/);
	await page.goto(url('/admin/webhooks?edit=wh-no-such'));
	await expect(page.getByTestId('webhook-edit-missing')).toBeVisible();

	const trigger = (await rpc<{ trigger: { id: string } }>('TriggerService/CreateTrigger', { namespace: NS, orkflowName: FLOW })).trigger.id;
	await page.goto(url(`/orkflows/${FLOW}?tab=triggers`));
	await page.getByTestId(`edit-trigger-${trigger}`).click();
	await expect(page.getByTestId('trigger-edit-dialog')).toBeVisible();
	await expect(page).toHaveURL(new RegExp(`tab=triggers.*edit=${trigger}`));
	await page.reload();
	await expect(page.getByTestId('trigger-edit-dialog')).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(page).not.toHaveURL(/edit=/);
	await page.goto(url(`/orkflows/${FLOW}?tab=triggers&edit=trg-no-such`));
	await expect(page.getByTestId('trigger-edit-missing')).toBeVisible();
	await rpc('TriggerService/DeleteTrigger', { namespace: NS, id: trigger });

	// A schedule's name opens its edit at an address of its own, and its fires stay behind their button.
	await page.goto(url(`/orkflows/${FLOW}?tab=schedules`));
	await page.getByTestId('schedule-name').fill('floor-nightly');
	await page.getByTestId('schedule-cron').fill('0 2 * * *');
	await page.getByTestId('schedule-create-form').getByRole('button', { name: 'Create schedule' }).click();
	const open = page.getByTestId('edit-schedule-floor-nightly');
	await open.click();
	await expect(page).toHaveURL(/edit=floor-nightly/);
	const dialog = page.getByTestId('schedule-edit-dialog');
	await expect(dialog).toBeVisible();
	await page.reload();
	await expect(dialog).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(dialog).toHaveCount(0);
	await expect(page).not.toHaveURL(/edit=/);
	const expand = page.getByTestId('schedule-expand-floor-nightly');
	await expect(expand).toHaveAttribute('aria-expanded', 'false');
	await expand.click();
	await expect(expand).toHaveAttribute('aria-expanded', 'true');
	await page.goto(url(`/orkflows/${FLOW}?tab=schedules&edit=no-such`));
	await expect(page.getByTestId('schedule-edit-missing')).toBeVisible();
});

for (const theme of ['light', 'dark'] as const) {
	test(`schedule edit: the dialog fits the floor in ${theme}, its last control reachable, no axe violation`, async ({ page }) => {
		await page.setViewportSize({ width: 1280, height: 800 });
		await page.emulateMedia({ colorScheme: theme });
		const schedule = `floor-edit-${theme}`;
		await rpc('ScheduleService/CreateSchedule', {
			namespace: NS,
			name: schedule,
			orkflowName: FLOW,
			description: 'Every field on show',
			input: { data: { mode: 'full' } },
			spec: { cron: ['0 2 * * *'], timezone: 'Europe/Lisbon', jitter: '30s', pauseOnFailure: true },
			paused: true
		});
		const dialog = page.getByTestId('schedule-edit-dialog');
		await expect(async () => {
			await page.goto(url(`/orkflows/${FLOW}?tab=schedules&edit=${schedule}`));
			await expect(dialog).toBeVisible({ timeout: 2_000 });
		}).toPass({ timeout: 60_000 });
		await expect(page.getByTestId('schedule-edit-orkflow')).toHaveText(FLOW);
		await page.screenshot({ path: out(`screenshot-schedule-edit-${theme}.png`) });
		const box = await dialog.boundingBox();
		expect(box!.y).toBeGreaterThanOrEqual(0);
		expect(box!.y + box!.height).toBeLessThanOrEqual(800);
		const save = page.getByTestId('schedule-edit-submit');
		await save.scrollIntoViewIfNeeded();
		await expect(save).toBeInViewport();
		await page.screenshot({ path: out(`screenshot-schedule-edit-end-${theme}.png`) });
		const axe = await new AxeBuilder({ page }).include('[data-testid="schedule-edit-dialog"]').withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa', 'wcag22aa']).analyze();
		expect(axe.violations.map((v) => v.id)).toEqual([]);
		await page.keyboard.press('Escape');
		await rpc('ScheduleService/DeleteSchedule', { namespace: NS, name: schedule });
	});
}

for (const theme of ['light', 'dark'] as const) {
	test(`delete: a row's Delete and its confirm carry the trash glyph at the floor in ${theme}, and the row still fits`, async ({ page }) => {
		await page.setViewportSize({ width: 1280, height: 800 });
		await page.emulateMedia({ colorScheme: theme });
		const trigger = (await rpc<{ trigger: { id: string } }>('TriggerService/CreateTrigger', { namespace: NS, orkflowName: FLOW })).trigger.id;
		await page.goto(url(`/orkflows/${FLOW}?tab=triggers`));
		const remove = page.getByTestId(`delete-trigger-${trigger}`);
		await expect(remove.locator('svg use')).toHaveAttribute('href', '#i-trash');
		expect(await overflows(page)).toEqual([]);
		await remove.scrollIntoViewIfNeeded();
		await page.screenshot({ path: out(`screenshot-trash-row-${theme}.png`) });
		await remove.click();
		await expect(page.getByTestId('delete-trigger-accept').locator('svg use')).toHaveAttribute('href', '#i-trash');
		await page.screenshot({ path: out(`screenshot-trash-confirm-${theme}.png`) });
		await page.getByTestId('delete-trigger-cancel').click();
		await rpc('TriggerService/DeleteTrigger', { namespace: NS, id: trigger });

		await page.goto(url('/admin/namespaces'));
		await expect(page.getByTestId(`namespace-delete-${NS}`).locator('svg use')).toHaveAttribute('href', '#i-trash');
		expect(await overflows(page)).toEqual([]);
		await page.screenshot({ path: out(`screenshot-trash-namespaces-${theme}.png`) });
	});
}

test('namespaces: unsaved typing is not lost moving from one namespace page to another', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url(`/admin/namespaces/${NS}`));
	const description = page.getByTestId(`ns-edit-description-${NS}`);
	await description.fill('typed and not saved');
	const asked: string[] = [];
	page.on('dialog', (dialog) => {
		asked.push(dialog.message());
		void dialog.dismiss();
	});
	// An in-app navigation to the same route, another namespace.
	await page.evaluate((href) => {
		const link = document.createElement('a');
		link.href = href;
		document.querySelector('main')!.appendChild(link);
		link.click();
	}, `/admin/namespaces/floor-held-${STAMP}?ns=${NS}`);
	await expect.poll(() => asked.length).toBe(1);
	expect(asked[0]).toContain('Discard the namespace changes');
	await expect(description).toHaveValue('typed and not saved');
	expect(page.url()).toContain(`/admin/namespaces/${NS}`);
	await page.evaluate(() => document.querySelector('main > a:last-child')?.remove());
	await description.fill('Edited on the floor.');
	await page.getByTestId(`ns-edit-cancel-${NS}`).click();
	await expect(page.getByTestId(`namespace-edit-form-${NS}`)).toHaveCount(0);
});

test('dashboard: at the floor the counts are one row, the registers sit side by side, and the charts follow', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	for (const theme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: theme });
		await page.goto(url('/'));
		await expect(page.getByTestId('failure-table')).toBeVisible();
		await expect(page.getByTestId('viz-outcomes')).toBeVisible();
		await expect(page.getByTestId('viz-durations')).toBeVisible();
		await expect(page.getByTestId('viz-most-failing')).toHaveCount(0);
		const [failures, pools, running] = await Promise.all([
			page.getByTestId('failure-table').boundingBox(),
			page.getByTestId('pool-list').boundingBox(),
			page.getByTestId('running-list').boundingBox()
		]);
		expect(Math.abs(failures!.y - pools!.y)).toBeLessThan(2);
		expect(pools!.x).toBeGreaterThan(failures!.x + failures!.width - 1);
		expect(failures!.y).toBeGreaterThan(running!.y);
		// No register of the page's first screen needs a sideways scroll.
		expect(await overflows(page)).toEqual([]);
		await page.screenshot({ path: out(`screenshot-dashboard-floor-${theme}.png`), fullPage: true });
	}
	await page.emulateMedia({ colorScheme: 'light' });
	await page.setViewportSize({ width: 1280, height: 800 });
	const figures = await page.evaluate(() => ['stat-running', 'stat-p95'].map((id) => getComputedStyle(document.querySelector(`[data-testid="${id}"]`)!).fontSize));
	const h1 = await page.locator('main h1').evaluate((el) => parseFloat(getComputedStyle(el).fontSize));
	for (const size of figures) expect(parseFloat(size)).toBeLessThan(h1);
});

test('sidebar: the Runs glyph is its own, and the count of running runs follows a run that ends', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(url('/admin'));
	await expect(page.getByTestId('nav-runs').locator('svg use')).toHaveAttribute('href', '#i-runs');
	for (const theme of ['light', 'dark'] as const) {
		await page.emulateMedia({ colorScheme: theme });
		await page.getByTestId('sidebar').screenshot({ path: out(`screenshot-sidebar-${theme}.png`) });
	}
	await page.emulateMedia({ colorScheme: 'light' });

	// While it counts a run, it asks again at the stats cache's own pace, not every 30 s.
	const asked: number[] = [];
	page.on('request', (request) => {
		if (request.url().includes('StatsService/GetStats')) asked.push(Date.now());
	});
	await expect(page.getByTestId('nav-running')).toBeVisible();
	await expect.poll(() => asked.length, { timeout: 15_000 }).toBeGreaterThanOrEqual(2);

	await rpc('RunService/CancelRun', { namespace: NS, runId: longRun, reason: 'the sidebar should let go of it' });
	await waitFor(`run ${longRun} ended`, async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { namespace: NS, runId: longRun })).run?.status === 'STATUS_CANCELED', 120_000);
	await expect(page.getByTestId('nav-running')).toHaveCount(0, { timeout: 15_000 });
	note(`the sidebar asked ${asked.length} times while it counted a run, and let go of it once it ended`);
});

for (const [width, height] of [[1920, 1080], [2560, 1440], [3440, 1440]] as const) {
	test(`wide: every page at ${width}x${height} fills the width in both themes, and running text keeps its measure`, async ({ page }) => {
		test.setTimeout(5 * 60_000);
		await page.setViewportSize({ width, height });
		const found: string[] = [];
		for (const theme of ['light', 'dark'] as const) {
			await page.emulateMedia({ colorScheme: theme });
			for (const { path: p, ready, name } of floorPages()) {
				await page.goto(url(p));
				await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
				await expect(page.getByTestId(ready).first()).toBeVisible();
				const measured = await page.evaluate(() => {
					const root = document.documentElement;
					const main = document.querySelector('main#content')!;
					const box = main.getBoundingClientRect();
					const pad = parseFloat(getComputedStyle(main).paddingRight);
					const prose = [...main.querySelectorAll<HTMLElement>('p')].filter((el) => {
						const words = (el.innerText ?? '').split(/\s+/).length;
						return words > 25 && el.getBoundingClientRect().height > 0 && !el.closest('pre, code, table');
					});
					return {
						sideways: root.scrollWidth > root.clientWidth,
						// Of the column beside the sidebar, whose right edge the page's own scrollbar gutter sets.
						gap: Math.round(main.parentElement!.getBoundingClientRect().width - box.width),
						pad,
						tooWide: prose.filter((el) => el.getBoundingClientRect().width > 860).map((el) => `${el.dataset.testid ?? el.innerText.slice(0, 40)}: ${Math.round(el.getBoundingClientRect().width)}`),
						tooWideForms: [...main.querySelectorAll<HTMLElement>('form')].filter((f) => f.querySelectorAll('input, textarea, select').length >= 3 && f.getBoundingClientRect().width > 900).map((f) => `${f.dataset.testid ?? f.querySelector('input')?.dataset.testid ?? 'a form'}: ${Math.round(f.getBoundingClientRect().width)}`),
						tables: [...main.querySelectorAll<HTMLElement>('table')].filter((t) => t.getBoundingClientRect().width > 1 && t.parentElement!.getBoundingClientRect().width > 1).map((t) => ({ id: t.dataset.testid ?? 'a table', fill: t.getBoundingClientRect().width / t.parentElement!.getBoundingClientRect().width }))
					};
				});
				if (measured.sideways) found.push(`${p}: the page scrolls sideways`);
				if (measured.gap > 1) found.push(`${p}: ${measured.gap} px of unused width beside the content column`);
				for (const text of measured.tooWide) found.push(`${p}: running text is too wide: ${text}`);
				for (const form of measured.tooWideForms) found.push(`${p}: a form is too wide: ${form}`);
				for (const t of measured.tables) if (t.fill < 0.99) found.push(`${p}: ${t.id} fills ${Math.round(t.fill * 100)}% of its frame`);
				if (theme === 'light' || width === 3440) await page.screenshot({ path: out(`wide-${width}-${name}-${theme}.png`) });
			}
		}
		await page.emulateMedia({ colorScheme: 'light' });
		note(`wide ${width}: ${found.length} finding(s)${found.length ? ': ' + found.join('; ') : ''}`);
		expect(found).toEqual([]);
	});
}

test('wide: a run of a thousand tasks fills 3440 wide, its timeline with it, and what the width costs is recorded', async ({ page }) => {
	test.setTimeout(5 * 60_000);
	const big = `floor-big-${STAMP}`;
	const COUNT = 1000;
	await rpc('OrkflowService/ApplyOrkflow', {
		namespace: NS,
		yaml: [
			'apiVersion: orkstack/v1',
			'kind: Orkflow',
			`name: ${big}`,
			'version: "1.0.0"',
			'tasks:',
			'  - id: gate',
			'    await:',
			'      timeout_seconds: 3600',
			...Array.from({ length: COUNT - 1 }, (_, i) => [`  - id: t${i}`, '    uses: shell', '    needs: [gate]', '    network: none', '    with: {op: exec, command: [echo, ok]}']).flat(),
			''
		].join('\n')
	});
	const id = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { namespace: NS, orkflowName: big })).run.id;
	await waitFor(`run ${id} has started`, async () => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { namespace: NS, runId: id })).run?.status === 'STATUS_RUNNING', 120_000);
	const opened: Record<number, number[]> = {};
	// Each width twice, in turn, so neither is measured only with a cold browser cache.
	for (const width of [1280, 3440, 1280, 3440]) {
		await page.setViewportSize({ width, height: width === 1280 ? 800 : 1440 });
		const start = Date.now();
		await page.goto(url(`/runs/${id}`));
		await expect(page.getByTestId('run-status')).toBeVisible();
		await expect(page.getByTestId(`task-t${COUNT - 2}`)).toBeAttached({ timeout: 60_000 });
		(opened[width] ??= []).push(Date.now() - start);
		if (width === 3440) {
			const fit = await page.evaluate(() => {
				const root = document.documentElement;
				const main = document.querySelector('main#content')!;
				return { sideways: root.scrollWidth > root.clientWidth, gap: Math.round(main.parentElement!.getBoundingClientRect().width - main.getBoundingClientRect().width) };
			});
			expect(fit.sideways, 'the run page scrolls sideways').toBe(false);
			expect(fit.gap).toBeLessThanOrEqual(1);
		}
	}
	await page.screenshot({ path: out('wide-3440-big-run.png') });
	// Recorded, not bounded: the five-second budget is the ceiling specs', measured alone.
	note(`a ${COUNT}-task run page opened in ${opened[1280].join(', ')} ms at 1280 wide and ${opened[3440].join(', ')} ms at 3440 wide`);
	await rpc('RunService/CancelRun', { namespace: NS, runId: id, reason: 'measured' });
});
