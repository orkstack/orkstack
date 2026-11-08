import { expect, test, type Page } from './test';
import { mkdirSync, readdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	clusterEnv,
	ensureBucket,
	killStarted,
	noteTo,
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
import { pageCli } from './uiHelpers';

// The one terminal icon, on every route (ui/DESIGN.md, the Shell Icon Rule): a
// page has exactly one command-line icon listing its commands, and prints no
// `orkctl` on the page itself. A row's or a dialog's own icon stays with it,
// because it carries that row's values, so it is not counted: what counts as
// "its own" is where it stands in the page (a table row, a list item, a modal,
// or a task's panel on the run page, marked data-cli-own), never which
// component drew it, so a page-level icon cannot hide by its shape.
//
// `the table names every route` fails when a route is added without an entry
// here, so a new page cannot skip the rule. `the rule sees a second icon` is
// the positive control: with an extra icon in the page's flow, the count is 2.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, bucket and port; it
// ends only the process it started. The Orkflow is an awaited task, so no Orker
// and no image is involved.

const PORT = 19031;
const ENGINE = `http://127.0.0.1:${PORT}`;
const UI = ENGINE;
const BUCKET = ownBucket('cli-icon-rule');
const STAMP = Date.now().toString(36);
const NS = `icons-${STAMP}`;
const HELD = `icons-held-${STAMP}`;
const NAME = 'waits';
const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'cli-icon-rule') : path.join(uiDir, 'test-results', 'cli-icon-rule');
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

let waiting = '';
let failed = '';

async function submit(namespace: string): Promise<string> {
	const answer = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME, namespace });
	return answer.run.id;
}

test.beforeAll(async () => {
	test.setTimeout(180_000);
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (await tcpOpen(PORT)) throw new Error(`port ${PORT} already in use`);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...clusterEnv(temporalNamespace('cli-icon-rule')),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: `127.0.0.1:${PORT}`,
		ORK_ADMIN_ADDR: '127.0.0.1:0',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS } });
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: HELD, hold: { reason: 'billing review' } } });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: ORKFLOW, namespace: NS });
	// A second version, so the Versions tab has a comparison to list.
	await rpc('OrkflowService/ApplyOrkflow', { yaml: ORKFLOW.replace('0.1.0', '0.2.0'), namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: ORKFLOW, namespace: HELD });
	await rpc('WebhookService/RegisterWebhook', { url: 'https://example.com/hook', events: [] });
	await rpc('ScheduleService/CreateSchedule', { namespace: NS, name: 'nightly', orkflowName: NAME, spec: { cron: ['0 2 * * *'], timezone: 'UTC' }, paused: true });
	await rpc('TriggerService/CreateTrigger', { namespace: NS, orkflowName: NAME, description: 'icons' });
	waiting = await submit(NS);
	failed = await submit(NS);
	// Rejected, so the dashboard has failures and draws the panels that fold.
	await waitFor('the await to open', async () => {
		try {
			await rpc('RunService/ResolveAwait', { namespace: NS, runId: failed, taskId: 'approval', reject: true, reason: 'no' });
			return true;
		} catch {
			return false;
		}
	});
	await waitFor('the rejected run to fail', async () => {
		const answer = await rpc<{ run: { status: string } }>('RunService/GetRun', { runId: failed, namespace: NS });
		return answer.run.status === 'STATUS_FAILED';
	});
	note(`namespaces ${NS}, ${HELD}; runs ${waiting} (waiting), ${failed} (failed)`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test.beforeEach(async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
});

/** Every CLI icon in the page's flow: not in a row, a list item, a modal or a task's panel. */
function pageLevelIcons(page: Page): Promise<string[]> {
	return page.locator('[data-cli-icon]').evaluateAll((els) =>
		els.filter((el) => !el.closest('tr, li, dialog, [data-cli-own]')).map((el) => el.getAttribute('data-testid') ?? '(no testid)')
	);
}

interface Visit {
	/** The route's source directory under src/routes, for the coverage check. */
	route: string;
	name: string;
	url: () => string;
	/** On the page once its data is. */
	ready: string;
	/** The page's one icon. */
	icon: string;
	/** Labels its popover must hold. The list is checked only when the icon is page-cli. */
	labels?: string[];
	/** Icons of its own a row must still have on this page, so "not counted" is not vacuous. */
	rowIcon?: string;
	/** Authored text the page shows as data (the example gallery's descriptions), not the page's own words. */
	authored?: string;
}

const VISITS: Visit[] = [
	{ route: '', name: 'dashboard', url: () => `/?ns=${NS}`, ready: 'failure-fingerprints', icon: 'page-cli', labels: ['The numbers on this page', 'Tasks that needed a retry', 'What this namespace reserved'] },
	{ route: '', name: 'dashboard, on hold', url: () => `/?ns=${HELD}`, ready: 'dashboard-hold-banner', icon: 'page-cli', labels: ['The numbers on this page', 'Release the hold'] },
	{ route: 'runs', name: 'runs', url: () => `/runs?ns=${NS}`, ready: 'runs-cli-trigger', icon: 'runs-cli' },
	{ route: 'runs/[id]', name: 'run, waiting', url: () => `/runs/${waiting}?ns=${NS}`, ready: 'run-inspect-cli-trigger', icon: 'run-inspect-cli' },
	{ route: 'runs/[id]', name: 'run, failed', url: () => `/runs/${failed}?ns=${NS}`, ready: 'run-inspect-cli-trigger', icon: 'run-inspect-cli' },
	{ route: 'runs/[id]/compare', name: 'compare', url: () => `/runs/${failed}/compare?to=${waiting}&ns=${NS}`, ready: 'page-cli-trigger', icon: 'page-cli', labels: ['This comparison'] },
	{ route: 'orkflows', name: 'orkflows', url: () => `/orkflows?ns=${NS}`, ready: `orkflow-row-${NAME}`, icon: 'page-cli', labels: ['The Orkflows'] },
	{ route: 'orkflows', name: 'orkflows, on hold', url: () => `/orkflows?ns=${HELD}`, ready: 'orkflows-hold-banner', icon: 'page-cli', labels: ['The Orkflows', 'Release the hold'] },
	{ route: 'orkflows/[name]', name: 'orkflow overview', url: () => `/orkflows/${NAME}?ns=${NS}`, ready: 'orkflow-drawing', icon: 'page-cli', labels: ['This Orkflow'] },
	{ route: 'orkflows/[name]', name: 'orkflow versions', url: () => `/orkflows/${NAME}?ns=${NS}&tab=versions`, ready: 'page-cli-trigger', icon: 'page-cli', labels: ['Its versions', 'Diff these versions', 'Compare their interfaces', 'What calls it'] },
	{ route: 'orkflows/[name]', name: 'orkflow edit', url: () => `/orkflows/${NAME}?ns=${NS}&tab=edit`, ready: 'editor-cli-trigger', icon: 'editor-cli' },
	{ route: 'orkflows/[name]', name: 'orkflow schedules', url: () => `/orkflows/${NAME}?ns=${NS}&tab=schedules`, ready: 'schedule-row-nightly', icon: 'page-cli', labels: ['Its schedules'], rowIcon: 'schedule-fire-cli-nightly-trigger' },
	{ route: 'orkflows/[name]', name: 'orkflow triggers', url: () => `/orkflows/${NAME}?ns=${NS}&tab=triggers`, ready: 'trigger-list', icon: 'page-cli', labels: ['Its triggers'] },
	{ route: 'apply', name: 'apply', url: () => `/apply?ns=${NS}`, ready: 'apply-namespace', icon: 'page-cli', labels: ['Apply it'], authored: '[aria-labelledby="apply-examples-heading"]' },
	{ route: 'admin', name: 'operate', url: () => `/admin?ns=${NS}`, ready: 'system-info', icon: 'page-cli', labels: ["This replica's configuration and build"] },
	{ route: 'admin/activities', name: 'activities', url: () => `/admin/activities?ns=${NS}`, ready: 'activities-cli-trigger', icon: 'activities-cli' },
	{ route: 'admin/audit', name: 'audit', url: () => `/admin/audit?ns=${NS}`, ready: 'audit-scope', icon: 'page-cli', labels: ['This listing'] },
	{ route: 'admin/automation', name: 'automation', url: () => `/admin/automation?ns=${NS}`, ready: 'automation-scope', icon: 'page-cli', labels: ['Export this namespace', 'Plan the document'] },
	{ route: 'admin/cache', name: 'cache', url: () => `/admin/cache?ns=${NS}`, ready: 'cache-scope', icon: 'page-cli', labels: ['How cached tasks fared'] },
	{ route: 'admin/image-policy', name: 'image policy', url: () => `/admin/image-policy?ns=${NS}`, ready: 'page-cli-trigger', icon: 'page-cli', labels: ['The policy', 'Change it'] },
	{ route: 'admin/namespaces/[name]', name: 'one namespace', url: () => `/admin/namespaces/${NS}?ns=${NS}`, ready: 'namespace-facts', icon: 'page-cli', labels: ['This namespace', 'Set this budget'] },
	{ route: 'admin/namespaces', name: 'namespaces', url: () => `/admin/namespaces?ns=${NS}`, ready: 'namespace-list', icon: 'page-cli', labels: ['The namespaces'] },
	{ route: 'admin/pools', name: 'pools', url: () => `/admin/pools?ns=${NS}`, ready: 'pools-cli-trigger', icon: 'pools-cli' },
	{ route: 'admin/releases', name: 'releases', url: () => `/admin/releases?ns=${NS}`, ready: 'releases-scope', icon: 'page-cli', labels: ['The releases'] },
	{ route: 'admin/retention', name: 'retention', url: () => `/admin/retention?ns=${NS}`, ready: 'page-cli-trigger', icon: 'page-cli', labels: ['The maximum', 'Change it'] },
	{ route: 'admin/usage', name: 'usage', url: () => `/admin/usage?ns=${NS}`, ready: 'usage-admin-scope', icon: 'page-cli', labels: ['What each namespace reserved', 'The namespaces'] },
	{ route: 'admin/webhooks', name: 'webhooks', url: () => `/admin/webhooks?ns=${NS}`, ready: 'webhook-list', icon: 'page-cli', labels: ['The subscriptions', 'The deliveries'] }
];

/** Every directory under src/routes with a +page.svelte, relative to it. */
function sourceRoutes(dir = path.join(uiDir, 'src', 'routes'), prefix = ''): string[] {
	return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
		if (entry.isDirectory()) return sourceRoutes(path.join(dir, entry.name), prefix ? `${prefix}/${entry.name}` : entry.name);
		return entry.name === '+page.svelte' ? [prefix] : [];
	});
}

test('the table names every route', () => {
	expect([...new Set(VISITS.map((v) => v.route))].sort()).toEqual(sourceRoutes().sort());
});

test('every route has exactly one page-level CLI icon, and prints no orkctl', async ({ page }) => {
	test.setTimeout(5 * 60_000);
	for (const visit of VISITS) {
		await page.goto(`${UI}${visit.url()}`);
		await expect(page.getByTestId(visit.ready).first(), visit.name).toBeVisible();
		await expect(page.getByTestId(`${visit.icon}-trigger`), visit.name).toBeVisible();
		// Whatever renders after its first data has had the chance to add an icon.
		await page.waitForLoadState('networkidle').catch(() => undefined);
		const icons = await pageLevelIcons(page);
		note(`${visit.name}: ${icons.join(', ')}`);
		expect(icons, `${visit.name}: its page-level icons`).toEqual([`${visit.icon}-trigger`]);
		if (visit.rowIcon) await expect(page.getByTestId(visit.rowIcon), `${visit.name}: a row keeps its own`).toBeVisible();
		const words = await page.locator('#content').evaluate((content, authored) => {
			const copy = content.cloneNode(true) as HTMLElement;
			if (authored) copy.querySelectorAll(authored).forEach((el) => el.remove());
			return copy.textContent ?? '';
		}, visit.authored);
		expect(words, `${visit.name}: the Shell Icon Rule`).not.toContain('orkctl');

		await page.getByTestId(`${visit.icon}-trigger`).click();
		await expect(page.getByTestId(`${visit.icon}-popover`), visit.name).toBeVisible();
		for (const label of visit.labels ?? []) await expect(pageCli(page, label), `${visit.name}: ${label}`).toContainText(/\S/);
		await page.getByTestId(`${visit.icon}-popover`).evaluate((panel) => Promise.all(panel.getAnimations().map((a) => a.finished)));
		await page.screenshot({ path: out(`${visit.name.replace(/[ ,]+/g, '-')}.png`) });
		await page.keyboard.press('Escape');
	}
});

test("a notice's own icon is not a second page-level icon", async ({ page }) => {
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}&tab=triggers`);
	await expect(page.getByTestId('trigger-list')).toBeVisible();
	await page.getByTestId('trigger-create-submit').click();
	await expect(page.getByTestId('trigger-secret-cli-trigger')).toBeVisible();
	expect(await pageLevelIcons(page)).toEqual(['page-cli-trigger']);
});

test('the rule sees a second icon in the page flow', async ({ page }) => {
	await page.goto(`${UI}/admin/retention?ns=${NS}`);
	await expect(page.getByTestId('page-cli-trigger')).toBeVisible();
	expect(await pageLevelIcons(page)).toEqual(['page-cli-trigger']);
	await page.getByTestId('page-cli-trigger').evaluate((button) => {
		const second = button.cloneNode(true) as HTMLElement;
		second.setAttribute('data-testid', 'second-cli-trigger');
		document.getElementById('content')!.append(second);
	});
	expect(await pageLevelIcons(page)).toEqual(['page-cli-trigger', 'second-cli-trigger']);
});

test('a row, a list item and a modal keep their own icons, carrying their own values', async ({ page }) => {
	await page.goto(`${UI}/admin/webhooks?ns=${NS}`);
	await page.locator('[data-testid^="edit-webhook-"]').first().click();
	const dialog = page.getByTestId('webhook-edit-dialog');
	await expect(dialog.locator('[data-cli-icon]')).toHaveCount(1);
	await dialog.getByTestId('webhook-edit-cli-trigger').click();
	await expect(page.getByTestId('webhook-edit-cli-command')).toContainText('webhook update wh-');
	await page.keyboard.press('Escape');
	await page.goto(`${UI}/orkflows/${NAME}?ns=${NS}&tab=schedules`);
	await page.getByTestId('schedule-fire-cli-nightly-trigger').click();
	await expect(page.getByTestId('schedule-fire-cli-nightly-command')).toContainText('schedule fire nightly');
});
