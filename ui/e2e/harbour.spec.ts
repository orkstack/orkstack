import { expect, test, type Page } from './test';
import { mkdirSync, readFileSync } from 'node:fs';
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
	repoRoot,
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
import { selectTask } from './uiHelpers';

// Harbour's own behaviour, against a real engine, Orker, Temporal and object
// store (brief §12): every dialog (Jump, Cancel run…, Cordon…) and the
// editor's More disclosure, the editor's docs rail at 1440, 1280, 1220, 1100
// and 390, selecting a fan-out item, the live-updates control, the one
// tooltip, the asserted principal of a cancel, the drawer, and forced
// colours. Each test names the regression it catches.

const ENGINE_PORT = '18931';
const ENGINE = `http://127.0.0.1:${ENGINE_PORT}`;
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-harbour-${STAMP}`;
const BUCKET = ownBucket('harbour');
const FAN = `hb-fan-${STAMP}`;
const SLEEP = `hb-sleep-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'harbour') : path.join(uiDir, 'test-results', 'harbour');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

let fanRun = '';
let liveRun = '';
let canceledRun = '';

const runStatus = async (runId: string) => (await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId })).run?.status;

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
	spawnLogged(out('engine.log'), bin('ork'), [], { ...catalogEnv, ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ORK_HTTP_ADDR: `127.0.0.1:${ENGINE_PORT}`, ...uiEnv() });
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...s3Env, ORK_S3_BUCKET: BUCKET, ...orkerEnv('harbour') });
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${ENGINE}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	const fan = readFileSync(path.join(repoRoot, 'examples', 'each-fan-out.ork.yaml'), 'utf8').replace(/^name: each-fan-out$/m, `name: ${FAN}`);
	await rpc('OrkflowService/ApplyOrkflow', { yaml: fan });
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: [
			'apiVersion: orkstack/v1',
			'kind: Orkflow',
			`name: ${SLEEP}`,
			'version: "1.0.0"',
			'tasks:',
			'  - id: wait',
			'    uses: shell',
			'    network: none',
			'    with: {op: exec, command: [sleep, "600"]}',
			''
		].join('\n')
	});
	fanRun = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: FAN })).run.id;
	liveRun = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: SLEEP })).run.id;
	canceledRun = (await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: SLEEP })).run.id;
	await waitFor(`fan-out ${fanRun} succeeded`, async () => (await runStatus(fanRun)) === 'STATUS_SUCCEEDED', 240_000);
	await waitFor(`${liveRun} is running`, async () => (await runStatus(liveRun)) === 'STATUS_RUNNING', 120_000);
	// A cancel the gateway asserted a principal for: the run must say who asked, and that it was asserted.
	const res = await fetch(`${ENGINE}/orkstack.v1.RunService/CancelRun`, {
		method: 'POST',
		headers: { 'content-type': 'application/json', 'Ork-Principal': 'dana@acme' },
		body: JSON.stringify({ runId: canceledRun, reason: 'harbour spec' })
	});
	if (!res.ok) throw new Error(`CancelRun: ${res.status} ${await res.text()}`);
	await waitFor(`${canceledRun} canceled`, async () => (await runStatus(canceledRun)) === 'STATUS_CANCELED', 120_000);
	note(`engine ${ENGINE}; fan-out ${fanRun}, live ${liveRun}, canceled ${canceledRun}`);
});

test.afterAll(async () => {
	try {
		if (liveRun) await rpc('RunService/CancelRun', { runId: liveRun });
	} catch (err) {
		note(`cancel of ${liveRun} at teardown failed: ${err}`);
	}
	await killStarted();
	removeBucket(BUCKET, note);
});

const at = (p: string) => `${ENGINE}${p}${p.includes('?') ? '&' : '?'}ns=default`;

async function shot(page: Page, name: string): Promise<void> {
	await page.screenshot({ path: out(name), fullPage: true });
}

test('Jump (Ctrl K) is a modal dialog named "Jump to" that counts its matches live and gives focus back', async ({ page }) => {
	// Regression: the jump dialog opened without being modal, or left focus on the body when it closed.
	await page.goto(at('/runs'));
	const jump = page.getByRole('button', { name: 'Jump to a run, Orkflow or page' });
	await jump.focus();
	await page.keyboard.press('Control+k');
	const dialog = page.getByRole('dialog', { name: 'Jump to' });
	await expect(dialog).toBeVisible();
	await expect(page.getByTestId('command-palette-input')).toBeFocused();
	await page.keyboard.press('Escape');
	await expect(dialog).toHaveCount(0);
	await expect(jump).toBeFocused();
	// A match opens: the count says there is one or more, and Enter goes there.
	await page.keyboard.press('Control+k');
	await page.getByTestId('command-palette-input').fill('Pools');
	await expect(page.getByTestId('command-palette-status')).toHaveText(/^[1-9]\d* results?$/);
	await page.keyboard.press('Enter');
	await expect(page).toHaveURL(/\/admin\/pools\?/);
});

test('Cancel run… asks in a reason dialog: the safe choice has the focus, Escape keeps the run running, focus returns', async ({ page }) => {
	// Regression: a destructive confirm took Enter by default, or Escape canceled the run it was meant to keep.
	await page.goto(at(`/runs/${liveRun}`));
	const trigger = page.getByRole('button', { name: 'Cancel run…' });
	await trigger.click();
	const dialog = page.getByRole('dialog', { name: /^Cancel run / });
	await expect(dialog).toBeVisible();
	await expect(page.getByRole('button', { name: 'Keep running' })).toBeFocused();
	await expect(page.getByLabel('Reason (optional)')).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(dialog).toHaveCount(0);
	await expect(trigger).toBeFocused();
	// No cancel was asked for: the trigger still offers it, and no request is noted.
	await expect(trigger).toHaveText('Cancel run…');
	await expect(page.getByTestId('cancellation-requested')).toHaveCount(0);
	await trigger.click();
	await expect(dialog.getByRole('button', { name: 'Cancel run', exact: true })).toBeEnabled();
	await page.getByRole('button', { name: 'Keep running' }).click();
	await expect(dialog).toHaveCount(0);
	await expect(trigger).toHaveText('Cancel run…');
	await expect(page.getByTestId('cancellation-requested')).toHaveCount(0);
	// The confirm with a reason cancels it, and a cancel nobody asserted shows no principal.
	await trigger.click();
	await page.getByLabel('Reason (optional)').fill('harbour spec: confirm path');
	await dialog.getByRole('button', { name: 'Cancel run', exact: true }).click();
	await expect(page.getByTestId('run-status')).toHaveText('Canceled', { timeout: 120_000 });
	await expect(page.getByTestId('run-canceled-by')).toHaveCount(0);
	liveRun = '';
});

test('a cancel shows who asked for it, always marked asserted, on the run and in the runs list', async ({ page }) => {
	// Regression: a gateway's principal rendered as a verified identity (AGENTS.md §7).
	await page.goto(at(`/runs/${canceledRun}`));
	await expect(page.getByTestId('run-status')).toHaveText('Canceled');
	const by = page.getByTestId('run-canceled-by');
	await expect(by).toContainText('dana@acme');
	await expect(by).toContainText('asserted');
	await expect(by).toContainText('not verified by OrkStack');
	await page.goto(at('/runs'));
	const row = page.getByTestId(`run-row-canceled-by-${canceledRun}`);
	await expect(row).toContainText('dana@acme');
	await expect(row).toContainText('asserted');
});

test('the live control swaps its name, announces its state, explains itself on focus, and Escape hides only the tip', async ({ page }) => {
	// Regression: "Pause updates, pressed" read twice, or the tip could not be reached or dismissed (WCAG 1.4.13).
	await page.goto(at('/runs'));
	const live = page.getByTestId('runs-live');
	const toggle = page.getByRole('button', { name: 'Pause updates' });
	await expect(live.getByRole('status')).toHaveText(/Live$/);
	await expect(toggle).not.toHaveAttribute('aria-pressed', /.*/);
	await toggle.focus();
	await page.keyboard.press('Shift+Tab');
	await page.keyboard.press('Tab');
	const tip = page.getByRole('tooltip');
	await expect(tip).toHaveText('Pause live updates to this page. The run keeps going; resume to catch up.');
	await expect(toggle).toHaveAccessibleDescription('Pause live updates to this page. The run keeps going; resume to catch up.');
	await page.keyboard.press('Escape');
	await expect(tip).toBeHidden();
	await expect(toggle).toBeFocused();
	await page.keyboard.press('Enter');
	await expect(page.getByRole('button', { name: 'Resume updates' })).toBeFocused();
	await expect(live.getByRole('status')).toHaveText(/Paused$/);
	await page.getByRole('button', { name: 'Resume updates' }).click();
	await expect(live.getByRole('status')).toHaveText(/Live$/);
	await shot(page, 'live-control.png');
});

test('a hovered tooltip stays while the pointer crosses onto it, the next tip follows a sweep, and no element carries a native title', async ({ page }) => {
	// Regression: native titles came back beside the one tooltip, or the tip vanished as the pointer moved onto it.
	await page.goto(at(`/runs/${fanRun}`));
	const copy = page.getByRole('button', { name: 'Copy run id' });
	await copy.hover();
	const tip = page.getByRole('tooltip');
	await expect(tip).toHaveText('Copy run id');
	await tip.hover();
	await expect(tip).toBeVisible();
	// Sweeping straight on to the next tipped control shows its tip: the old
	// tip's pending hide never cancels the new one's show.
	await page.getByTestId('run-progress').hover();
	await expect(tip).toHaveText(/tasks and items ended|tasks ended/);
	await page.mouse.move(5, 5);
	await expect(tip).toBeHidden();
	expect(await page.locator('body [title]:not(svg *)').count()).toBe(0);
});

test('a fan-out item is selected by its own button, and only that one is pressed', async ({ page }) => {
	// Regression: selecting an item left another pressed, or the item was a div no keyboard could reach.
	await page.goto(at(`/runs/${fanRun}`));
	// A settled fan-out opens its item grid when it is the selection.
	await selectTask(page, 'convert');
	const first = page.getByTestId('task-convert-item-0');
	const second = page.getByTestId('task-convert-item-1');
	await second.focus();
	await page.keyboard.press('Enter');
	await expect(second).toHaveAttribute('aria-pressed', 'true');
	await expect(first).toHaveAttribute('aria-pressed', 'false');
	await expect(page.getByTestId('task-convert-item-panel')).toBeVisible();
	// Selecting another releases the first: one item is pressed at a time.
	await first.focus();
	await page.keyboard.press('Enter');
	await expect(first).toHaveAttribute('aria-pressed', 'true');
	await expect(second).toHaveAttribute('aria-pressed', 'false');
});

const editorAt = (flow: string) => at(`/orkflows/${flow}?tab=edit`);

for (const [width, expanded, where] of [
	[1440, 'true', 'beside'],
	[1280, 'true', 'beside'],
	[1220, 'false', 'beside'],
	[1100, 'false', 'a bar under']
] as const) {
	test(`the editor at ${width} px: the docs rail is ${where} the card and ${expanded === 'true' ? 'open' : 'collapsed'}`, async ({ page }) => {
		// Regression: the rail overlapped the code, or opened over it on a laptop (layout C).
		await page.setViewportSize({ width, height: 900 });
		await page.goto(editorAt(FAN));
		await editorReady(page, 'editor-yaml');
		const toggle = page.getByTestId('editor-docs-toggle');
		await expect(toggle).toHaveAttribute('aria-expanded', expanded);
		const card = await page.getByRole('region', { name: 'Edit' }).boundingBox();
		const rail = await page.getByTestId('editor-docs').boundingBox();
		expect(card && rail).toBeTruthy();
		if (where === 'beside') expect(rail!.x).toBeGreaterThanOrEqual(card!.x + card!.width);
		else {
			// One column: the collapsed docs are a bar as wide as the card, held at the viewport's foot.
			expect(Math.abs(rail!.x - card!.x)).toBeLessThan(2);
			expect(rail!.y + rail!.height).toBeLessThanOrEqual(901);
		}
		await toggle.click();
		await expect(toggle).toHaveAttribute('aria-expanded', expanded === 'true' ? 'false' : 'true');
		// The docs body follows the toggle.
		const body = page.getByRole('region', { name: /^Docs for/ });
		if (expanded === 'true') await expect(body).toBeHidden();
		else await expect(body).toBeVisible();
		await shot(page, `editor-${width}.png`);
	});
}

test('the editor rail follows the caret to the task it is in', async ({ page }) => {
	// Regression: the docs showed the first task whatever line the caret was on.
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(editorAt(FAN));
	await editorReady(page, 'editor-yaml');
	await page.getByText('uses: shell').first().click();
	await expect(page.getByTestId('editor-docs')).toContainText('convert uses shell');
	await expect(page.getByRole('button', { name: 'Docs for convert' })).toHaveAttribute('aria-expanded', 'true');
});

test('at 390 px Import and Export fold into More, a plain disclosure that Escape closes', async ({ page }) => {
	// Regression: the editor header clipped its buttons on a phone, or More trapped focus.
	await page.setViewportSize({ width: 390, height: 844 });
	await page.goto(editorAt(FAN));
	await editorReady(page, 'editor-yaml');
	await expect(page.getByTestId('editor-export')).toBeHidden();
	const more = page.getByRole('button', { name: 'More actions' });
	await expect(more).toHaveAttribute('aria-expanded', 'false');
	await more.click();
	await expect(more).toHaveAttribute('aria-expanded', 'true');
	const menu = page.getByTestId('editor-more-menu');
	await expect(menu.getByRole('button', { name: 'Export' })).toBeVisible();
	await expect(menu.getByText('Import')).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(more).toHaveAttribute('aria-expanded', 'false');
	await expect(more).toBeFocused();
	await shot(page, 'editor-390.png');
});

test('Cordon… cannot confirm until a reason is given, and Back leaves the Orker serving', async ({ page }) => {
	// Regression: a cordon went through with no reason, or its confirm drew red (red is a status colour only).
	await page.goto(at('/admin/pools'));
	// A pool's Orkers are shown by default, each with Cordon….
	await expect(page.getByRole('button', { name: 'Hide Orkers' }).first()).toHaveAttribute('aria-expanded', 'true');
	const trigger = page.getByRole('button', { name: 'Cordon…' }).first();
	await trigger.click();
	const dialog = page.getByRole('dialog', { name: /^Cordon / });
	await expect(dialog).toBeVisible();
	const reason = page.getByLabel('Reason (required)');
	await expect(reason).toHaveAttribute('aria-required', 'true');
	await expect(reason).toHaveAttribute('aria-invalid', 'true');
	const confirm = dialog.getByRole('button', { name: 'Cordon', exact: true });
	await expect(confirm).toBeDisabled();
	await expect(confirm).toHaveAccessibleDescription('Give a reason to cordon this Orker.');
	await reason.fill('draining for a kernel update');
	await expect(confirm).toBeEnabled();
	await expect(reason).not.toHaveAttribute('aria-invalid', /.*/);
	await dialog.getByRole('button', { name: 'Back' }).click();
	await expect(dialog).toHaveCount(0);
	await expect(trigger).toBeFocused();
	// Back cordoned nothing: the Orker still offers Cordon…, never Uncordon.
	await expect(trigger).toHaveText('Cordon…');
	await expect(page.getByRole('button', { name: 'Uncordon' })).toHaveCount(0);
});

test('below 900 px the sidebar is a drawer: Escape closes it and focus returns to Menu', async ({ page }) => {
	// Regression: the drawer could not be closed from the keyboard, or left focus nowhere.
	await page.setViewportSize({ width: 800, height: 900 });
	await page.goto(at('/runs'));
	const menu = page.getByRole('button', { name: 'Menu' });
	const side = page.getByRole('navigation', { name: 'Main' });
	await expect(side.getByRole('link', { name: /^Runs/ })).toBeHidden();
	await expect(menu).toHaveAttribute('aria-expanded', 'false');
	await menu.click();
	await expect(menu).toHaveAttribute('aria-expanded', 'true');
	await expect(side.getByRole('link', { name: /^Runs/ })).toBeVisible();
	await page.keyboard.press('Escape');
	await expect(menu).toHaveAttribute('aria-expanded', 'false');
	await expect(menu).toBeFocused();
	await expect(side.getByRole('link', { name: /^Runs/ })).toBeHidden();
});

test('forced colours: every status keeps its word and the live control its state', async ({ page }) => {
	// Regression: in forced colours a status was only a colour, and the colour was gone.
	await page.emulateMedia({ forcedColors: 'active' });
	await page.goto(at('/runs'));
	await expect(page.getByTestId(`run-row-status-${fanRun}`)).toHaveText('Succeeded');
	await expect(page.getByTestId(`run-row-status-${canceledRun}`)).toHaveText('Canceled');
	await expect(page.getByTestId('runs-live').getByRole('status')).toHaveText(/Live$/);
	await shot(page, 'forced-colours-runs.png');
	await page.goto(at(`/runs/${fanRun}`));
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded');
	await shot(page, 'forced-colours-run.png');
});
