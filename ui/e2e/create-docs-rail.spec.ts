import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	activityRef,
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
import { expectDocsBarHeldAtFoot } from './uiHelpers';
import { fillYaml } from './yaml';

// The Create Orkflow page's docs rail (EditorLayout, EditorDocsRail), against
// a real engine reading the first-party images' declared interfaces: the
// rail beside the editor shows the activity of the task the caret is in,
// follows the caret to a task of another image, opens and shuts with its
// toggle, and starts shut below the 1280 px floor, as the Edit tab's does.

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('create-docs-rail');
const ENGINE = 'http://127.0.0.1:18645';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('create-docs-rail');
const NS = `create-docs-rail-${Date.now().toString(36)}`;
const proof = proofOut(
	process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'create-docs-rail') : path.join(uiDir, 'test-results', 'create-docs-rail')
);
const note = noteTo(proof('transcript.txt'));
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

// Two tasks of two first-party images: `fetch` (http, its default op) on
// lines 6–9 and `convert` (shell's op script) on lines 10–15, each setting
// one key of its op.
const DOC = [
	'apiVersion: orkstack/v1',
	'kind: Orkflow',
	'name: create-docs-rail',
	'version: "0.1.0"',
	'tasks:',
	'  - id: fetch',
	'    uses: http',
	'    with:',
	'      url: https://example.com',
	'  - id: convert',
	'    uses: shell',
	'    needs: [fetch]',
	'    with:',
	'      op: script',
	'      script: echo converted',
	''
].join('\n');

const createPage = `${UI}/orkflows/+new`;
const rail = (page: Page) => page.getByTestId('editor-docs');
const toggle = (page: Page) => page.getByTestId('editor-docs-toggle');
const railBody = (page: Page) => page.getByRole('region', { name: /^Docs for/ });
const card = (page: Page) => page.getByRole('region', { name: 'Orkflow YAML' });
/** Puts the caret at the end of the editor line that holds `text`. */
const caretOn = (page: Page, text: string) => page.locator('.cm-line', { hasText: text }).click();
/** Every reference the page asks InspectActivity about, in order, from the moment it is called. */
function inspected(page: Page): string[] {
	const asked: string[] = [];
	page.on('request', (request) => {
		if (request.url().includes('/orkstack.v1.ActivityService/InspectActivity')) asked.push(String(request.postDataJSON()?.reference ?? ''));
	});
	return asked;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18645)) throw new Error('port 18645 already in use');
	ensureActivityImages();
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18645',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('built UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'create docs rail' } });
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('the docs rail beside the Create editor shows the activity of the task at the caret, and follows it', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	const asked = inspected(page);
	await page.goto(`${createPage}?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', DOC);
	// Open at a desktop width, beside the card.
	await expect(toggle(page)).toHaveAttribute('aria-expanded', 'true');
	const box = await card(page).boundingBox();
	const railBox = await rail(page).boundingBox();
	expect(box && railBox).toBeTruthy();
	expect(railBox!.x).toBeGreaterThanOrEqual(box!.x + box!.width);
	expect(Math.round(railBox!.width)).toBe(340);

	// The caret in `fetch`: the http image's own description, its keys, and
	// which it sets; the subtitle names the line the task starts on.
	await caretOn(page, 'uses: http');
	await expect(rail(page)).toContainText('fetch uses http');
	await expect(page.getByTestId('editor-docs-keys')).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('editor-docs-summary')).not.toBeEmpty();
	const url = page.getByTestId('editor-docs-field-url');
	await expect(url.locator('.kb-key')).toHaveText('url');
	await expect(url).toContainText(/\bset\b/);
	await expect(page.getByTestId('editor-docs-field-script')).toHaveCount(0);
	await expect(page.getByTestId('editor-docs-sub')).toHaveText(/^line 6 · 1 of \d+ set$/);
	note(`fetch: ${(await rail(page).innerText()).split('\n').slice(0, 4).join(' | ')}`);
	await shot(page, 'create-docs-http-1440.png');

	// The caret in `convert`: the rail follows it to the shell image.
	await caretOn(page, 'uses: shell');
	await expect(rail(page)).toContainText('convert uses shell');
	await expect(page.getByTestId('editor-docs-field-script')).toBeVisible({ timeout: 30_000 });
	await expect(page.getByTestId('editor-docs-field-script')).toContainText(/\bset\b/);
	await expect(page.getByTestId('editor-docs-field-url')).toHaveCount(0);
	await expect(page.getByTestId('editor-docs-sub')).toHaveText(/^line 10 · 1 of \d+ set$/);
	await expect(page.getByRole('button', { name: 'Docs for convert' })).toHaveAttribute('aria-expanded', 'true');
	note(`convert: ${(await rail(page).innerText()).split('\n').slice(0, 4).join(' | ')}`);
	await shot(page, 'create-docs-shell-1440.png');

	// Outside every task, once the rail has followed the caret there: it asks for one.
	await caretOn(page, 'name: create-docs-rail');
	await expect(page.getByTestId('editor-docs-empty')).toBeVisible();
	await expect(rail(page)).not.toContainText('convert uses shell');

	// Back in `fetch`: the answer it already had, with no new read.
	await caretOn(page, 'url: https://example.com');
	await expect(rail(page)).toContainText('fetch uses http');
	await expect(page.getByTestId('editor-docs-field-url')).toBeVisible();
	// One read for each image, whichever the caret reached first.
	expect([...asked].sort()).toEqual(['http', 'shell']);
});

test('a reference that does not resolve is read once, not at every rest of the caret', async ({ page }) => {
	// Regression: a failed read was forgotten, so each rest of the caret in the
	// task asked again and flashed "Reading…" over the error.
	const missing = activityRef('no-such-activity');
	await page.setViewportSize({ width: 1440, height: 900 });
	const asked = inspected(page);
	await page.goto(`${createPage}?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', DOC.replace('uses: shell', `uses: ${missing}`));
	await caretOn(page, `uses: ${missing}`);
	await expect(page.getByTestId('editor-docs-error')).toBeVisible({ timeout: 30_000 });
	note(`unresolved: ${await page.getByTestId('editor-docs-error').innerText()}`);
	// Away to `fetch`, and back to two other lines of the same task.
	await caretOn(page, 'uses: http');
	await expect(rail(page)).toContainText('fetch uses http');
	await caretOn(page, 'script: echo converted');
	await expect(page.getByTestId('editor-docs-error')).toBeVisible();
	await caretOn(page, 'needs: [fetch]');
	await expect(page.getByTestId('editor-docs-sub')).toHaveText('line 10');
	await expect(page.getByTestId('editor-docs-error')).toBeVisible();
	await caretOn(page, 'uses: http');
	await expect(rail(page)).toContainText('fetch uses http');
	expect(asked.filter((reference) => reference === missing)).toHaveLength(1);
	await shot(page, 'create-docs-unresolved-1440.png');
});

test('a task that runs no activity says so, and a half-typed one that it names none yet', async ({ page }) => {
	// Regression: the rail asked to place the caret in a task while it was in one.
	const kinds = [
		'apiVersion: orkstack/v1',
		'kind: Orkflow',
		'name: create-docs-kinds',
		'version: "0.1.0"',
		'tasks:',
		'  - id: approve',
		'    await:',
		'      timeout_seconds: 60',
		'  - id: pause',
		'    sleep:',
		'      seconds: 5',
		'  - id: notify',
		'    uses: orkflow:notify-block@0.2.1',
		'  - id: draft',
		'    with:',
		'      url: https://example.com',
		''
	].join('\n');
	await page.setViewportSize({ width: 1440, height: 900 });
	const asked = inspected(page);
	await page.goto(`${createPage}?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', kinds);
	const none = page.getByTestId('editor-docs-none');
	for (const [line, kind, words] of [
		['timeout_seconds: 60', 'await', 'approve waits to be resolved from outside the run, so it runs no activity.'],
		['seconds: 5', 'sleep', 'pause sleeps for a set time, so it runs no activity.'],
		['uses: orkflow:notify-block@0.2.1', 'orkflow', 'notify starts a child run of notify-block@0.2.1, so it runs no activity of its own.'],
		['url: https://example.com', 'activity', 'draft names no activity yet: its uses: says which one runs it.']
	] as const) {
		await caretOn(page, line);
		await expect(none).toHaveAttribute('data-kind', kind);
		await expect(none).toHaveText(words);
		await expect(page.getByTestId('editor-docs-empty')).toHaveCount(0);
	}
	await shot(page, 'create-docs-no-activity-1440.png');
	expect(asked).toEqual([]);
});

test('the rail toggles: shut it is a 44 px strip beside the card, open it shows the docs again', async ({ page }) => {
	await page.setViewportSize({ width: 1440, height: 900 });
	await page.goto(`${createPage}?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', DOC);
	await caretOn(page, 'uses: shell');
	await expect(page.getByTestId('editor-docs-field-script')).toBeVisible({ timeout: 30_000 });

	await toggle(page).click();
	await expect(toggle(page)).toHaveAttribute('aria-expanded', 'false');
	await expect(railBody(page)).toBeHidden();
	await expect.poll(async () => Math.round((await rail(page).boundingBox())!.width)).toBe(44);
	const box = await card(page).boundingBox();
	expect((await rail(page).boundingBox())!.x).toBeGreaterThanOrEqual(box!.x + box!.width);
	await shot(page, 'create-docs-shut-1440.png');

	await toggle(page).click();
	await expect(toggle(page)).toHaveAttribute('aria-expanded', 'true');
	await expect(railBody(page)).toBeVisible();
	await expect(page.getByTestId('editor-docs-field-script')).toBeVisible();
	await expect.poll(async () => Math.round((await rail(page).boundingBox())!.width)).toBe(340);
});

for (const [width, expanded, where] of [
	[1280, 'true', 'beside'],
	[1220, 'false', 'beside'],
	[1100, 'false', 'a bar under']
] as const) {
	test(`at ${width} px the rail starts ${expanded === 'true' ? 'open' : 'shut'}, ${where} the card`, async ({ page }) => {
		await page.setViewportSize({ width, height: 800 });
		await page.goto(`${createPage}?ns=${NS}`);
		await fillYaml(page, 'apply-yaml', DOC);
		await expect(toggle(page)).toHaveAttribute('aria-expanded', expanded);
		if (expanded === 'true') await expect(railBody(page)).toBeVisible();
		else await expect(railBody(page)).toBeHidden();
		const box = await card(page).boundingBox();
		const railBox = await rail(page).boundingBox();
		expect(box && railBox).toBeTruthy();
		if (where === 'beside') expect(railBox!.x).toBeGreaterThanOrEqual(box!.x + box!.width);
		else {
			// One column: the shut docs are a bar as wide as the card, held at the viewport's foot.
			expect(Math.abs(railBox!.x - box!.x)).toBeLessThan(2);
			expect(Math.abs(railBox!.width - box!.width)).toBeLessThan(2);
			await expectDocsBarHeldAtFoot(page, card(page));
		}
		// Shut, the toggle still opens it on the caret's task.
		await caretOn(page, 'uses: http');
		if (expanded === 'false') await toggle(page).click();
		await expect(toggle(page)).toHaveAttribute('aria-expanded', 'true');
		await expect(page.getByTestId('editor-docs-field-url')).toBeVisible({ timeout: 30_000 });
		await shot(page, `create-docs-${width}.png`);
	});
}

test('what a create answered is under the card header, in view beside the code', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 800 });
	await page.goto(`${createPage}?ns=${NS}`);
	// A need that names no task: the engine refuses the create, and stores nothing.
	await fillYaml(page, 'apply-yaml', DOC.replace('needs: [fetch]', 'needs: [fetchx]'));
	await page.getByTestId('apply-submit').click();
	const error = page.getByTestId('apply-notices').getByTestId('apply-error');
	await expect(error).toContainText('fetchx');
	await expect(error).toBeInViewport();
	const box = await card(page).boundingBox();
	const errorBox = await error.boundingBox();
	expect(errorBox!.y).toBeLessThan(box!.y + 120);
	await shot(page, 'create-notice-1280.png');
});
