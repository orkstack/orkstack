import { expect, test, type Page } from './test';
import { mkdirSync, readdirSync, readFileSync } from 'node:fs';
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
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { yamlValue } from './yaml';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('examples-gallery');

// The example gallery against a real engine, Orker, Temporal and MinIO
// (examples/README.md):
//
//   P1. the Apply page lists one card per examples/*.ork.yaml — read from the
//       directory here, not from the page — each showing the file's `name:`
//       and `description:`;
//   P2. a new example applies from its card with no validation issue, plans
//       from its Orkflow page with the decide task's output assumed (the
//       branch the decision picks runs, the others are skipped, the re-join
//       runs), and runs to the outcome the plan named.
//
// Topology: one engine, one Orker, the production UI build served by that
// engine, on a port of this spec's own and a Temporal namespace of its own;
// only the processes this spec started are ever stopped (killStarted).

const ENGINE = 'http://127.0.0.1:18853';
// The UI is the engine's own origin: it serves the build beside its API.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE
	? `${process.env.ORK_TEMPORAL_NAMESPACE}-examples-gallery`
	: 'ork-ui-examples-gallery';
const STAMP = Date.now().toString(36);
const NS = `examples-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'examples-gallery')
	: path.join(uiDir, 'test-results', 'examples-gallery');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

// The same two top-level keys the gallery reads, read independently: a plain
// value with one layer of quotes stripped, or a block scalar's lines.
function topLevel(yaml: string, key: string): string {
	const lines = yaml.split('\n');
	const index = lines.findIndex((line) => line.startsWith(`${key}:`));
	if (index === -1) return '';
	const value = lines[index].slice(key.length + 1).trim();
	if (/^[|>][-+]?\d*$/.test(value)) {
		const body: string[] = [];
		for (const line of lines.slice(index + 1)) {
			if (line.trim() !== '' && !/^\s/.test(line)) break;
			body.push(line.trim());
		}
		return body.join(' ').replace(/\s+/g, ' ').trim();
	}
	const quoted = /^(["'])([\s\S]*)\1$/.exec(value);
	return quoted ? quoted[2] : value;
}

const EXAMPLES = readdirSync(path.join(repoRoot, 'examples'))
	.filter((file) => file.endsWith('.ork.yaml'))
	.sort()
	.map((file) => {
		const yaml = readFileSync(path.join(repoRoot, 'examples', file), 'utf8');
		return { slug: file.replace(/\.ork\.yaml$/, ''), name: topLevel(yaml, 'name'), description: topLevel(yaml, 'description') };
	});

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18853)) throw new Error('port 18853 already in use');

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ...uiEnv(), ORK_HTTP_ADDR: '127.0.0.1:18853'
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('examples-gallery') });
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'the example gallery' } });
	note(`namespace ${NS}, engine ${ENGINE} (serving the UI), ${EXAMPLES.length} examples in examples/`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('P1: the gallery lists every example with its name and description', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	const cards = page.getByTestId('apply-examples').locator(':scope > div');
	await expect(cards).toHaveCount(EXAMPLES.length);
	for (const example of EXAMPLES) {
		const card = page.getByTestId(`apply-example-${example.slug}`);
		await expect(card).toBeVisible();
		await expect(card.locator('h3')).toHaveText(example.name);
		await expect(card).toContainText(example.description);
		note(`P1 card ${example.slug}: ${example.name} — ${example.description}`);
	}
	await shot(page, 'screenshot-apply-gallery.png');
});

// F-ui-perf-7: no font face is inlined into the render-blocking stylesheet,
// and the page every route boots from preloads exactly the two latin faces
// every page uses. That page is the build's index.html, the SPA fallback the
// engine answers /apply with; it names its preloads as <link> tags, the fonts
// beside the entry scripts, and its stylesheet the same way.
test('P0: fonts stay out of the stylesheet, and the two latin faces are preloaded', async () => {
	const response = await fetch(`${UI}/apply?ns=${NS}`);
	expect(response.status).toBe(200);
	expect(response.headers.get('content-type')).toContain('text/html');
	const html = await response.text();
	const links = [...html.matchAll(/<link\b[^>]*>/g)].map((m) => m[0]);
	const attr = (link: string, name: string) => new RegExp(`\\b${name}="([^"]*)"`).exec(link)?.[1] ?? '';
	const fonts = links.filter((link) => attr(link, 'rel') === 'preload' && attr(link, 'as') === 'font');
	expect(fonts.map((link) => /assets\/([a-z-]+)\./.exec(attr(link, 'href'))?.[1]).sort()).toEqual([
		'archivo-narrow-latin-wght-normal',
		'spline-sans-mono-latin-wght-normal'
	]);
	// A font preload without `crossorigin` is fetched a second time by the
	// @font-face that uses it.
	for (const font of fonts) expect(font, 'the preload is usable by @font-face').toMatch(/\bcrossorigin\b/);
	const scripts = links.filter((link) => attr(link, 'rel') === 'modulepreload').map((link) => attr(link, 'href'));
	expect(scripts.some((href) => /^\/_app\/immutable\/entry\/start\.[^/]+\.js$/.test(href)), 'the entry is preloaded').toBe(true);
	expect(scripts.some((href) => /^\/_app\/immutable\/entry\/app\.[^/]+\.js$/.test(href)), 'the app is preloaded').toBe(true);
	const sheets = links.filter((link) => attr(link, 'rel') === 'stylesheet').map((link) => attr(link, 'href'));
	expect(sheets.length > 0).toBe(true);
	for (const sheet of sheets) {
		const css = await (await fetch(new URL(sheet, `${UI}/apply`))).text();
		expect(css, `${sheet} inlines no font`).not.toContain('data:font/woff2');
	}
	note(`P0 font preloads: ${fonts.length}; module preloads: ${scripts.length}; stylesheets checked: ${sheets.join(', ')}`);
});

// F-ui-perf-5: the page ships names and descriptions; a document is served
// one at a time, byte for byte, only when a card asks for it. The cards are a
// file of the build (/apply/examples.json) and each document one of its own
// (/apply/examples/<file>), written from examples/ when the UI is built.
test('P1b: the page carries no example YAML; one example is fetched on demand', async ({ page }) => {
	// The longest plain line of each document's tasks: in what the page
	// loads, raw or serialized, only if the document itself were shipped.
	const needles = EXAMPLES.map((example) => {
		const yaml = readFileSync(path.join(repoRoot, 'examples', `${example.slug}.ork.yaml`), 'utf8');
		const tasks = yaml.slice(Math.max(0, yaml.indexOf('\ntasks:')));
		const needle = tasks
			.split('\n')
			.map((line) => line.trim())
			.filter((line) => !/["'<>&\\]/.test(line))
			.sort((a, b) => b.length - a.length)[0];
		expect(needle.length >= 12, `${example.slug} has a distinctive line`).toBe(true);
		return { slug: example.slug, needle };
	});

	// The cards: one per example, its name and description, and no document.
	const listed = await fetch(`${UI}/apply/examples.json`);
	expect(listed.status).toBe(200);
	expect(listed.headers.get('content-type')).toContain('application/json');
	const listedText = await listed.text();
	const cards = JSON.parse(listedText) as Record<string, string>[];
	expect(cards.map((card) => Object.keys(card).sort())).toEqual(EXAMPLES.map(() => ['description', 'file', 'name', 'slug']));
	expect(
		cards.map((card) => ({ slug: card.slug, file: card.file, name: card.name, description: card.description.replace(/\s+/g, ' ').trim() }))
	).toEqual(EXAMPLES.map((example) => ({ ...example, file: `${example.slug}.ork.yaml` })));
	for (const { slug, needle } of needles) expect(listedText, `${slug}'s document is not in the cards`).not.toContain(needle);

	// One document, as it is in examples/; a document there is not, a 404 and
	// never the app.
	const served = await fetch(`${UI}/apply/examples/decide-and-route.ork.yaml`);
	expect(served.status).toBe(200);
	expect(served.headers.get('content-type')).toContain('text/yaml');
	expect(await served.text()).toBe(readFileSync(path.join(repoRoot, 'examples', 'decide-and-route.ork.yaml'), 'utf8'));
	const missing = await fetch(`${UI}/apply/examples/nope.ork.yaml`);
	expect(missing.status).toBe(404);
	expect(await missing.text(), 'a missing example is not answered with the app').not.toContain('/_app/');

	// The page itself: everything it loads — the document, its scripts, the
	// cards — carries no example's document, and it fetches one only when a
	// card is chosen.
	const fetched: string[] = [];
	const loads: Promise<{ path: string; body: string }>[] = [];
	page.on('request', (request) => {
		if (request.url().includes('/apply/examples/')) fetched.push(new URL(request.url()).pathname);
	});
	page.on('response', (response) => {
		const url = new URL(response.url());
		if (url.origin !== UI || url.pathname.startsWith('/apply/examples/')) return;
		loads.push(
			response
				.text()
				.then((body) => ({ path: url.pathname, body }))
				.catch(() => ({ path: url.pathname, body: '' }))
		);
	});
	await page.goto(`${UI}/apply?ns=${NS}`);
	await expect(page.getByTestId('apply-examples').locator(':scope > div')).toHaveCount(EXAMPLES.length);
	await page.waitForLoadState('networkidle');
	const loaded = await Promise.all(loads);
	expect(loaded.map((load) => load.path)).toContain('/apply/examples.json');
	for (const load of loaded) {
		for (const { slug, needle } of needles) expect(load.body, `${slug}'s document is not in ${load.path}`).not.toContain(needle);
	}
	expect(fetched).toEqual([]);
	await page.getByTestId('apply-example-use-decide-and-route').click();
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/name: decide-and-route/);
	expect(fetched).toEqual(['/apply/examples/decide-and-route.ork.yaml']);
	const bytes = loaded.reduce((n, load) => n + load.body.length, 0);
	note(`P1b page load: ${loaded.length} responses, ${bytes} bytes, no example document; one fetch on choosing: ${fetched.join(', ')}`);
});

test('P2: a new example applies, plans and runs from the browser', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await page.getByTestId('apply-example-use-decide-and-route').click();
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/name: decide-and-route/);
	await page.getByTestId('apply-validate').click();
	await expect(page.getByTestId('apply-validate')).toHaveText('Validate');
	await expect(page.getByTestId('apply-validation-issue')).toHaveCount(0);
	await page.getByTestId('apply-submit').click();
	// An apply with no warning goes straight to the Orkflow's page; the
	// `apply-success` banner is shown only when warnings remain to be read
	// (ui/src/routes/apply/+page.svelte).
	await page.waitForURL(/\/orkflows\/decide-and-route/, { timeout: 60_000 });
	await expect(page.getByTestId('orkflow-name')).toContainText('decide-and-route');
	await expect(page.getByTestId('orkflow-version')).toHaveText('v0.1.0');
	note('P2 applied decide-and-route@0.1.0 from its card with no validation issue');

	// Plan, with the decision supplied: a condition that reads a key under
	// `result` cannot be evaluated without it.
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-assumed-outputs').fill('{"decide":{"result":{"route":"direct","notify":false}}}');
	await expect(page.getByTestId('run-submit-assumed-outputs-error')).toHaveCount(0);
	await page.getByTestId('run-submit-plan').click();
	await expect(page.getByTestId('run-plan-table')).toBeVisible();
	await expect(page.getByTestId('run-plan-row-direct-outcome')).toHaveText('runs');
	await expect(page.getByTestId('run-plan-row-canary-outcome')).toHaveText('skipped');
	await expect(page.getByTestId('run-plan-row-hold-outcome')).toHaveText('skipped');
	await expect(page.getByTestId('run-plan-row-report-outcome')).toHaveText('runs');
	note(`P2 plan: ${await page.getByTestId('run-plan-summary').textContent()}`);
	await shot(page, 'screenshot-decide-plan.png');

	// The default input decides `direct`, so the run is the path planned.
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	await expect(page.getByTestId('task-report-status')).toHaveText(/succeeded/i);
	await expect(page.getByTestId('task-direct-status')).toHaveText(/succeeded/i);
	await expect(page.getByTestId('task-canary-status')).toHaveText(/skipped/i);
	await expect(page.getByTestId('task-hold-status')).toHaveText(/skipped/i);
	note(`P2 run ${page.url()}: report succeeded, direct succeeded, canary and hold skipped`);
	await shot(page, 'screenshot-decide-run.png');
});
