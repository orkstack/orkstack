import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killStarted,
	noteTo,
	orkerEnv,
	proofOut,
	repoRoot,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	waitFor,
	waitForEngine
} from './harness';

// The example gallery against a real engine, Orker, Temporal and MinIO (spec
// advanced-orkflow-examples, Playwright):
//
//   P1. the Apply page lists one card per examples/*.ork.yaml — read from the
//       directory here, not from the page — each showing the file's `name:`
//       and `description:`;
//   P2. a new example applies from its card with no validation issue, plans
//       from its Orkflow page with the decide task's output assumed (the
//       branch the decision picks runs, the others are skipped, the re-join
//       runs), and runs to the outcome the plan named.
//
// Topology: one engine, one Orker, the production UI build, on ports of this
// spec's own and a Temporal namespace of its own; only the processes this
// spec started are ever stopped (killStarted).

const ENGINE = 'http://127.0.0.1:18853';
const ENGINE_WS = 'ws://127.0.0.1:18853';
const UI = 'http://127.0.0.1:14853';
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
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18853, 14853]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18853', ORK_WS_ALLOWED_ORIGINS: UI
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('examples-gallery') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14853', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'the example gallery' } });
	note(`namespace ${NS}, engine ${ENGINE}, ui ${UI}, ${EXAMPLES.length} examples in examples/`);
});

test.afterAll(async () => { await killStarted(); });

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

test('P2: a new example applies, plans and runs from the browser', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await page.getByTestId('apply-example-use-decide-and-route').click();
	await expect(page.getByTestId('apply-yaml')).toHaveValue(/name: decide-and-route/);
	await page.getByTestId('apply-validate').click();
	await expect(page.getByTestId('apply-validate')).toHaveText('Validate');
	await expect(page.getByTestId('apply-validation-issue')).toHaveCount(0);
	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-success')).toContainText('Applied decide-and-route@0.1.0', { timeout: 60_000 });
	note('P2 applied decide-and-route@0.1.0 from its card with no validation issue');

	// Plan, with the decision supplied: a condition that reads a key under
	// `result` cannot be evaluated without it.
	await page.goto(`${UI}/orkflows/decide-and-route?ns=${NS}`);
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
