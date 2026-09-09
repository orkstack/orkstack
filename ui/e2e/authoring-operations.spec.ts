import { expect, test, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killAll,
	orkerEnv,
	proofOut,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// Authoring is useful only if selecting a version, filling its declared input,
// and replaying a prior run all address the same immutable definition. This
// uses the built UI and a live engine/Orker, not mocked client responses.
const ENGINE = 'http://127.0.0.1:18089';
const UI = 'http://127.0.0.1:14182';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-authoring';
const NS = 'authoring-ops';
const FLOW = `authoring-${Date.now().toString(36)}`;
const proof = proofOut(process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'authoring-operations') : path.join(uiDir, 'test-results', 'authoring-operations'));
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);

function yaml(version: string, defaultValue: string) {
	return `name: ${FLOW}
version: "${version}"
description: authoring operations
inputs:
  message:
    type: string
    default: "${defaultValue}"
  enabled:
    type: boolean
    default: false
  config:
    type: object
    default: {}
  metadata:
    type: object
    default: {}
  scale:
    type: number
    default: 1
tasks:
  - id: hello
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", "printf '{\\\"ok\\\":true}' > /orkstack/output/values.json"]
`;
}

function shot(page: Page, name: string) { return page.screenshot({ path: proof(name), fullPage: true }); }

test.beforeAll(async () => {
	mkdirSync(proof('..'), { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18089, 14182]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(proof('engine.log'), './bin/ork', [], { ...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18089', ORK_WS_ALLOWED_ORIGINS: UI });
	spawnLogged(proof('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('authoring-operations') });
	await waitForEngine(ENGINE);
	spawnLogged(proof('ui.log'), 'node', ['build'], { HOST: '127.0.0.1', PORT: '14182', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') }, uiDir);
	// A readiness probe only: any always-present route answers it, and this one
	// needs no namespace to exist yet (CreateNamespace is the next line).
	await waitFor('built UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'authoring operations' } });
	await waitFor('a live default pool', async () => {
		const answer = await rpc<{ pools: { liveOrkerCount: number }[] }>('NamespaceService/ListPools', { namespace: NS });
		return answer.pools.some((pool) => pool.liveOrkerCount > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: yaml('1.0.0', 'one') });
	await rpc('OrkflowService/ApplyOrkflow', { namespace: NS, yaml: yaml('2.0.0', 'two') });
});

test.afterAll(async () => { await killAll(); });

test('version-pinned submit, typed inputs, CLI, and run-again retain author intent', async ({ page }) => {
	// The orkflow's own page IS the submit surface: `?version=` selects the
	// version the page shows, and the dialog it opens pins that same version,
	// so the definition being filled in is the one the reader is looking at.
	await page.goto(`${UI}/orkflows/${FLOW}?ns=${NS}&version=1.0.0`);
	await expect(page.getByTestId('orkflow-version')).toHaveText('v1.0.0');
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-version-mode')).toContainText(
		'Pinned explicitly to 1.0.0'
	);
	await expect(page.getByTestId('declared-input-message')).toHaveValue('one');
	await page.getByTestId('declared-input-message').fill('from typed form');
	await page.getByTestId('run-submit-input-json-mode').click();
	await expect(page.getByTestId('run-submit-inputs-json')).toHaveValue(/from typed form/);
	await page.getByTestId('run-submit-input-form-mode').click();
	await page.getByTestId('declared-input-config').fill('{');
	await page.getByTestId('declared-input-metadata').fill('{}');
	await expect(page.getByTestId('run-submit-declared-input-error')).toBeVisible();
	await expect(page.getByTestId('run-submit-submit')).toBeDisabled();
	await page.getByTestId('run-submit-input-json-mode').click();
	await expect(page.getByTestId('declared-input-config')).toHaveValue('{');
	await expect(page.getByTestId('run-submit-inputs-json')).toHaveCount(0);
	await page.getByTestId('declared-input-config').fill('{}');
	await page.getByTestId('declared-input-scale').fill('1.5');
	await page.getByTestId('declared-input-message').fill('');
	await page.getByTestId('run-submit-input-json-mode').click();
	await expect(page.getByTestId('run-submit-inputs-json')).toHaveValue(/"message": ""/);
	await expect(page.getByTestId('run-submit-inputs-json')).toHaveValue(/"scale": 1.5/);
	await page.getByTestId('run-submit-input-form-mode').click();
	await page.getByText('Advanced submission options').click();
	await page.getByTestId('run-submit-idempotency-key').fill('authoring-retry-key');
	await page.getByTestId('run-submit-cli-trigger').click();
	await expect(page.getByTestId('run-submit-cli-command')).toContainText("--version '1.0.0'");
	await shot(page, 'screenshot-version-pinned-submit.png');

	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: FLOW, orkflowVersion: '1.0.0', namespace: NS, idempotencyKey: 'authoring-source', input: { data: { message: 'original', enabled: false } }
	});
	// Replay is a dialog on the run's own page, seeded from the run that page
	// already loaded: no URL carries the input, and the version the new draft
	// pins is the one the source run actually ran.
	await page.goto(`${UI}/runs/${encodeURIComponent(submitted.run.id)}?ns=${NS}`);
	await page.getByTestId('run-again').click();
	await expect(page.getByTestId('run-again-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-replay-banner')).toContainText(
		`Reviewing a new run from ${submitted.run.id}`
	);
	await expect(page.getByTestId('run-submit-version-mode')).toContainText(
		'Pinned explicitly to 1.0.0'
	);
	await page.getByTestId('run-submit-input-json-mode').click();
	await expect(page.getByTestId('run-submit-inputs-json')).toHaveValue(/original/);
	await expect(page.getByTestId('run-submit-idempotency-key')).toHaveValue('');
	await shot(page, 'screenshot-run-again-review.png');
});

test('apply validates without writing, imports and exports exact YAML, and guards a dirty draft', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill('name: broken\nversion: "1"\ntasks: []\n');
	await page.getByTestId('apply-validate').click();
	await expect(page.getByTestId('apply-validation-issues')).toBeVisible();
	const before = await rpc<{ orkflows: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect(before.orkflows.filter((workflow) => workflow.name === 'broken')).toHaveLength(0);
	await shot(page, 'screenshot-validation-no-write.png');

	const source = yaml('3.0.0', 'imported exact bytes');
	await page.getByTestId('apply-import').setInputFiles({ name: 'exact.yaml', mimeType: 'application/yaml', buffer: Buffer.from(source) });
	await expect(page.getByTestId('apply-yaml')).toHaveValue(source);
	const download = page.waitForEvent('download');
	await page.getByTestId('apply-export').click();
	expect(await (await download).createReadStream()).toBeTruthy();
	page.once('dialog', (dialog) => void dialog.dismiss());
	await page.getByRole('link', { name: 'Orkflows' }).click();
	await expect(page).toHaveURL(/\/apply/);
	page.once('dialog', (dialog) => void dialog.accept());
	await page.getByRole('link', { name: 'Orkflows' }).click();
	await expect(page).toHaveURL(/\/orkflows/);
	await shot(page, 'screenshot-dirty-draft-guard.png');
});
