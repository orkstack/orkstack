import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	orkerEnv,
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
	waitFor,
	waitForEngine
} from './harness';
import { fillYaml, yamlValue } from './yaml';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('authoring-operations');

// Authoring is useful only if selecting a version, filling its declared input,
// and replaying a prior run all address the same immutable definition. This
// uses the built UI and a live engine/Orker, not mocked client responses.
const ENGINE = 'http://127.0.0.1:18089';
const UI = 'http://127.0.0.1:14182';
const TEMPORAL_NAMESPACE = temporalNamespace('authoring');
const NS = 'authoring-ops';
const FLOW = `authoring-${Date.now().toString(36)}`;
const proof = proofOut(process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'authoring-operations') : path.join(uiDir, 'test-results', 'authoring-operations'));
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);

function yaml(version: string, defaultValue: string) {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${FLOW}
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
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18089, 14182]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(proof('engine.log'), bin('ork'), [], { ...catalogEnv, ...cluster, ...storeEnv(BUCKET), ORK_HTTP_ADDR: '127.0.0.1:18089', ORK_WS_ALLOWED_ORIGINS: UI });
	spawnLogged(proof('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('authoring-operations') });
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

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

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
	await expect(page.getByTestId('run-submit-cli-command')).toContainText('--version 1.0.0');
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
	await fillYaml(page, 'apply-yaml', 'apiVersion: orkstack/v1\nkind: Orkflow\nname: broken\nversion: "1"\ntasks: []\n');
	await page.getByTestId('apply-validate').click();
	await expect(page.getByTestId('apply-validation-issues')).toBeVisible();
	const before = await rpc<{ orkflows: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect(before.orkflows.filter((workflow) => workflow.name === 'broken')).toHaveLength(0);
	await shot(page, 'screenshot-validation-no-write.png');

	const source = yaml('3.0.0', 'imported exact bytes');
	await page.getByTestId('apply-import').setInputFiles({ name: 'exact.yaml', mimeType: 'application/yaml', buffer: Buffer.from(source) });
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(source);
	const download = page.waitForEvent('download');
	await page.getByTestId('apply-export').click();
	expect(await (await download).createReadStream()).toBeTruthy();
	// The dirty-draft guard's confirm() fires synchronously from inside the
	// click, so the listener must be registered in the same tick the click is
	// issued (Promise.all) and its resolution awaited before moving on —
	// otherwise a `page.once` can miss the dialog and Playwright auto-dismisses
	// it, leaving a later accept()/dismiss() call to fire on an already-handled
	// dialog.
	await Promise.all([
		page.waitForEvent('dialog').then((dialog) => dialog.dismiss()),
		page.getByRole('link', { name: 'Orkflows' }).click()
	]);
	await expect(page).toHaveURL(/\/apply/);
	await Promise.all([
		page.waitForEvent('dialog').then((dialog) => dialog.accept()),
		page.getByRole('link', { name: 'Orkflows' }).click()
	]);
	await expect(page).toHaveURL(/\/orkflows/);
	await shot(page, 'screenshot-dirty-draft-guard.png');
});

// Activity interface manifests (docs/ACTIVITY_IMAGES.md "Declaring the interface"): the
// first-party `file` image declares its with: contract in the
// org.orkstack.activity.interface label, and validate/apply check a document
// against it after resolution. A missing required key is an ERROR the engine
// refuses before storing anything; an undeclared key is a WARNING that
// changes nothing — the document applies, and the warning stays visible.
//
// This page has never disabled Apply on a validation ERROR (secret-policy's
// e2e clicks Apply after one to see the server refuse it), so the ERROR half
// is proven the same way — Apply is refused in the engine's words and
// nothing is stored — rather than by a disabled button.
test('apply reports a missing required with key as an error and an undeclared key as a warning that survives apply', async ({ page }) => {
	const name = `${FLOW}-iface`;
	const missing = `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "1.0.0"
tasks:
  - id: copy
    uses: file
    with:
      op: copy
`;
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', missing);
	await page.getByTestId('apply-validate').click();
	const error = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].with.src"]');
	await expect(error).toHaveAttribute('data-severity', 'error');
	await expect(error).toHaveClass(/severity-error/);
	await expect(error).toContainText('with.src is required by the interface of');
	await expect(error).toContainText('/orkstack/activity-file@sha256:');
	await expect(error).toContainText('for op "copy" and is not supplied');
	await shot(page, 'screenshot-apply-interface-error.png');

	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-error')).toContainText('tasks[0].with.src: with.src is required by the interface of');
	const refused = await rpc<{ orkflows?: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect((refused.orkflows ?? []).filter((workflow) => workflow.name === name)).toHaveLength(0);

	const warned = `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "1.0.0"
tasks:
  - id: copy
    uses: file
    with:
      op: copy
      src: in/x
      srcs: [why]
`;
	await fillYaml(page, 'apply-yaml', warned);
	await page.getByTestId('apply-validate').click();
	const warning = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].with.srcs"]');
	await expect(warning).toHaveAttribute('data-severity', 'warning');
	await expect(warning).toHaveClass(/severity-warning/);
	await expect(warning).toContainText('(declared: as, op, src); the image may still accept it');
	await expect(page.locator('[data-testid="apply-validation-issue"][data-severity="error"]')).toHaveCount(0);
	await expect(page.getByTestId('apply-submit')).toBeEnabled();

	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-success')).toContainText(`Applied ${name}@1.0.0`);
	// The warning is still on the page after a successful apply.
	await expect(page.getByTestId('apply-validation-issues')).toContainText('tasks[0].with.srcs');
	const stored = await rpc<{ orkflows?: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect((stored.orkflows ?? []).filter((workflow) => workflow.name === name)).toHaveLength(1);
	await shot(page, 'screenshot-apply-interface-issues.png');
});
