import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	catalogEnv,
	clusterEnv,
	cosignSign,
	ensureActivityImages,
	killStarted,
	noteTo,
	orkerEnv,
	proofOut,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine,
	openNamespaceEdit,
	bin,
	temporalAddress,
	temporalReachable
} from './harness';
import { adminNamespaceUrl } from './uiHelpers';

// Image supply-chain policy in a browser, against a real engine, Orker,
// Temporal, MinIO and registry (docs/DEPLOY.md "Image supply-chain policy"):
//
//   1. Admin → Image policy: the scope caption says Deployment-wide and the
//      empty policy reads No / Unrestricted / None / Never; a save of two
//      prefixes, require digest and two keys shows them, each key's
//      fingerprint equal to `orkctl admin image-policy get`'s line;
//   2. the form's prefilter refuses `GHCR.io/x/` and an EC PRIVATE KEY block
//      without a request;
//   3. Admin → Namespaces: `prod` created requiring cosign:e2e from the
//      select shows it in the Signed by column; `sandbox` with None reads —;
//   4. the Apply page's Validate anchors the registry refusal (an image
//      outside the floor, pinned by digest) and the tag
//      refusal to tasks[0].uses; `uses: shell` validates clean, applies in
//      prod, and its run SUCCEEDS (the Orker verified the signature);
//   5. prod moved to cosign:e2e-other: Submit a run shows the server's
//      failed_precondition sentence verbatim;
//   6. removing e2e-other while prod names it is refused naming prod; after
//      prod is set to None the removal succeeds and one key remains;
//   7. the audit page's image_policy.update records carry fingerprints and
//      no PEM.
//
// Isolation (AGENTS.md §11.1): its own Temporal namespace, its own OrkStack
// namespaces, and — because the image policy is ONE deployment-wide document
// at a fixed key — its own bucket (test/tools/bucket-ensure), so no other
// suite ever reads the policy this one writes. The shell image is signed in
// the shared local registry with this run's own key; a signature is metadata
// beside the image, which nothing else verifies. It ends only the processes
// it started (killStarted), never anything by name.
const ENGINE = 'http://127.0.0.1:18337';
const ENGINE_WS = 'ws://127.0.0.1:18337';
const UI_ORIGIN = 'http://127.0.0.1:14337';
const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-image-policy-${STAMP}`;
const PROD = `prod-${STAMP}`;
const SANDBOX = `sandbox-${STAMP}`;
const BUCKET = `orkstack-image-policy-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'image-policy')
	: path.join(uiDir, 'test-results', 'image-policy');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));
const KEYS = out('keys');
const OTHER_KEYS = out('keys-other');

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function bucketEnv(action: 'ensure' | 'cleanup'): NodeJS.ProcessEnv {
	return {
		...process.env,
		ORK_BUCKET_ACTION: action,
		ORK_S3_BUCKET: BUCKET,
		ORK_S3_ENDPOINT: s3Env.ORK_S3_ENDPOINT,
		ORK_S3_REGION: s3Env.ORK_S3_REGION,
		ORK_S3_ACCESS_KEY: s3Env.ORK_S3_ACCESS_KEY,
		ORK_S3_SECRET_KEY: s3Env.ORK_S3_SECRET_KEY
	};
}

function orkctl(...args: string[]): string {
	return execFileSync(bin('orkctl'), [...args, '--server', ENGINE], {
		cwd: repoRoot,
		encoding: 'utf8'
	});
}

function document(name: string, uses: string): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${name}
version: "0.1.0"
description: Image policy coverage.
tasks:
  - id: build
    uses: ${uses}
    timeout_seconds: 300
    retry:
      maximum_attempts: 1
    with:
      op: exec
      command: ["sh", "-c", "echo signed"]
`;
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (!(await tcpOpen(5001))) throw new Error('the local registry is not reachable on 127.0.0.1:5001');
	for (const port of [18337, 14337]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	// The image is signed AFTER it is pushed (a push can move the tag), with
	// this run's own key; e2e-other signs nothing.
	note(cosignSign(['keygen', KEYS]).trim());
	note(cosignSign(['keygen', OTHER_KEYS]).trim());
	note(cosignSign(['sign', '--key', path.join(KEYS, 'cosign.key'), activityRef('shell')]).trim());

	execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('ensure') });

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	const store = { ...s3Env, ORK_S3_BUCKET: BUCKET };
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...store,
		ORK_HTTP_ADDR: '127.0.0.1:18337',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...store, ...orkerEnv('image-policy') });
	spawnLogged(
		out('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14337', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS },
		uiDir
	);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	note(`engine ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET} (this run's own)`);
});

test.afterAll(async () => {
	await killStarted();
	try {
		execFileSync('go', ['run', './test/tools/bucket-ensure'], { cwd: repoRoot, env: bucketEnv('cleanup') });
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

async function openPolicy(page: Page): Promise<void> {
	await page.goto(`${UI_ORIGIN}/admin/image-policy?ns=${PROD}`);
	await expect(page.getByTestId('image-policy-info')).toBeVisible();
}

// Admin → Namespaces filtered to PROD, so its row is reached whichever page it
// sorts onto. Before PROD exists the filter matches nothing, so readiness is
// the filter itself, not the table.
async function openNamespaces(page: Page): Promise<void> {
	await page.goto(adminNamespaceUrl(UI_ORIGIN, PROD, PROD));
	await expect(page.getByTestId('namespaces-filter-value-0')).toHaveValue(PROD);
}

async function chooseSigner(page: Page, testid: string, value: string): Promise<void> {
	await page.getByTestId(testid).click();
	await page.getByTestId(`${testid}-option-${value}`).click();
}

test('1: the Image policy page shows the empty policy, then a saved one with orkctl’s fingerprints', async ({
	page
}) => {
	await openPolicy(page);
	await expect(page.getByTestId('image-policy-scope-caption')).toContainText('Deployment-wide');
	await expect(page.getByTestId('image-policy-require-digest')).toHaveText('No');
	await expect(page.getByTestId('image-policy-allowed-registries')).toHaveText('Unrestricted');
	await expect(page.getByTestId('image-policy-signing-keys')).toHaveText('None');
	await expect(page.getByTestId('image-policy-updated')).toContainText('Never');
	await page.screenshot({ path: out('screenshot-empty-policy.png'), fullPage: true });

	await page.getByTestId('image-policy-form-allowed-registries').fill('localhost:5001/orkstack/\nghcr.io/acme/');
	await page.getByTestId('image-policy-form-require-digest').check();
	await page.getByTestId('image-policy-form-key-name').fill('e2e');
	await page.getByTestId('image-policy-form-key-pem').fill(readFileSync(path.join(KEYS, 'cosign.pub'), 'utf8'));
	await page.getByTestId('image-policy-form-add-key').click();
	await page.getByTestId('image-policy-form-key-name').fill('e2e-other');
	await page
		.getByTestId('image-policy-form-key-pem')
		.fill(readFileSync(path.join(OTHER_KEYS, 'cosign.pub'), 'utf8'));
	await page.getByTestId('image-policy-form-add-key').click();
	await page.getByTestId('image-policy-form-submit').click();
	await expect(page.getByTestId('image-policy-set-success')).toBeVisible();

	await expect(page.getByTestId('image-policy-require-digest')).toHaveText('Yes');
	await expect(page.getByTestId('image-policy-allowed-registries')).toContainText('localhost:5001/orkstack/');
	await expect(page.getByTestId('image-policy-allowed-registries')).toContainText('ghcr.io/acme/');
	const cli = orkctl('admin', 'image-policy', 'get');
	note(`$ orkctl admin image-policy get\n${cli}`);
	const cliLine = cli.split('\n').find((line) => line.includes(' e2e  sha256:'));
	expect(cliLine, 'orkctl prints the e2e key').toBeTruthy();
	const cliFingerprint = cliLine!.trim().split(/\s+/).pop()!;
	await expect(page.getByTestId('image-policy-signing-key-e2e')).toHaveText(`e2e  ${cliFingerprint}`);
	note(`page: ${await page.getByTestId('image-policy-signing-key-e2e').innerText()} == orkctl: e2e  ${cliFingerprint}`);
	await page.screenshot({ path: out('screenshot-saved-policy.png'), fullPage: true });
});

test('2: the form refuses a bad prefix and a private key locally, without a request', async ({ page }) => {
	await openPolicy(page);
	let updates = 0;
	page.on('request', (request) => {
		if (request.url().includes('SystemService/UpdateImagePolicy')) updates++;
	});
	await page.getByTestId('image-policy-form-allowed-registries').fill('GHCR.io/x/');
	await page.getByTestId('image-policy-form-submit').click();
	await expect(page.getByTestId('image-policy-form-error')).toContainText('not lower-case');
	note(`prefilter (no request): ${await page.getByTestId('image-policy-form-error').innerText()}`);

	await page.getByTestId('image-policy-form-key-name').fill('leaked');
	await page
		.getByTestId('image-policy-form-key-pem')
		.fill('-----BEGIN EC PRIVATE KEY-----\nMHcCAQEE\n-----END EC PRIVATE KEY-----\n');
	await page.getByTestId('image-policy-form-add-key').click();
	await expect(page.getByTestId('image-policy-form-error')).toContainText('"EC PRIVATE KEY"');
	note(`key prefilter (no request): ${await page.getByTestId('image-policy-form-error').innerText()}`);
	expect(updates).toBe(0);
	await page.screenshot({ path: out('screenshot-prefilter.png'), fullPage: true });
});

test('3: Admin → Namespaces offers the signer select and shows the Signed by column', async ({ page }) => {
	await openNamespaces(page);
	await page.getByTestId('ns-create-name').fill(PROD);
	await chooseSigner(page, 'ns-create-require-signed-by', 'cosign:e2e');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${PROD}.`);
	await expect(page.getByTestId(`namespace-require-signed-by-${PROD}`)).toHaveText('cosign:e2e');
	note(`${PROD} Signed by: ${await page.getByTestId(`namespace-require-signed-by-${PROD}`).innerText()}`);
	await page.screenshot({ path: out('screenshot-signed-by-column.png'), fullPage: true });

	// Each create filters the list to the name it created.
	await page.getByTestId('ns-create-name').fill(SANDBOX);
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toContainText(`Created ${SANDBOX}.`);
	await expect(page.getByTestId(`namespace-require-signed-by-${SANDBOX}`)).toHaveText('—');
	note(`${SANDBOX} Signed by: ${await page.getByTestId(`namespace-require-signed-by-${SANDBOX}`).innerText()}`);
});

/**
 * outsideImage is activity-shell (this run's tag) in the shared registry, addressed as
 * 127.0.0.1:<port> (a different registry string from the floor's
 * `localhost:<port>/orkstack/`) and pinned by the digest the registry serves
 * for the same Accept list the engine's resolver sends.
 */
async function outsideImage(): Promise<{ outside: string; outsideRepo: string }> {
	const tagged = activityRef('shell');
	const match = /^localhost:(\d+)\/(.+):([^:/]+)$/.exec(tagged);
	if (!match) throw new Error(`activityRef('shell') = ${tagged}: not a localhost:<port>/<repo>:<tag> reference`);
	const [, port, repo, tag] = match;
	const response = await fetch(`http://127.0.0.1:${port}/v2/${repo}/manifests/${tag}`, {
		method: 'HEAD',
		headers: {
			Accept: [
				'application/vnd.oci.image.index.v1+json',
				'application/vnd.oci.image.manifest.v1+json',
				'application/vnd.docker.distribution.manifest.list.v2+json',
				'application/vnd.docker.distribution.manifest.v2+json'
			].join(', ')
		}
	});
	const digest = response.headers.get('docker-content-digest');
	if (!response.ok || !digest) throw new Error(`HEAD ${tagged} via 127.0.0.1: ${response.status}, digest ${digest}`);
	const outsideRepo = `127.0.0.1:${port}/${repo}`;
	return { outside: `${outsideRepo}@${digest}`, outsideRepo };
}

test('4: Validate anchors both refusals to tasks[0].uses; a signed catalog image applies and runs', async ({
	page
}) => {
	// The image outside the floor is the shared registry's own shell image
	// spelled through 127.0.0.1 rather than localhost, and pinned by digest:
	// the rule checks require_digest BEFORE allowed_registries
	// (docs/DEPLOY.md "Image supply-chain policy"), so a tag-spelled outside image would
	// meet the tag sentence first, and a real, resolvable reference outside
	// `localhost:5001/orkstack/` needs no public-registry round trip.
	const { outside, outsideRepo } = await outsideImage();
	await page.goto(`${UI_ORIGIN}/apply?ns=${PROD}`);
	await page.getByTestId('apply-yaml').fill(document('deploy', outside));
	await page.getByTestId('apply-validate').click();
	const issues = page.getByTestId('apply-validation-issues');
	await expect(issues).toContainText(
		`tasks[0].uses: task "build": image "${outside}" resolves to ${outsideRepo}, which is outside the registries this deployment allows: localhost:5001/orkstack/, ghcr.io/acme/ (orkctl admin image-policy get)`
	);
	note(`validate issue: ${await issues.innerText()}`);
	await page.screenshot({ path: out('screenshot-validate-registry-issue.png'), fullPage: true });

	await page.getByTestId('apply-yaml').fill(document('deploy', activityRef('shell')));
	await page.getByTestId('apply-validate').click();
	await expect(issues).toContainText(
		`tasks[0].uses: task "build": image "${activityRef('shell')}" is a mutable tag, and this deployment requires a digest`
	);
	note(`validate issue: ${await issues.innerText()}`);

	await page.getByTestId('apply-yaml').fill(document('deploy', 'shell'));
	await page.getByTestId('apply-validate').click();
	await expect(issues).toHaveCount(0);
	await page.getByTestId('apply-submit').click();
	await page.waitForURL(/\/orkflows\/deploy/);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
	await page.getByTestId('run-submit-submit').click();
	await page.waitForURL(/\/runs\/ork-/);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
	note(`uses: shell in ${PROD} (cosign:e2e): run ${page.url()} SUCCEEDED`);
	await page.screenshot({ path: out('screenshot-signed-run-succeeded.png'), fullPage: true });
});

test('5: with prod moved to cosign:e2e-other, Submit a run shows the refusal verbatim', async ({ page }) => {
	await openNamespaces(page);
	await openNamespaceEdit(page, PROD);
	await chooseSigner(page, `ns-edit-require-signed-by-${PROD}`, 'cosign:e2e-other');
	await page.getByTestId(`ns-edit-save-${PROD}`).click();
	await expect(page.getByTestId(`namespace-require-signed-by-${PROD}`)).toHaveText('cosign:e2e-other');

	await page.goto(`${UI_ORIGIN}/orkflows/deploy?ns=${PROD}`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-loading')).toHaveCount(0);
	await page.getByTestId('run-submit-submit').click();
	const error = page.getByTestId('run-submit-error');
	await expect(error).toContainText('failed_precondition');
	await expect(error).toContainText('none verifies with key "e2e-other"');
	await expect(error).toContainText(`namespace "${PROD}" requires cosign:e2e-other`);
	note(`submit refusal shown: ${await error.innerText()}`);
	await page.screenshot({ path: out('screenshot-submit-refusal.png'), fullPage: true });
});

test('6: removing a key a namespace names is refused naming it; after clearing, it succeeds', async ({ page }) => {
	await openPolicy(page);
	await page.getByTestId('image-policy-form-remove-key-e2e-other').click();
	await page.getByTestId('image-policy-form-submit').click();
	const refusal = page.getByTestId('image-policy-set-error');
	await expect(refusal).toContainText(
		`signing key "e2e-other" is still required by namespace(s) ${PROD}; clear their require_signed_by first`
	);
	note(`key removal refused: ${await refusal.innerText()}`);
	await page.screenshot({ path: out('screenshot-key-removal-refused.png'), fullPage: true });

	await openNamespaces(page);
	await openNamespaceEdit(page, PROD);
	await chooseSigner(page, `ns-edit-require-signed-by-${PROD}`, '');
	await page.getByTestId(`ns-edit-save-${PROD}`).click();
	await expect(page.getByTestId(`namespace-require-signed-by-${PROD}`)).toHaveText('—');

	await openPolicy(page);
	await page.getByTestId('image-policy-form-remove-key-e2e-other').click();
	await page.getByTestId('image-policy-form-submit').click();
	await expect(page.getByTestId('image-policy-set-success')).toBeVisible();
	await expect(page.getByTestId('image-policy-signing-keys').locator('div')).toHaveCount(1);
	note(`after clearing ${PROD}: ${await page.getByTestId('image-policy-signing-keys').innerText()}`);
});

test('7: the audit page shows image_policy.update records with fingerprints and no PEM', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/audit?ns=${PROD}`);
	const rows = page.getByTestId('audit-row').filter({ has: page.getByTestId('audit-action').getByText('image_policy.update') });
	await expect(rows.first()).toBeVisible();
	const details = await rows.getByTestId('audit-detail').allInnerTexts();
	note(`image_policy.update details:\n${details.join('\n')}`);
	expect(details.length).toBeGreaterThan(0);
	expect(details.some((d) => /signing_keys=\{e2e=sha256:[0-9a-f]{64}/.test(d))).toBe(true);
	for (const d of details) expect(d.includes('-----BEGIN')).toBe(false);
	await page.screenshot({ path: out('screenshot-audit.png'), fullPage: true });
});
