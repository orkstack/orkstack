import { expect, test, type Page } from '@playwright/test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// M10.7 proof: the UI is namespace-aware, and an orkflow can be edited,
// versioned, promoted, rolled back and diffed without leaving the browser.
//
// Real engine (Connect API over real Temporal + real MinIO) and the real
// production UI build. Nothing is stubbed: every version listed, every diff
// rendered and every stale-edit refusal below is the engine's own answer.
//
// Isolation (AGENTS.md §6): the Temporal namespace is this milestone's own,
// and the OrkStack namespaces are this milestone's own two. The orkflow name
// carries a per-run stamp, so a version list is never polluted by yesterday's
// run and `promote` always has a lineage it can predict.

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(uiDir, '..');

// Ports of their own: this spec and e2e/realtime.spec.ts must be able to run in
// one `playwright test` invocation without fighting over a listener.
const ENGINE_HTTP = 'http://127.0.0.1:18081';
const ENGINE_WS = 'ws://127.0.0.1:18081';
const UI_ORIGIN = 'http://127.0.0.1:14174';

const TEMPORAL_NAMESPACE =
	process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-m10-7-ui-namespaces-editor';

// Two OrkStack namespaces, fixed so the registry does not grow by two on every
// run, plus a per-run orkflow name so the version lineage is always fresh.
const ALPHA = 'm10-7-alpha';
const BETA = 'm10-7-beta';
const STAMP = Date.now().toString(36);
const ORKFLOW = `editable-${STAMP}`;
const BETA_ORKFLOW = `beta-only-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof-m10-7');

const children: ChildProcess[] = [];

function outPath(name: string): string {
	mkdirSync(PROOF_DIR, { recursive: true });
	return path.join(PROOF_DIR, name);
}

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

function spawnLogged(
	name: string,
	command: string,
	args: string[],
	env: NodeJS.ProcessEnv,
	cwd: string = repoRoot
): ChildProcess {
	const out = createWriteStream(outPath(name), { flags: 'w' });
	const child = spawn(command, args, {
		cwd,
		env: { ...process.env, ...env },
		stdio: ['ignore', 'pipe', 'pipe']
	});
	child.stdout?.pipe(out);
	child.stderr?.pipe(out);
	children.push(child);
	return child;
}

async function waitFor(
	label: string,
	probe: () => Promise<boolean>,
	timeoutMs = 90_000
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			if (await probe()) return;
		} catch {
			// not ready yet
		}
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await new Promise((r) => setTimeout(r, 250));
	}
}

function tcpOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
	return new Promise((resolve) => {
		const sock = net.connect({ port, host });
		sock.once('connect', () => {
			sock.destroy();
			resolve(true);
		});
		sock.once('error', () => resolve(false));
		sock.setTimeout(1500, () => {
			sock.destroy();
			resolve(false);
		});
	});
}

async function killAll(): Promise<void> {
	const survivors = children.splice(0);
	for (const child of survivors) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 8_000;
	for (const child of survivors) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
	// AGENTS.md §6: never leave a stray engine behind, and anchor the pattern —
	// unanchored, `bin/ork` also matches `bin/orkctl`.
	try {
		execSync("pkill -f '/bin/ork$' || true", { timeout: 5_000 });
	} catch {
		// pkill exit status is informational only
	}
}

// rpc calls the engine through the UI's own same-origin proxy: the exact path
// the browser takes, so a fixture cannot be set up over a route the app does
// not have.
async function rpc<T>(method: string, body: unknown): Promise<T> {
	const res = await fetch(`${UI_ORIGIN}/orkstack.v1.${method}`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
	const text = await res.text();
	if (!res.ok) throw new Error(`${method} failed: ${res.status} ${text}`);
	return JSON.parse(text) as T;
}

function orkflowYaml(name: string, version: string, description: string): string {
	return `name: ${name}
version: "${version}"
description: ${description}

tasks:
  - id: greet
    plugin: shell
    with:
      op: exec
      command: echo hello
`;
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18081)) throw new Error('port 18081 already in use (stray engine?)');
	if (await tcpOpen(14174)) throw new Error('port 14174 already in use (stray UI?)');

	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// The engine stores orkflows, versions and the namespace registry in the
	// real bucket: the version list, the current pointer and the CAS this spec
	// exercises are MinIO objects, not process memory.
	spawnLogged('engine.log', './bin/ork', [], {
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		ORK_TEMPORAL_NAMESPACE: TEMPORAL_NAMESPACE,
		ORK_HTTP_ADDR: '127.0.0.1:18081',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN,
		ORK_ARTIFACT_BACKEND: 's3',
		ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
		ORK_S3_REGION: 'us-east-1',
		ORK_S3_BUCKET: 'orkstack-artifacts',
		ORK_S3_ACCESS_KEY: 'orkstack',
		ORK_S3_SECRET_KEY: 'orkstack-secret',
		ORK_S3_PATH_STYLE: 'true'
	});
	await waitFor('engine connect api', async () => {
		const res = await fetch(`${ENGINE_HTTP}/orkstack.v1.OrkflowService/ListOrkflows`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: '{}'
		});
		return res.ok;
	});

	spawnLogged(
		'ui.log',
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14174',
			ENGINE_API_URL: ENGINE_HTTP,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// Two namespaces (CreateNamespace is idempotent for an identical spec) and
	// one orkflow in each, so "scoped" is falsifiable: alpha's page must not
	// show beta's orkflow and the other way round.
	for (const name of [ALPHA, BETA]) {
		await rpc('NamespaceService/CreateNamespace', {
			namespace: { name, description: 'M10.7 UI proof' }
		});
	}
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml(ORKFLOW, '0.1.0', 'first revision'),
		namespace: ALPHA
	});
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml(BETA_ORKFLOW, '0.1.0', 'beta only'),
		namespace: BETA
	});
});

test.afterAll(async () => {
	await killAll();
});

// Acceptance 1: a namespace switcher scopes every page, the selection survives
// navigation and reload, and it is in the URL — so a pasted link opens the same
// namespace in a browser that has never seen this app.
test('the namespace switcher scopes every page, survives reload, and travels in the URL', async ({
	page,
	browser
}) => {
	// The default namespace holds neither orkflow.
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await expect(page.getByTestId('namespace-picker')).toHaveValue('default');
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toHaveCount(0);

	// Switching scopes the list and writes the choice into the URL.
	await page.getByTestId('namespace-picker').selectOption(ALPHA);
	await page.waitForURL(new RegExp(`ns=${ALPHA}`));
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
	await shot(page, 'screenshot-namespace-alpha.png');

	// Navigation preserves it, and every page the acceptance names is scoped by
	// it: the nav links carry the namespace and each destination reads inside it.
	const nav = page.locator('header nav');
	await nav.getByRole('link', { name: 'Runs', exact: true }).click();
	await page.waitForURL(new RegExp(`/runs\\?.*ns=${ALPHA}`));
	await expect(page.getByTestId('namespace-picker')).toHaveValue(ALPHA);
	await expect(page.getByTestId('runs-empty')).toContainText(ALPHA);

	await nav.getByRole('link', { name: 'Submit run', exact: true }).click();
	await page.waitForURL(new RegExp(`/submit\\?.*ns=${ALPHA}`));
	// The picker offers alpha's orkflow and not beta's: submit is scoped too.
	await expect(page.getByTestId('orkflow-select')).toContainText(ORKFLOW);
	await expect(page.getByTestId('orkflow-select')).not.toContainText(BETA_ORKFLOW);

	await nav.getByRole('link', { name: 'Apply orkflow', exact: true }).click();
	await page.waitForURL(new RegExp(`/apply\\?.*ns=${ALPHA}`));
	await expect(page.getByTestId('apply-namespace')).toContainText(ALPHA);

	// Reload preserves it.
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${ALPHA}`);
	await page.reload();
	await expect(page.getByTestId('namespace-picker')).toHaveValue(ALPHA);

	// A pasted link opens the same namespace in a browser with no localStorage.
	const pasted = `${UI_ORIGIN}/orkflows?ns=${ALPHA}`;
	const fresh = await browser.newContext();
	const colleague = await fresh.newPage();
	await colleague.goto(pasted);
	await expect(colleague.getByTestId('namespace-picker')).toHaveValue(ALPHA);
	await expect(colleague.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();
	await fresh.close();

	// A bare URL in the browser that made the choice restores it and writes it
	// back into the URL (design §1.9), instead of silently resetting to default.
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await page.waitForURL(new RegExp(`ns=${ALPHA}`));
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();

	// The other namespace is scoped too, and choosing `default` sticks — the
	// restore must not drag the user back to the namespace they just left.
	await page.getByTestId('namespace-picker').selectOption(BETA);
	await page.waitForURL(new RegExp(`ns=${BETA}`));
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toBeVisible();
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toHaveCount(0);
	await page.getByTestId('namespace-picker').selectOption('default');
	await page.waitForURL((url) => !url.searchParams.has('ns'));
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await expect(page.getByTestId('namespace-picker')).toHaveValue('default');
});

// Acceptance 2 (editor + validation from the real ApplyOrkflow response) and
// acceptance 3 (a stale edit is detected through M10.4's base_version, and
// never overwrites).
test('editing applies a new version, real validation errors, and a stale edit is refused', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=edit`);

	// The editor opens on the head version's stored YAML and says which version
	// the edit is derived from — that value is the base_version it sends.
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/first revision/);
	await shot(page, 'screenshot-editor.png');

	// 1. A document the compiler rejects: the message is the engine's, and
	//    nothing is stored.
	await page.getByTestId('editor-yaml').fill('name: broken\nversion: "9.9.9"\ntasks: []\n');
	await page.getByTestId('editor-apply').click();
	await expect(page.getByTestId('editor-error')).toBeVisible();
	await expect(page.getByTestId('editor-error')).toContainText(/task/i);
	await shot(page, 'screenshot-editor-validation-error.png');
	const afterInvalid = await rpc<{ versions: { version: string }[] }>(
		'OrkflowService/ListOrkflowVersions',
		{ name: ORKFLOW, namespace: ALPHA }
	);
	expect(afterInvalid.versions.map((v) => v.version)).toEqual(['0.1.0']);

	// 2. A good edit lands as a NEW version; the old one is untouched.
	await page.getByTestId('editor-yaml').fill(orkflowYaml(ORKFLOW, '0.2.0', 'second revision'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await expect(page.getByTestId('version-row-0.2.0')).toBeVisible();
	await expect(page.getByTestId('version-row-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();

	// 3. Stale edit: a colleague applies 0.3.0 while this editor holds 0.2.0.
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=edit`);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.0');
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml(ORKFLOW, '0.3.0', 'applied by somebody else'),
		namespace: ALPHA
	});
	await page.getByTestId('editor-yaml').fill(orkflowYaml(ORKFLOW, '0.2.5', 'my edit'));
	await page.getByTestId('editor-apply').click();

	// The refusal is reported, names both versions, and nothing was written.
	await expect(page.getByTestId('editor-conflict')).toBeVisible();
	await expect(page.getByTestId('editor-conflict')).toContainText('0.2.0');
	await expect(page.getByTestId('editor-conflict-head')).toHaveText('0.3.0');
	await shot(page, 'screenshot-editor-stale.png');
	const afterStale = await rpc<{ versions: { version: string }[] }>(
		'OrkflowService/ListOrkflowVersions',
		{ name: ORKFLOW, namespace: ALPHA }
	);
	expect(afterStale.versions.map((v) => v.version).sort()).toEqual(['0.1.0', '0.2.0', '0.3.0']);
	// The edit is still in the box: the conflict path never discards the work.
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/my edit/);

	// The conflict offers the diff of what arrived underneath, from the server.
	await page.getByTestId('editor-conflict-diff').click();
	await expect(page.getByTestId('conflict-diff')).toContainText(`--- a/${ORKFLOW}@0.2.0`);
	await expect(page.getByTestId('conflict-diff')).toContainText('+description: applied by somebody else');

	// Rebasing is explicit, and then the edit applies onto the new head.
	await page.getByTestId('editor-conflict-rebase').click();
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.3.0');
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await expect(page.getByTestId('version-row-0.2.5')).toBeVisible();
});

// Acceptance 2: promote with a confirmation step, and rollback is promoting the
// previous version. Nothing is optimistic — the badge moves only after the
// engine says the pointer moved.
test('promoting and rolling back moves the current version, behind a confirmation', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=versions`);
	await expect(page.getByTestId('version-current-0.2.5')).toBeVisible();
	await shot(page, 'screenshot-versions.png');

	// Cancelling the confirmation changes nothing.
	await page.getByTestId('promote-0.1.0').click();
	await expect(page.getByTestId('promote-confirm')).toContainText('0.2.5');
	await expect(page.getByTestId('promote-confirm')).toContainText('0.1.0');
	await shot(page, 'screenshot-promote-confirm.png');
	await page.getByTestId('promote-cancel').click();
	await expect(page.getByTestId('promote-confirm')).toHaveCount(0);
	await expect(page.getByTestId('version-current-0.2.5')).toBeVisible();

	// Rollback: promote an older version. The pointer moves; HEAD does not —
	// head is the edit lineage's tip (0.2.5, applied last), and conflating the
	// two is what would run the wrong code after a rollback.
	await page.getByTestId('promote-0.1.0').click();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-current-0.2.5')).toHaveCount(0);
	await expect(page.getByTestId('version-head-0.2.5')).toBeVisible();
	await shot(page, 'screenshot-versions-rolled-back.png');

	// The engine agrees: this is the stored pointer, not a UI state.
	const listed = await rpc<{ currentVersion: string; headVersion: string }>(
		'OrkflowService/ListOrkflowVersions',
		{ name: ORKFLOW, namespace: ALPHA }
	);
	expect(listed.currentVersion).toBe('0.1.0');
	expect(listed.headVersion).toBe('0.2.5');

	// Roll forward again.
	await page.getByTestId('promote-0.3.0').click();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-0.3.0')).toBeVisible();
});

// Acceptance 2: a diff between any two versions, rendered from the server's
// unified diff — one implementation, so the CLI and the UI cannot drift.
test('diffing two versions renders the engine unified diff', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=versions`);
	await page.getByTestId('diff-from-0.1.0').check();
	await page.getByTestId('diff-to-0.3.0').check();
	await page.getByTestId('diff-run').click();

	const diff = page.getByTestId('version-diff');
	await expect(diff).toBeVisible();
	await expect(diff).toContainText(`--- a/${ORKFLOW}@0.1.0`);
	await expect(diff).toContainText(`+++ b/${ORKFLOW}@0.3.0`);
	await expect(diff).toContainText('-description: first revision');
	await expect(diff).toContainText('+description: applied by somebody else');
	await shot(page, 'screenshot-diff.png');

	// The engine's text, verbatim: the rendered diff is byte-identical to the
	// RPC's answer, so nothing is re-derived in the browser.
	const answer = await rpc<{ unifiedDiff: string }>('OrkflowService/DiffOrkflowVersions', {
		name: ORKFLOW,
		fromVersion: '0.1.0',
		toVersion: '0.3.0',
		namespace: ALPHA
	});
	expect((await diff.innerText()).trim()).toBe(answer.unifiedDiff.trim());
});

// Acceptance 5: a failed request says what failed and offers a retry — no view
// can sit on a spinner. The failure is injected at the network, so the loader
// really fails and the retry really re-issues it.
test('a failed load reports the failure and retries, instead of spinning', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${ALPHA}`);

	// Break the version listing, then navigate client-side into the detail page.
	await page.route('**/orkstack.v1.OrkflowService/ListOrkflowVersions', (route) => route.abort());
	await page.getByTestId(`orkflow-row-${ORKFLOW}`).getByRole('link').click();
	await page.getByTestId('tab-versions').click();

	const failure = page.getByTestId('versions-error');
	await expect(failure).toBeVisible();
	await expect(failure).toContainText(/could not|failed|error/i);
	await expect(page.getByTestId('versions-retry')).toBeVisible();
	await expect(page.getByTestId('versions-loading')).toHaveCount(0);
	await shot(page, 'screenshot-error-retry.png');

	// Heal the network and retry: the same view loads without a page reload.
	await page.unroute('**/orkstack.v1.OrkflowService/ListOrkflowVersions');
	await page.getByTestId('versions-retry').click();
	await expect(page.getByTestId('version-row-0.3.0')).toBeVisible();
	await expect(page.getByTestId('versions-error')).toHaveCount(0);
});
