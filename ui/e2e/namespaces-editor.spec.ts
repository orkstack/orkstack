import { expect, test, type Page } from './test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { catalogEnv, ensureActivityImages } from './harness';
import { expectNamespace, selectNamespace } from './uiHelpers';

// The UI is namespace-aware, and an orkflow can be edited,
// versioned, promoted, rolled back and diffed without leaving the browser.
//
// Real engine (Connect API over real Temporal + real MinIO) and the real
// production UI build. Nothing is stubbed: every version listed, every diff
// rendered and every stale-edit refusal below is the engine's own answer.
//
// Isolation (AGENTS.md §11.1): the Temporal namespace is this spec's own,
// and the OrkStack namespaces are this spec's own two. The orkflow name
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
	process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-namespaces-editor';

// Two OrkStack namespaces, fixed so the registry does not grow by two on every
// run, plus a per-run orkflow name so the version lineage is always fresh.
const ALPHA = 'editor-alpha';
const BETA = 'editor-beta';
const DEFAULT_NS = 'default';
const STAMP = Date.now().toString(36);
const ORKFLOW = `editable-${STAMP}`;
const BETA_ORKFLOW = `beta-only-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'namespaces-editor')
	: path.join(uiDir, 'test-results', 'namespaces-editor');

// The tests below are sequential and stateful ON PURPOSE: each builds on the
// versions the last one applied, which is the only way to prove promote,
// rollback and a diff against a real lineage rather than a fixture.
//
// Serial mode is the declaration of that. Without it Playwright answers one
// failure by restarting the worker and running the REST of the file in it —
// re-entering beforeAll, which mints a new STAMP and applies a new orkflow, so
// the survivors go looking for versions nobody ever applied. One defect then
// reports as four failures and names none of them. In serial mode a failure
// skips what depended on it, and the report has one line.
test.describe.configure({ mode: 'serial' });

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
	// AGENTS.md §11.1: never leave a stray engine behind, and anchor the pattern —
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

// purgeOwnOrkflows deletes, from this spec's two fixed namespaces, every
// orkflow a run of this spec creates (`editable-*`, `beta-only-*`). None of
// them is ever submitted, so DeleteOrkflow's recent-run guard never refuses.
async function purgeOwnOrkflows(): Promise<void> {
	for (const namespace of [ALPHA, BETA]) {
		const names: string[] = [];
		let pageToken = '';
		do {
			const page = await rpc<{ orkflows?: { name: string }[]; nextPageToken?: string }>(
				'OrkflowService/ListOrkflows',
				{ namespace, pageSize: 100, pageToken }
			);
			for (const o of page.orkflows ?? []) {
				if (/^(editable|beta-only)-/.test(o.name)) names.push(o.name);
			}
			pageToken = page.nextPageToken ?? '';
		} while (pageToken);
		for (const name of names) {
			await rpc('OrkflowService/DeleteOrkflow', { name, namespace });
		}
	}
}

function orkflowYaml(name: string, version: string, description: string): string {
	return `name: ${name}
version: "${version}"
description: ${description}

tasks:
  - id: greet
    uses: shell
    with:
      op: exec
      command: ["echo", "hello"]
`;
}

// settle waits for a tab to reach one of its two end states and FAILS NAMING
// THE CAUSE when it reached the error one.
//
// A bare `expect(getByTestId('editor-base-version')).toHaveText('0.1.0')`
// cannot tell "the head version could not be read" from "not painted yet":
// both are an absent element, so a proof run that hits the first reports the
// second and the transcript names no cause at all — a flake that has been
// observed, once in thirteen runs, with nothing in the log to say why. Nothing
// here retries and nothing here relaxes an assertion; the wait is
// the same wait, and the failure is legible.
//
// An error panel whose own retry is in flight is NOT an end state: it still
// shows the failure it is busy replacing, and its button is disabled and reads
// "Retrying…". So the failure this waits for is an idle one.
async function settle(page: Page, ready: string, failed: string, what: string): Promise<void> {
	const ok = page.getByTestId(ready);
	const panel = page.getByTestId(failed);
	const idleFailure = panel.locator('button:not([disabled])');
	await expect(ok.or(idleFailure).first()).toBeVisible();
	if (await idleFailure.isVisible()) {
		const said = (await panel.innerText()).replace(/\s+/g, ' ').trim();
		throw new Error(`${what} reported a failed read instead of loading: ${said}`);
	}
}

const openEditor = (page: Page) => settle(page, 'editor-form', 'editor-error-load', 'the Edit tab');
const openVersions = (page: Page) =>
	settle(page, 'version-list', 'versions-error', 'the Versions tab');

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18081)) throw new Error('port 18081 already in use (stray engine?)');
	if (await tcpOpen(14174)) throw new Error('port 14174 already in use (stray UI?)');

	// The three first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// The engine stores orkflows, versions and the namespace registry in the
	// real bucket: the version list, the current pointer and the CAS this spec
	// exercises are MinIO objects, not process memory.
	spawnLogged('engine.log', './bin/ork', [], {
		...catalogEnv,
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
			namespace: { name, description: 'namespaces-editor spec' }
		});
	}
	// The namespaces are fixed but the stack is shared across runs, so the
	// orkflows earlier runs of this spec left in them would otherwise pile up
	// until this run's orkflow no longer sorts onto the Orkflows page's first
	// page and the scoping assertions below look at the wrong page.
	await purgeOwnOrkflows();
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
	await purgeOwnOrkflows().catch((err) => console.warn(`namespaces-editor cleanup: ${err}`));
	await killAll();
});

// A namespace switcher scopes every page, the selection survives
// navigation and reload, and it is in the URL — so a pasted link opens the same
// namespace in a browser that has never seen this app.
test('the namespace switcher scopes every page, survives reload, and travels in the URL', async ({
	page,
	browser
}) => {
	// The default namespace holds neither orkflow — and the address bar says
	// which namespace it is showing even before anybody chooses one, so a URL
	// copied out of it carries the sender's namespace rather than leaving the
	// reader's browser to answer for them.
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await expectNamespace(page, DEFAULT_NS);
	await expect(page).toHaveURL(/[?&]ns=default(&|$)/);
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toHaveCount(0);

	// Switching scopes the list and writes the choice into the URL.
	await selectNamespace(page, ALPHA);
	await page.waitForURL(new RegExp(`ns=${ALPHA}`));
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
	await shot(page, 'screenshot-namespace-alpha.png');

	// Navigation preserves it, and every page is scoped by
	// it: the nav links carry the namespace and each destination reads inside it.
	const nav = page.locator('nav[aria-label="Primary"]');
	await nav.getByRole('link', { name: 'Runs', exact: true }).click();
	await page.waitForURL(new RegExp(`/runs\\?.*ns=${ALPHA}`));
	await expectNamespace(page, ALPHA);
	await expect(page.getByTestId('runs-empty')).toContainText(ALPHA);

	// Submitting lives on each orkflow's own page rather than on a route that
	// asks which orkflow it is looking at, so "submit is scoped" is now the
	// same claim as "the list is scoped, and its links carry the namespace":
	// alpha's list offers alpha's orkflow and not beta's, the link to it keeps
	// the namespace, and the submit dialog opens on that page.
	await nav.getByRole('link', { name: 'Orkflows', exact: true }).click();
	await page.waitForURL(new RegExp(`/orkflows\\?.*ns=${ALPHA}`));
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
	await page.getByTestId(`orkflow-row-${ORKFLOW}`).getByRole('link').click();
	await page.waitForURL(new RegExp(`/orkflows/${ORKFLOW}\\?.*ns=${ALPHA}`));
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-panel')).toBeVisible();
	await page.getByTestId('orkflow-submit-dialog-close').click();

	await nav.getByRole('link', { name: 'Apply orkflow', exact: true }).click();
	await page.waitForURL(new RegExp(`/apply\\?.*ns=${ALPHA}`));
	await expect(page.getByTestId('apply-namespace')).toContainText(ALPHA);

	// Reload preserves it.
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${ALPHA}`);
	await page.reload();
	await expectNamespace(page, ALPHA);

	// A pasted link opens the same namespace in a browser with no localStorage.
	const pasted = `${UI_ORIGIN}/orkflows?ns=${ALPHA}`;
	const fresh = await browser.newContext();
	const colleague = await fresh.newPage();
	await colleague.goto(pasted);
	await expectNamespace(colleague, ALPHA);
	await expect(colleague.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();
	await fresh.close();

	// A bare URL in the browser that made the choice restores it and writes it
	// back into the URL, instead of silently resetting to default.
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await page.waitForURL(new RegExp(`ns=${ALPHA}`));
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toBeVisible();

	// The other namespace is scoped too, and `default` is a choice like any
	// other: it is written into the URL, so the link a user copies out of the
	// address bar says which namespace they were looking at.
	await selectNamespace(page, BETA);
	await page.waitForURL(new RegExp(`ns=${BETA}`));
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toBeVisible();
	await expect(page.getByTestId(`orkflow-row-${ORKFLOW}`)).toHaveCount(0);

	await selectNamespace(page, DEFAULT_NS);
	await page.waitForURL(/[?&]ns=default(&|$)/);
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
	const senderLink = page.url();

	// The colleague case for `default` — the one an absent parameter breaks.
	// A second browser whose last choice was BETA opens the sender's link and
	// must land in DEFAULT. When `default` travelled as silence, this URL was
	// bare, the reader's restore answered it with `editor-beta`, and the same
	// address showed two people two different namespaces' data.
	const other = await browser.newContext();
	const otherPage = await other.newPage();
	await otherPage.goto(`${UI_ORIGIN}/orkflows`);
	await selectNamespace(otherPage, BETA);
	await otherPage.waitForURL(new RegExp(`ns=${BETA}`));
	await expect(otherPage.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toBeVisible();
	await otherPage.goto(senderLink);
	await expectNamespace(otherPage, DEFAULT_NS);
	await expect(otherPage.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
	await expect(otherPage).toHaveURL(/[?&]ns=default(&|$)/);
	await other.close();

	// And the choice sticks in the browser that made it: a bare URL restores
	// `default` rather than dragging the user back to the namespace they left.
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await expectNamespace(page, DEFAULT_NS);
	await expect(page).toHaveURL(/[?&]ns=default(&|$)/);
	await expect(page.getByTestId(`orkflow-row-${BETA_ORKFLOW}`)).toHaveCount(0);
});

// The editor, with validation from the real ApplyOrkflow response, and a
// stale edit detected through base_version that never overwrites.
test('editing applies a new version, real validation errors, and a stale edit is refused', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=edit`);
	await openEditor(page);

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
	await openVersions(page);
	await expect(page.getByTestId('version-row-0.2.0')).toBeVisible();
	await expect(page.getByTestId('version-row-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();

	// 3. Stale edit: a colleague applies 0.3.0 while this editor holds 0.2.0.
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=edit`);
	await openEditor(page);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.0');
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml(ORKFLOW, '0.3.0', 'applied by somebody else'),
		namespace: ALPHA
	});
	await page.getByTestId('editor-yaml').fill(orkflowYaml(ORKFLOW, '0.2.5', 'my edit'));
	await page.getByTestId('editor-apply').click();

	// The refusal is reported, names both versions, and nothing was written.
	// This editor was opened with no `?version=`, so its base and its expected
	// head are the same version (0.2.0) — the default.
	await expect(page.getByTestId('editor-conflict')).toBeVisible();
	await expect(page.getByTestId('editor-conflict')).toContainText('0.2.0');
	await expect(page.getByTestId('editor-conflict-expected')).toHaveText('0.2.0');
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

	// Retargeting is explicit, and it moves the CONCURRENCY TOKEN — the head
	// this edit is checked against — while the provenance stays where the edit
	// really came from. Then the edit applies onto the new
	// head.
	await page.getByTestId('editor-conflict-rebase').click();
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.3.0');
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.0');
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await openVersions(page);
	await expect(page.getByTestId('version-row-0.2.5')).toBeVisible();
});

// Promote with a confirmation step, and rollback is promoting the
// previous version. Nothing is optimistic — the badge moves only after the
// engine says the pointer moved.
test('promoting and rolling back moves the current version, behind a confirmation', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=versions`);
	await openVersions(page);
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

// A diff between any two versions, rendered from the server's
// unified diff — one implementation, so the CLI and the UI cannot drift.
test('diffing two versions renders the engine unified diff', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${ALPHA}&tab=versions`);
	await openVersions(page);
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

// A failed request says what failed and offers a retry — no view
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

// The half a retry has to actually deliver: the Edit tab must
// COME BACK, not merely repaint.
//
// The editor is built out of the head version's document, so an instance built
// while that read was failing holds nothing — and because it keeps its place in
// the template, a SUCCESSFUL retry re-renders that same instance. An empty box
// over an empty base version is a retry in name only: the browser's reload
// button is then the real recovery, which is the state this spec forbids.
test('the Edit tab recovers its document and its base version on retry', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=${ALPHA}`);

	// Break the read the editor is seeded from, then navigate client-side, so
	// the loader really runs in the browser and really fails.
	await page.route('**/orkstack.v1.OrkflowService/ListOrkflowVersions', (route) => route.abort());
	await page.getByTestId(`orkflow-row-${ORKFLOW}`).getByRole('link').click();
	await page.getByTestId('tab-edit').click();

	await expect(page.getByTestId('editor-error-load')).toBeVisible();
	await expect(page.getByTestId('editor-form')).toHaveCount(0);
	await expect(page.getByTestId('editor-loading')).toHaveCount(0);

	// Heal the route and use the view's own retry — not the browser's reload.
	await page.unroute('**/orkstack.v1.OrkflowService/ListOrkflowVersions');
	await page.getByTestId('editor-retry-load').click();
	await openEditor(page);

	// Recovered means the editor holds what a first successful load would have
	// given it: the head version as its base_version token, and the stored
	// document in the box.
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.5');
	await expect(page.getByTestId('editor-yaml')).toHaveValue(new RegExp(`name: ${ORKFLOW}`));
	await shot(page, 'screenshot-editor-retry.png');

	// And it applies, which is what proves the token came back: without one the
	// editor refuses itself with "reload before applying" and stores nothing.
	await page
		.getByTestId('editor-yaml')
		.fill(orkflowYaml(ORKFLOW, '0.4.0', 'applied after a retry'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await openVersions(page);
	await expect(page.getByTestId('version-row-0.4.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.4.0')).toBeVisible();
});
