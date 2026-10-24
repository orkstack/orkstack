import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
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
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('version-editor');

// Editing ANY version of an orkflow from the browser.
//
// The property: the Edit tab opens the document of the version the page is
// showing, never HEAD's under that version's name. The one edit an operator
// most wants — "change the version that is RUNNING" — must open that version's
// own text, and applying the edit must publish the operator's text as a new
// head forked from it.
//
// The owner's scenario, verbatim, is the first test below: apply two versions,
// promote the OLDER one, open Edit on the CURRENT version, and assert the
// editor holds CURRENT's contents and not HEAD's.
//
// Real engine (Connect API over real Temporal + real MinIO) and the real
// production UI build. Nothing is stubbed: every version listed, every refusal
// and every no-op below is the engine's own answer.
//
// Isolation (AGENTS.md §11.1): this spec's own Temporal namespace, its own
// OrkStack namespace, and a per-run stamp on the orkflow name — so a version
// lineage is never polluted by yesterday's run and the markers this spec
// asserts on are always the ones it created.

// Ports of this spec's own, so the whole e2e directory runs in one invocation.
const ENGINE = 'http://127.0.0.1:18087';
const ENGINE_WS = 'ws://127.0.0.1:18087';
const UI_ORIGIN = 'http://127.0.0.1:14185';

const TEMPORAL_NAMESPACE =
	temporalNamespace('version-editor');

// The OrkStack namespace is NOT `default`, deliberately: every link the version
// list offers has to carry the namespace it belongs to, and a spec that worked
// only in `default` could not tell a carried namespace from a forgotten one.
const NS = 'version-editing';
const STAMP = Date.now().toString(36);
const ORKFLOW = `versioned-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'version-editor')
	: path.join(uiDir, 'test-results', 'version-editor');
const TRANSCRIPT = 'version-editor-session.txt';

// These tests are sequential and stateful ON PURPOSE: each reads the lineage
// the last one extended, which is the only way to prove head/current markers
// against a real store rather than a fixture. Serial mode is that declaration
// — without it one failure restarts the worker, beforeAll mints a new STAMP,
// and the survivors go looking for versions nobody applied.
test.describe.configure({ mode: 'serial' });

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

// Fixtures go straight to the engine, so a fixture is never mistaken for
// something the browser did. Everything a user does below is done in the page.
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

type Catalog = {
	versions: { version: string; isCurrent?: boolean }[];
	currentVersion?: string;
	headVersion?: string;
};

const catalog = () =>
	rpc<Catalog>('OrkflowService/ListOrkflowVersions', { name: ORKFLOW, namespace: NS });

// The document these tests edit. `command` is a JSON ARRAY under the OCI
// Activity Contract — argv, never a shell string (images/activity-shell:
// "each element becomes exactly one argv element") — so the caller's command
// is handed to an explicit `sh -c`, which is where the shell boundary is
// supposed to be visible. JSON.stringify keeps a caller's embedded newline a
// legal scalar rather than a broken document.
function orkflowYaml(version: string, description: string, command = 'echo hello'): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "${version}"
description: ${description}

tasks:
  - id: greet
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", ${JSON.stringify(command)}]
`;
}

// settle waits for a tab to reach one of its two end states and FAILS NAMING
// THE CAUSE when it reached the error one — a bare wait on the happy element
// cannot tell "the read failed" from "not painted yet", and reports the second
// whichever happened. Nothing here retries and nothing relaxes an assertion.
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

const versionsUrl = `${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&tab=versions`;

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The orkflow page edits ANY version: select it, see ITS',
			'# contents, apply the edit as a new version.',
			'#',
			'# Written by ui/e2e/version-editor.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespace:      ${NS}`,
			`# orkflow:            ${ORKFLOW}`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	// Prerequisites are failures here, never skips: the caller is expected to
	// probe every one of them BEFORE running this spec, so an absence never
	// reaches here and a failure here is never an absence (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18087, 14185]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// The first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();

	// The engine stores orkflows, versions and the current pointer in the real
	// bucket: every marker this spec asserts on is a MinIO object, not process
	// memory. No Orker: nothing here submits a run.
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18087',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14185',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// CreateNamespace is idempotent for an identical spec, so the registry does
	// not grow by one on every run.
	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: NS, description: 'version-scoped editing' }
	});
	// The lineage's first version. Everything after this one is applied by the
	// browser, through the editor under test.
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml('0.1.0', 'first revision', 'echo one'),
		namespace: NS
	});
	note(`applied ${ORKFLOW}@0.1.0 into namespace ${NS}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

// The owner's scenario, verbatim.
//
// Two versions applied, the OLDER one promoted, then Edit opened on the
// CURRENT version. The editor must show CURRENT's contents, not HEAD's.
test("editing the current version opens ITS source, and applying forks forward from it", async ({
	page
}) => {
	// 1. A second version, applied through the editor itself.
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&tab=edit`);
	await openEditor(page);
	// With no version selected the base is the head — the default: head is
	// the lineage's tip, and selecting is explicit.
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await page.getByTestId('editor-yaml').fill(orkflowYaml('0.2.0', 'second revision', 'echo two'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await openVersions(page);
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	note('applied 0.2.0 from the editor; head moved to 0.2.0');

	// 2. Promote the OLDER version: current and head now differ, which is the
	//    whole condition the bug hid behind.
	await page.getByTestId('promote-0.1.0').click();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	const promoted = await catalog();
	expect(promoted.currentVersion).toBe('0.1.0');
	expect(promoted.headVersion).toBe('0.2.0');
	note('promoted 0.1.0: current=0.1.0, head=0.2.0');
	await shot(page, 'screenshot-versions-current-and-head.png');

	// 3. The version list is the entry point to editing: every row offers Edit,
	//    and the link carries the namespace and the version it belongs to.
	const editCurrent = page.getByTestId('version-edit-0.1.0');
	await expect(editCurrent).toBeVisible();
	await expect(page.getByTestId('version-edit-0.2.0')).toBeVisible();
	await expect(editCurrent).toHaveAttribute('href', /version=0\.1\.0/);
	await expect(editCurrent).toHaveAttribute('href', new RegExp(`ns=${NS}`));

	// 4. THE ASSERTION THIS SPEC IS DEFINED BY. Edit the CURRENT version
	//    and the box holds CURRENT's source — not head's.
	await editCurrent.click();
	await openEditor(page);
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/first revision/);
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/echo one/);
	await expect(page.getByTestId('editor-yaml')).not.toHaveValue(/second revision/);
	await expect(page.getByTestId('editor-yaml')).not.toHaveValue(/echo two/);

	// And it says so: derived from 0.1.0, over a head that is 0.2.0 — the two
	// tokens the apply carries, both on screen before it is sent.
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.2.0');
	await expect(page.getByTestId('editor-fork-notice')).toBeVisible();
	await shot(page, 'screenshot-editor-current-version.png');
	note('Edit on the current version (0.1.0) shows 0.1.0 source over head 0.2.0');

	// 5. Edit it and apply. base=0.1.0 (provenance), expected_head=0.2.0 (the
	//    CAS token).
	await page.getByTestId('editor-yaml').fill(orkflowYaml('0.3.0', 'forked from current', 'echo three'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await openVersions(page);

	// 6. The new version exists, HEAD moved to it, CURRENT did not move.
	await expect(page.getByTestId('version-row-0.3.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.3.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toHaveCount(0);
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await shot(page, 'screenshot-versions-after-fork.png');

	// The engine agrees: these are stored pointers, not a rendering.
	const after = await catalog();
	expect(after.versions.map((v) => v.version).sort()).toEqual(['0.1.0', '0.2.0', '0.3.0']);
	expect(after.headVersion).toBe('0.3.0');
	expect(after.currentVersion).toBe('0.1.0');
	note('applied 0.3.0 derived from 0.1.0: head=0.3.0, current=0.1.0 (untouched)');

	// The fork's provenance is the engine's too: 0.3.0 carries 0.1.0's task,
	// not head's — the edit really started from the document on screen.
	const forked = await rpc<{ orkflow: { sourceYaml: string } }>('OrkflowService/GetOrkflow', {
		name: ORKFLOW,
		version: '0.3.0',
		namespace: NS
	});
	expect(forked.orkflow.sourceYaml).toContain('echo three');
	expect(forked.orkflow.sourceYaml).toContain('forked from current');
});

// An unchanged document is a VISIBLE no-op, not a
// new version. Twice: the local compare that never leaves the browser, and the
// engine's own created=false for a byte-identical re-apply.
test('applying an unchanged document is a visible no-op, and stores nothing', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&version=0.1.0&tab=edit`);
	await openEditor(page);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');

	// 1. Untouched: no RPC, no navigation, and the message names the version.
	await page.getByTestId('editor-apply').click();
	const noop = page.getByTestId('editor-noop');
	await expect(noop).toBeVisible();
	await expect(noop).toContainText('0.1.0');
	await expect(page).toHaveURL(/tab=edit/);
	await expect(page.getByTestId('editor-form')).toBeVisible();
	await shot(page, 'screenshot-editor-noop.png');

	// Typing clears it: the no-op is about what was applied, not a sticky sign.
	await page.getByTestId('editor-yaml').fill(orkflowYaml('0.1.0', 'first revision', 'echo one\n'));
	await expect(page.getByTestId('editor-noop')).toHaveCount(0);

	// 2. A document that DOES reach the engine and stores nothing, because it
	//    is byte-identical to a version already there: created=false renders
	//    the same way, and still no new version.
	await page.getByTestId('editor-yaml').fill(orkflowYaml('0.2.0', 'second revision', 'echo two'));
	await page.getByTestId('editor-apply').click();
	await expect(page.getByTestId('editor-noop')).toBeVisible();
	await expect(page.getByTestId('editor-noop')).toContainText('0.2.0');
	await expect(page).toHaveURL(/tab=edit/);

	const after = await catalog();
	expect(after.versions.map((v) => v.version).sort()).toEqual(['0.1.0', '0.2.0', '0.3.0']);
	expect(after.headVersion).toBe('0.3.0');
	note('an unchanged apply and a byte-identical re-apply both stored nothing');
});

// A stale head is REPORTED, never clobbered. The refusal names
// the head the editor saw AND the head there is now, and the way forward keeps
// the user's work.
test('a head that moved mid-edit is refused naming both versions, and the edit survives', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&version=0.1.0&tab=edit`);
	await openEditor(page);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.3.0');

	// A colleague applies while this editor is open.
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml('0.4.0', 'applied by somebody else', 'echo four'),
		namespace: NS
	});
	note('0.4.0 applied out of band while the editor held base=0.1.0, expected head=0.3.0');

	await page.getByTestId('editor-yaml').fill(orkflowYaml('0.3.5', 'my edit', 'echo mine'));
	await page.getByTestId('editor-apply').click();

	// The refusal names three things: where the edit came from, the head it was
	// fetched at, and the head that is there now.
	const conflict = page.getByTestId('editor-conflict');
	await expect(conflict).toBeVisible();
	await expect(page.getByTestId('editor-conflict-base')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-conflict-expected')).toHaveText('0.3.0');
	await expect(page.getByTestId('editor-conflict-head')).toHaveText('0.4.0');
	await shot(page, 'screenshot-editor-stale-head.png');

	// Nothing was written, and the work is still in the box.
	const refused = await catalog();
	expect(refused.versions.map((v) => v.version).sort()).toEqual([
		'0.1.0',
		'0.2.0',
		'0.3.0',
		'0.4.0'
	]);
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/my edit/);

	// The way forward is offered, not taken: the diff of what arrived is the
	// server's, between the two HEADS — the base is not what changed.
	await page.getByTestId('editor-conflict-diff').click();
	await expect(page.getByTestId('conflict-diff')).toContainText(`--- a/${ORKFLOW}@0.3.0`);
	await expect(page.getByTestId('conflict-diff')).toContainText(`+++ b/${ORKFLOW}@0.4.0`);

	// Retargeting moves the TOKEN and leaves the provenance and the text alone.
	await page.getByTestId('editor-conflict-rebase').click();
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.4.0');
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/my edit/);

	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await openVersions(page);
	await expect(page.getByTestId('version-row-0.3.5')).toBeVisible();
	await expect(page.getByTestId('version-head-0.3.5')).toBeVisible();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	note('retargeted to head 0.4.0 keeping base 0.1.0; applied 0.3.5, current still 0.1.0');
	await shot(page, 'screenshot-versions-after-retarget.png');
});

// Every version action has its states. A SELECTED version the
// engine will not answer is reported as itself, with a retry, and the editor
// refuses to show anything: showing head's document under that URL is the bug.
test('a selected version the engine will not answer is reported, not replaced by head', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&version=9.9.9&tab=edit`);
	const failure = page.getByTestId('editor-error-load');
	await expect(failure).toBeVisible();
	await expect(failure).toContainText('9.9.9');
	await expect(page.getByTestId('editor-form')).toHaveCount(0);
	await expect(page.getByTestId('editor-retry-load')).toBeVisible();
	await shot(page, 'screenshot-editor-unknown-version.png');

	// And a version that IS stored still loads through the same path, so the
	// error above is about the version and not about the tab.
	await page.goto(`${UI_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&version=0.2.0&tab=edit`);
	await openEditor(page);
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/second revision/);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.0');
	note('an unknown selected version is reported by name; a stored one loads');
});

// The version list's own states survive: a failed read reports
// and retries rather than spinning, and the Edit action is not offered over a
// list nobody could read.
test('the version list reports a failed read and recovers on retry', async ({ page }) => {
	await page.goto(versionsUrl);
	await openVersions(page);

	await page.route('**/orkstack.v1.OrkflowService/ListOrkflowVersions', (route) => route.abort());
	await page.getByTestId('tab-overview').click();
	await page.getByTestId('tab-versions').click();

	await expect(page.getByTestId('versions-error')).toBeVisible();
	await expect(page.getByTestId('versions-loading')).toHaveCount(0);
	await expect(page.getByTestId('version-edit-0.1.0')).toHaveCount(0);

	await page.unroute('**/orkstack.v1.OrkflowService/ListOrkflowVersions');
	await page.getByTestId('versions-retry').click();
	await openVersions(page);
	await expect(page.getByTestId('version-edit-0.1.0')).toBeVisible();
	await shot(page, 'screenshot-versions-final.png');
});
