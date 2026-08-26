import { expect, test, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	clusterEnv,
	killAll,
	noteTo,
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

// M11.6 — the Admin section: namespaces, pool health, plugin inventory and
// system information, in a browser, against the real stack.
//
// What this milestone claims, and therefore what is measured here:
//
//   1. /admin is reachable from the main nav, deep-linkable, and its four
//      routes implement the design's feature list on real M11.3 RPCs;
//   2. writes are HONEST: a create is blocked locally before any RPC for a
//      bad slug, an update sends the etag it read, a stale etag is shown as a
//      conflict (never silently clobbered or silently retried), a refused
//      delete names the orkflow count it was refused with, and `default` is
//      visibly undeletable before any click;
//   3. a partial answer is shown as partial: a pool the cluster could not
//      reach reports "unknown", never "0" — the two mean opposite things;
//   4. the namespace switcher and the two global views coexist without
//      confusion — the URL still carries `?ns=`, and the page says whether
//      that selection scopes what it shows;
//   5. loading, empty and error states throughout.
//
// Topology: one engine, one Orker (so the default pool has a live count to
// show), the production UI build. Isolation per AGENTS.md §6: this
// milestone's own Temporal namespace, which is a hard partition, so no
// queue-level isolation is additionally required.

const ENGINE = 'http://127.0.0.1:18085';
const ENGINE_WS = 'ws://127.0.0.1:18085';
const UI_ORIGIN = 'http://127.0.0.1:14177';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-m11-6-admin-ui';

const STAMP = Date.now().toString(36);
const OPS_NS = `m11-6-ops-${STAMP}`;
const HOLDS_NS = `m11-6-holds-${STAMP}`;
const HOLDS_ORKFLOW = `m11-6-holds-orkflow-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof-m11-6');
const TRANSCRIPT = 'admin-session.txt';

// These tests share a live registry and build on each other's namespaces, so
// a fresh worker restart after one failure must not re-enter beforeAll with a
// new stamp while the others still expect the old one (namespaces-editor.spec.ts
// carries the identical reasoning).
test.describe.configure({ mode: 'serial' });

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# M11.6 — the Admin section: namespaces, pool health, plugins, system info.',
			'#',
			'# Written by ui/e2e/admin.spec.ts against a real engine + Orker on real',
			'# Temporal + MinIO.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespaces:     ${OPS_NS}, ${HOLDS_NS}, default`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18085, 14177]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);

	spawnLogged(outPath('engine.log'), './bin/ork', [], {
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18085',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), './bin/orker', [], { ...cluster, ...s3Env });
	await waitForEngine(ENGINE);
	note(`engine up at ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}`);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14177',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// The Orker has to be polling before pool health can show a live count.
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (
			answer.pools?.some((p) => p.name === 'main' && (p.liveOrkerCount ?? 0) > 0) ?? false
		);
	});
	note('the default pool "main" reports a live Orker');

	// The namespace this milestone's delete-refusal test needs is created up
	// front and given one stored orkflow, through the API directly: the UI's
	// OWN create form is exercised in the test below, on a different namespace,
	// so both paths (a namespace created by this spec's RPC fixture, and one
	// created through the form under test) are real.
	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: HOLDS_NS, description: 'M11.6 admin proof — holds an orkflow' }
	});
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: `name: ${HOLDS_ORKFLOW}\nversion: "0.1.0"\ndescription: exists only to block a delete\ntasks:\n  - id: noop\n    plugin: shell\n    with:\n      op: exec\n      command: "true"\n`,
		namespace: HOLDS_NS
	});
	note(`namespace ${HOLDS_NS} created and holds orkflow ${HOLDS_ORKFLOW}`);
});

test.afterAll(async () => {
	await killAll();
});

// Acceptance 1 & 4: Admin joins the main nav, is deep-linkable, and the
// system info page states plainly that it is NOT scoped by the switcher.
test('Admin is reachable from the nav, deep-linkable, and shows this replica\'s system info', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await page.locator('header nav').getByTestId('nav-admin').click();
	await expect(page).toHaveURL(/\/admin(\?|$)/);
	await expect(page.getByTestId('admin-nav')).toBeVisible();
	await expect(page.getByTestId('system-info')).toBeVisible();

	// Configuration-derived fields, verbatim from the injected SystemInfo
	// (cmd/ork/main.go) — the same reply `orkctl admin info` renders.
	await expect(page.getByTestId('system-temporal-address')).toHaveText('127.0.0.1:7233');
	await expect(page.getByTestId('system-artifact-backend')).toContainText('orkstack-artifacts');
	await expect(page.getByTestId('system-artifact-backend')).toContainText('s3');
	await expect(page.getByTestId('system-versioning')).toHaveText('off');
	await expect(page.getByTestId('system-server-time')).not.toHaveText('—');
	await shot(page, 'screenshot-admin-overview.png');

	// A pasted link with the tab and the namespace both open the same place —
	// deep-linkable, per design §6.1.
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=${OPS_NS}`);
	await expect(page.getByTestId('admin-tab-namespaces')).toHaveAttribute('aria-current', 'page');
	await expect(page).toHaveURL(new RegExp(`ns=${OPS_NS}`));
});

// Acceptance 2 & 6: local validation blocks a bad slug before any RPC, a good
// create lands with its pool policy, an update round-trips its etag, and a
// STALE etag — the "two tabs" case — is reported as a conflict rather than
// clobbered or silently retried.
test('creating a namespace validates locally, updates round-trip the etag, and a stale etag is a conflict', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('namespaces-scope-caption')).toContainText('does not filter');

	// 1. A bad slug is refused IN THE BROWSER: no request reaches the engine.
	let createCalled = false;
	await page.route('**/orkstack.v1.NamespaceService/CreateNamespace', (route) => {
		createCalled = true;
		return route.continue();
	});
	await page.getByTestId('ns-create-name').fill('Not_A_Slug');
	await page.getByTestId('ns-create-default-pool').fill('main');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-error-name')).toBeVisible();
	expect(createCalled).toBe(false);
	await shot(page, 'screenshot-admin-namespace-local-validation.png');

	// 2. An allow-list that excludes its own default pool is refused locally
	// too — the rule the registry enforces (validNamespacePools).
	await page.getByTestId('ns-create-name').fill(OPS_NS);
	await page.getByTestId('ns-create-default-pool').fill('main');
	await page.getByTestId('ns-create-allowed-pools').fill('cpu, io');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-error-allowed-pools')).toContainText('main');
	expect(createCalled).toBe(false);
	await page.unroute('**/orkstack.v1.NamespaceService/CreateNamespace');

	// 3. A valid create, with a real pool policy, reaches the registry.
	await page.getByTestId('ns-create-allowed-pools').fill('main, cpu');
	await page.getByTestId('ns-create-description').fill('M11.6 admin proof namespace');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toBeVisible();
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toBeVisible();
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('main');
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('cpu');
	await shot(page, 'screenshot-admin-namespace-created.png');

	// 4. A clean update: open Edit, change the description, save — the etag
	// this form read is still the stored one, so it applies.
	const row = page.getByTestId(`namespace-row-${OPS_NS}`);
	await row.getByTestId(`namespace-edit-${OPS_NS}`).click();
	const editForm = page.getByTestId(`namespace-edit-form-${OPS_NS}`);
	await expect(editForm).toBeVisible();
	await editForm.getByTestId(`ns-edit-description-${OPS_NS}`).fill('updated once, cleanly');
	await editForm.getByTestId(`ns-edit-save-${OPS_NS}`).click();
	await expect(editForm).toHaveCount(0);
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('updated once, cleanly');
	await shot(page, 'screenshot-admin-namespace-updated.png');

	// 5. The "two tabs" case: this form re-opens and captures the CURRENT
	// etag, then somebody else (another RPC caller, standing in for a second
	// browser tab) writes underneath it before this one saves.
	await row.getByTestId(`namespace-edit-${OPS_NS}`).click();
	const editForm2 = page.getByTestId(`namespace-edit-form-${OPS_NS}`);
	await expect(editForm2).toBeVisible();

	const current = await rpc<{ namespace: { etag: string } }>('NamespaceService/GetNamespace', {
		name: OPS_NS
	});
	await rpc('NamespaceService/UpdateNamespace', {
		namespace: {
			name: OPS_NS,
			description: 'changed from underneath by somebody else',
			defaultPool: 'main',
			allowedPools: ['main', 'cpu'],
			etag: current.namespace.etag
		}
	});
	note(`namespace ${OPS_NS} updated out-of-band while the UI's edit form held the old etag`);

	await editForm2.getByTestId(`ns-edit-description-${OPS_NS}`).fill('my edit, still in the box');
	await editForm2.getByTestId(`ns-edit-save-${OPS_NS}`).click();

	const conflict = page.getByTestId(`namespace-edit-conflict-${OPS_NS}`);
	await expect(conflict).toBeVisible();
	await expect(conflict).toContainText(/changed since it was read/i);
	// The edit is still in the box: a conflict never discards it.
	await expect(editForm2.getByTestId(`ns-edit-description-${OPS_NS}`)).toHaveValue(
		'my edit, still in the box'
	);
	await shot(page, 'screenshot-admin-namespace-conflict.png');

	// Reloading the etag and re-applying is the documented recovery
	// (UpdateNamespace's own doc comment: "the loser re-reads and re-applies").
	await page.getByTestId(`namespace-edit-conflict-retry-${OPS_NS}`).click();
	await expect(page.getByTestId(`namespace-edit-form-${OPS_NS}`)).toHaveCount(0);
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('my edit, still in the box');
	await shot(page, 'screenshot-admin-namespace-conflict-resolved.png');
});

// Acceptance 2: a delete refused by the engine names the orkflow count it was
// refused with, verbatim, and `default`'s delete control is disabled BEFORE
// any click, with the reason inline — not merely refused after one.
test('a delete holding an orkflow is refused naming the count, and default is visibly undeletable', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);

	await expect(page.getByTestId('namespace-delete-default')).toBeDisabled();
	await expect(page.getByTestId('namespace-delete-default-reason')).toBeVisible();
	await shot(page, 'screenshot-admin-default-undeletable.png');

	const row = page.getByTestId(`namespace-row-${HOLDS_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-delete-${HOLDS_NS}`).click();
	await expect(page.getByTestId('namespace-delete-confirm')).toBeVisible();
	await page.getByTestId('namespace-delete-confirm-accept').click();

	const error = page.getByTestId('namespace-delete-confirm-error');
	await expect(error).toBeVisible();
	// The server's own sentence (internal/api/namespaces.go DeleteNamespace),
	// rendered verbatim: it names the count AND the orkflow in the way.
	await expect(error).toContainText('1 orkflow');
	await expect(error).toContainText(HOLDS_ORKFLOW);
	await shot(page, 'screenshot-admin-delete-refused.png');

	await page.getByTestId('namespace-delete-confirm-cancel').click();
	await expect(page.getByTestId('namespace-delete-confirm')).toHaveCount(0);
	// Refused, not deleted: the row is still there.
	await expect(page.getByTestId(`namespace-row-${HOLDS_NS}`)).toBeVisible();
});

// Acceptance 1 & 3: pool health respects the namespace switcher, a pool with
// zero live Orkers is drawn as zero WITH the warning ("no live Orkers", never
// silently healthy-looking), and a pool the cluster could not answer for is
// drawn as UNKNOWN — never as zero, because the two mean opposite things.
test('pool health is namespace-scoped, and narrows rather than flattens an unreachable pool to zero', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pools-scope-caption')).toContainText(OPS_NS);
	await expect(page.getByTestId('pools-scope-caption')).toContainText('honours the namespace');

	// main: a live Orker really is polling for it.
	const mainCard = page.getByTestId('pool-card-main');
	await expect(mainCard).toBeVisible();
	await expect(mainCard.getByTestId('pool-orker-count-main')).not.toHaveText('0');
	await expect(mainCard.getByTestId('pool-default-main')).toBeVisible();

	// cpu: named in this namespace's allow-list, genuinely nobody serves it.
	// Rendered as a real zero, tinted, with the words that say what to do
	// about it — never a bare "0" a reader would skim past as healthy.
	const cpuCard = page.getByTestId('pool-card-cpu');
	await expect(cpuCard).toBeVisible();
	await expect(cpuCard.getByTestId('pool-orker-count-cpu')).toHaveText('0');
	await expect(cpuCard.getByTestId('pool-no-orkers-cpu')).toContainText('no live Orkers');
	await shot(page, 'screenshot-admin-pools.png');

	// Fault injection: the cluster could not be asked about a pool at all —
	// distinct from "asked, and it is empty". Content-type is JSON because
	// createConnectTransport defaults to useBinaryFormat: false, exactly the
	// wire format the real engine answers with (verified against s.pools.Pools
	// answering NamespaceService.ListPools).
	//
	// Getting a re-fetch under the fault route needs TWO things: a browser
	// fetch (page.route sees the browser's own fetch, never the Node SSR
	// server's — this is why the earlier `page.reload()` attempt here saw
	// nothing at all), and a navigation SvelteKit will not just serve from
	// its own cached load result for a URL already visited. The namespace
	// PICKER'S switch handler already asks for both — `goto(..., {
	// invalidateAll: true })` in +layout.svelte — so it is used here instead
	// of a plain link click, which SvelteKit is free to treat as a no-op
	// revisit of a URL already loaded once above.
	await page.route('**/orkstack.v1.NamespaceService/ListPools', (route) =>
		route.fulfill({
			contentType: 'application/json',
			body: JSON.stringify({
				pools: [
					{
						name: 'gpu',
						unavailableReason: 'fault injected by admin.spec.ts: the task queue could not be described'
					}
				],
				defaultPool: 'main'
			})
		})
	);
	await page.getByTestId('namespace-picker').selectOption('default');
	await page.waitForURL(/ns=default/);
	await page.getByTestId('namespace-picker').selectOption(OPS_NS);
	await page.waitForURL(new RegExp(`ns=${OPS_NS}`));
	const gpuCard = page.getByTestId('pool-card-gpu');
	await expect(gpuCard).toBeVisible();
	await expect(gpuCard.getByTestId('pool-unavailable-gpu')).toBeVisible();
	await expect(gpuCard.getByTestId('pool-unavailable-gpu')).toContainText('unknown');
	// The number itself is never drawn as zero for this pool: there IS no
	// pool-orker-count testid on an unavailable card.
	await expect(gpuCard.getByTestId('pool-orker-count-gpu')).toHaveCount(0);
	await shot(page, 'screenshot-admin-pools-unavailable.png');
	await page.unroute('**/orkstack.v1.NamespaceService/ListPools');
});

// Acceptance 1: the plugin inventory is global (no namespace scoping in the
// caption), lists what the Orker reports, and discloses a schema on request.
test('the plugin inventory is global and discloses schemas on request', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/admin/plugins?ns=${OPS_NS}`);
	await expect(page.getByTestId('plugins-scope-caption')).toContainText('across this whole deployment');

	const shellRow = page.getByTestId('plugin-row-shell');
	await expect(shellRow).toBeVisible();
	await expect(shellRow).toContainText('shell');

	await page.getByTestId('plugin-schema-toggle-shell').click();
	const schema = page.getByTestId('plugin-schema-shell');
	await expect(schema).toBeVisible();
	await expect(page.getByTestId('plugin-input-schema-shell')).toContainText('op');
	await shot(page, 'screenshot-admin-plugins.png');
});

// Acceptance 5: a failed read reports the failure and offers a retry, on an
// Admin route exactly as everywhere else in the app.
test('a failed Admin read reports the failure and retries', async ({ page }) => {
	// Navigate to a neutral page on a normal fetch first, THEN break the read
	// and go client-side (a link click): `load` for the FIRST request of a
	// browser navigation runs on the Node SSR server, invisible to
	// page.route, which only sees the browser's own fetch — the same
	// reason the pool-health fault injection above uses a link click rather
	// than page.goto.
	await page.goto(`${UI_ORIGIN}/admin?ns=default`);
	await page.route('**/orkstack.v1.NamespaceService/ListNamespaces', (route) => route.abort());
	await page.getByTestId('admin-tab-namespaces').click();

	const failure = page.getByTestId('namespaces-error');
	await expect(failure).toBeVisible();
	await expect(page.getByTestId('namespaces-retry')).toBeVisible();
	await shot(page, 'screenshot-admin-namespaces-error.png');

	await page.unroute('**/orkstack.v1.NamespaceService/ListNamespaces');
	await page.getByTestId('namespaces-retry').click();
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toBeVisible();
	await expect(page.getByTestId('namespaces-error')).toHaveCount(0);
});
