import { expect, test, type Page } from '@playwright/test';
import { execFileSync, type ChildProcess } from 'node:child_process';
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
//      bad slug, an update sends the etag it read AND preserves fields the
//      form does not expose (labels), a stale etag is shown as a conflict
//      (never silently clobbered or silently retried), a refused delete
//      names the orkflow count it was refused with, `default` is visibly
//      undeletable BEFORE any other namespace exists (not merely among
//      others), and deleting the namespace the app is currently scoped to
//      resets that scope rather than stranding it;
//   3. a partial answer is shown as partial, in BOTH shapes this milestone
//      has: a pool's own per-pool inventory (Pool.plugins, asserted for
//      every live pool including a NON-default one this spec starts
//      itself) and the deployment-wide inventory's name-only entries
//      (PluginInfo with no version — proto/orkstack/v1/service.proto),
//      produced HERE by a genuine default-pool outage, not fabricated;
//   4. the namespace switcher and the two global views coexist without
//      confusion — the URL still carries `?ns=`, and the page says whether
//      that selection scopes what it shows;
//   5. loading, empty and error states throughout.
//
// Topology: one engine, THREE Orker-shaped pollers — one on the built-in
// `main` pool, one on a `gpu` pool this spec creates, and one on a `deaf`
// pool that is a real Temporal poller with no activities registered
// (proof/M11.6/tools/deaf-poller) — so pool health has a genuinely
// non-default live pool to report on, a genuinely live pool whose plugin
// question genuinely goes unanswered, and the plugin inventory has a
// genuine source for name-only entries once `main` goes quiet (see the
// last two tests). The engine and its two Orkers (`main`, `gpu`) run
// against a bucket this run creates and this run alone has ever
// written to (proof/M11.6/tools/bucket-ensure) — the shared
// `orkstack-artifacts` bucket every other e2e spec in this module writes to
// has accumulated a stamped namespace from every earlier run of THIS spec
// too, and "default is the only namespace this deployment has" cannot occur
// there a second time. Isolation per AGENTS.md §6: this milestone's own
// Temporal namespace, which is a hard partition, so no queue-level isolation
// is additionally required.
//
// Two things here are genuinely destructive, and both are placed LAST among
// the tests that need the thing they destroy, in a suite that is `serial`
// specifically so that ordering is the safety rail:
//   - the `main` pool's Orker is killed to prove the plugin inventory's
//     name-only state for real (a pool answered by NAME ONLY is what a
//     non-default pool reports for a plugin the default pool is NOT
//     currently reachable to describe in full — internal/api/service.go's
//     own dedupePlugins deliberately drops a name-only report for a plugin
//     the default pool already described in full, so this state cannot be
//     produced by running a second Orker of the SAME build alongside a
//     healthy default pool; it requires the default pool to have gone
//     quiet, which is what this does, for real, waiting out the real
//     freshness window (internal/engine/pools.go DefaultPollerFreshness)
//     rather than asserting on a race);
//   - this run's OWN Temporal namespace is deleted outright to prove a pool
//     health read that cannot reach the cluster at all — no other black-box
//     lever reproduces that failure (see the comment on that test).

const ENGINE = 'http://127.0.0.1:18085';
const ENGINE_WS = 'ws://127.0.0.1:18085';
const UI_ORIGIN = 'http://127.0.0.1:14177';

// FORCED, never taken from the environment: this spec deletes this exact
// namespace outright in its last-but-one test (a genuine Temporal outage,
// see that test's own comment) to prove pool health renders every pool as
// unavailable rather than zero. An inherited `ORK_TEMPORAL_NAMESPACE`
// naming something else would move BOTH what this spec's own engine and
// Orkers run against AND what that test destroys onto whatever the caller's
// environment happened to name — reading it here would make this spec's
// most destructive step only as safe as everything that ever set that
// variable in a shell it might inherit from. AGENTS.md's own per-milestone
// isolation already assigned this namespace to M11.6; this is the one
// caller that must not let a different assignment override it.
const TEMPORAL_NAMESPACE = 'ork-m11-6-admin-ui';
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? 'alpine:3.20';

const STAMP = Date.now().toString(36);
const OPS_NS = `m11-6-ops-${STAMP}`;
const HOLDS_NS = `m11-6-holds-${STAMP}`;
const HOLDS_ORKFLOW = `m11-6-holds-orkflow-${STAMP}`;
const LABELLED_NS = `m11-6-labelled-${STAMP}`;
const LABEL_CONFLICT_NS = `m11-6-label-conflict-${STAMP}`;
const DOOMED_NS = `m11-6-doomed-${STAMP}`;
const DELETED_DURING_CONFLICT_NS = `m11-6-deleted-during-conflict-${STAMP}`;
// This run's own bucket (see the topology note above): created fresh by
// proof/M11.6/tools/bucket-ensure before either binary boots, and never
// shared with another run of this spec.
const BUCKET = process.env.ORK_S3_BUCKET_OVERRIDE ?? `orkstack-admin-m11-6-${STAMP}`;

// bucketEnv builds the environment `proof/M11.6/tools/bucket-ensure` needs
// for one action against THIS run's own bucket — shared between the create
// call in beforeAll and the cleanup call in afterAll so the two can never
// drift onto different credentials or a different bucket name.
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

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof-m11-6');
const TRANSCRIPT = 'admin-session.txt';

// These tests share a live registry and build on each other's namespaces (and
// the second one to run — "Admin is reachable…" — must run AFTER the first
// one, which needs `default` to be the ONLY namespace this bucket has ever
// held), so a fresh worker restart after one failure must not re-enter
// beforeAll with a new stamp while the others still expect the old one
// (namespaces-editor.spec.ts carries the identical reasoning).
test.describe.configure({ mode: 'serial' });

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// containerOrkflowYaml is a genuinely Docker-backed task: the container
// plugin, run for real (pkg/plugins/container), not merely named in a stored
// definition nobody executes. AGENTS.md's integration-test rule ("genuinely
// exercise real Temporal, MinIO and docker") is not satisfied by a document
// sitting in the catalog — ApplyOrkflow only validates and stores YAML, it
// never runs a task — so this orkflow is APPLIED *and submitted*, and the
// test that does so waits for the run to actually succeed before this
// namespace is used to prove a delete refusal.
function containerOrkflowYaml(name: string): string {
	return `name: ${name}
version: "0.1.0"
description: M11.6 admin proof — a real docker container, run for real.
tasks:
  - id: probe
    plugin: container
    timeout_seconds: 120
    with:
      image: ${IMAGE}
      pull: missing
      command: echo "m11-6 admin proof - real Temporal, real MinIO, real docker"
      timeout_seconds: 60
`;
}

type NamespaceWire = {
	name: string;
	description: string;
	defaultPool: string;
	allowedPools: string[];
	labels?: Record<string, string>;
	etag: string;
};

// addPoolToNamespace widens an existing namespace's allow-list by one pool,
// preserving every other field exactly as read — the same round trip the
// Admin UI's own edit form now does for labels (ui/src/routes/admin/namespaces/+page.svelte),
// done here through the raw RPC because this is test scaffolding, not the
// behavior under test.
async function addPoolToNamespace(name: string, pool: string): Promise<void> {
	const current = await rpc<{ namespace: NamespaceWire }>('NamespaceService/GetNamespace', {
		name
	});
	const ns = current.namespace;
	await rpc('NamespaceService/UpdateNamespace', {
		namespace: {
			name: ns.name,
			description: ns.description,
			defaultPool: ns.defaultPool,
			allowedPools: [...new Set([...(ns.allowedPools ?? []), pool])],
			labels: ns.labels ?? {},
			etag: ns.etag
		}
	});
}

let mainOrkerProc: ChildProcess | undefined;

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# M11.6 — the Admin section: namespaces, pool health, plugins, system info.',
			'#',
			'# Written by ui/e2e/admin.spec.ts against a real engine + two real Orkers on',
			'# real Temporal + MinIO + Docker.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# artifact bucket:    ${BUCKET} (created fresh by this run alone)`,
			`# ork namespaces:     ${OPS_NS}, ${HOLDS_NS}, ${LABELLED_NS}, ${LABEL_CONFLICT_NS}, ${DOOMED_NS}, ${DELETED_DURING_CONFLICT_NS}, default`,
			`# pools:              main (this run's first Orker), gpu (this run's second), deaf (a real Temporal poller with no activities registered — proof/M11.6/tools/deaf-poller)`,
			`# container image:    ${IMAGE}`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18085, 14177]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	// A genuine pool-unavailable proof (the last test below) deletes this
	// run's OWN Temporal namespace through the `temporal` CLI's operator
	// commands — the same tool proof/M8.4 and proof/M8.5 already depend on
	// (AGENTS.md), so this is not a novel prerequisite for this module.
	try {
		execFileSync('temporal', ['--version'], { stdio: 'ignore' });
	} catch {
		throw new Error(
			'the temporal CLI is not on PATH — it is required to induce a genuine ' +
				'pool-unavailable failure (see the last test in this file)'
		);
	}
	// A genuinely Docker-backed task (containerOrkflowYaml above) needs a
	// reachable daemon and the image on this host, the same check
	// ui/e2e/run-page.spec.ts makes for the identical reason.
	try {
		execFileSync(process.env.ORK_DOCKER_BIN ?? 'docker', ['image', 'inspect', IMAGE], {
			stdio: 'ignore'
		});
	} catch {
		throw new Error(`the container image ${IMAGE} is not on this host`);
	}

	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/deaf-poller', './proof/M11.6/tools/deaf-poller'], {
		cwd: repoRoot
	});
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// This run's own bucket (see the topology note at the top of this file):
	// idempotent, so a re-run against the same stamp is a no-op, and it is
	// created BEFORE either binary boots so the namespace registry both of
	// them open has never held anything but `default`.
	execFileSync('go', ['run', './proof/M11.6/tools/bucket-ensure'], {
		cwd: repoRoot,
		env: bucketEnv('ensure')
	});

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	const store = { ...s3Env, ORK_S3_BUCKET: BUCKET };

	spawnLogged(outPath('engine.log'), './bin/ork', [], {
		...cluster,
		...store,
		ORK_HTTP_ADDR: '127.0.0.1:18085',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	// The default pool's Orker — captured so the last-but-one test can kill
	// THIS process alone, not the `gpu` one, and not through killAll's
	// shared list (which still cleans it up harmlessly in afterAll: killing
	// an already-dead child is a no-op there).
	mainOrkerProc = spawnLogged(outPath('orker-main.log'), './bin/orker', [], {
		...cluster,
		...store
	});
	// A second Orker, on a pool NOTHING in this deployment names by default:
	// pool health's per-pool plugin list is asserted against it (a NON
	// -default live pool, not just `main`), and the deployment-wide plugin
	// inventory's name-only state (the last-but-one test) needs a pool that
	// is still live and reachable once `main` is not.
	spawnLogged(outPath('orker-gpu.log'), './bin/orker', [], {
		...cluster,
		...store,
		ORK_ORKER_POOL: 'gpu'
	});
	// A THIRD pool, `deaf`: a real Temporal activity poller with NO activity
	// registered (proof/M11.6/tools/deaf-poller — see that file's own
	// comment for why this is the only way to produce this state for real).
	// ObservePool counts it as a live Orker exactly like the two above; the
	// ListPlugins activity dispatched to its channel is never answered, so
	// PluginsOnPool's err is non-nil and internal/engine/pools.go's
	// `pool.Plugins` stays empty while `pool.LiveOrkerCount` stays real —
	// the live-count/empty-plugin state acceptance 3 names and that neither
	// the zero-Orker `cpu` pool nor the wholly-unreachable-cluster test
	// below can produce.
	spawnLogged(outPath('deaf-poller.log'), './bin/deaf-poller', [], {
		...cluster,
		ORK_DEAF_QUEUE: 'ork-pool-deaf'
	});
	await waitForEngine(ENGINE);
	note(`engine up at ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);

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

	// Both Orkers have to be polling before pool health can show live counts.
	await waitFor('the main pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return answer.pools?.some((p) => p.name === 'main' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});
	note('the default pool "main" reports a live Orker');

	// This deployment does not yet have a namespace naming `gpu` — ListPools
	// only reports a pool a namespace's own default/allow-list names — so the
	// live check for it is deferred to the pool-health test below, once OPS_NS
	// has been widened to include it. Nothing else in beforeAll depends on it.
});

test.afterAll(async () => {
	await killAll();
	// The symmetric half of the `bucket-ensure` call in beforeAll: this
	// run's own bucket is removed once nothing needs it any more, rather
	// than left to accumulate the way the shared `orkstack-artifacts`
	// bucket does (see the topology note at the top of this file). Best
	// effort and non-fatal — a cleanup failure here is a leaked resource to
	// clean up by hand, never a reason to fail a suite that already proved
	// everything it set out to prove.
	try {
		execFileSync('go', ['run', './proof/M11.6/tools/bucket-ensure'], {
			cwd: repoRoot,
			env: bucketEnv('cleanup')
		});
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

// Acceptance 2: `default`'s delete control is disabled with the reason
// inline BEFORE any click, and this is the one moment this bucket will ever
// again have JUST `default` in it — every later test in this file adds a
// namespace that outlives it. If this state is never proven here it is never
// proven at all: the shared `orkstack-artifacts` bucket every other spec
// writes to already carries a stamped namespace from an earlier run of THIS
// spec, so the same check there would find the table non-empty from the very
// first load.
test('default is undeletable when it is the only namespace this deployment has', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('namespaces-empty')).toBeVisible();
	const list = page.getByTestId('namespace-list');
	await expect(list).toBeVisible();
	await expect(list.getByTestId(/^namespace-row-/)).toHaveCount(1);
	await expect(page.getByTestId('namespace-row-default')).toBeVisible();
	await expect(page.getByTestId('namespace-delete-default')).toBeDisabled();
	await expect(page.getByTestId('namespace-delete-default-reason')).toBeVisible();
	await shot(page, 'screenshot-admin-default-undeletable.png');
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
	await expect(page.getByTestId('system-artifact-backend')).toContainText(BUCKET);
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

// AGENTS.md's integration rule, satisfied for real: a namespace this proof
// later refuses to delete holds an orkflow that genuinely RAN, on a genuine
// Docker container, through the real engine and the real Orker started
// above — not merely a YAML document ApplyOrkflow validated and stored.
test('a docker-backed orkflow this proof holds runs for real, before it is used to refuse a delete', async ({}) => {
	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: HOLDS_NS, description: 'M11.6 admin proof — holds an orkflow' }
	});
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: containerOrkflowYaml(HOLDS_ORKFLOW),
		namespace: HOLDS_NS
	});
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: HOLDS_ORKFLOW,
		namespace: HOLDS_NS
	});
	const runId = submitted.run.id;
	note(`namespace ${HOLDS_NS} created, holds ${HOLDS_ORKFLOW}, submitted run ${runId}`);

	await waitFor(
		'the docker-backed run succeeds',
		async () => {
			const answer = await rpc<{ run?: { status?: string } }>('RunService/GetRun', {
				runId,
				namespace: HOLDS_NS
			});
			return answer.run?.status === 'STATUS_SUCCEEDED';
		},
		120_000
	);
	note(`run ${runId} (container plugin, image ${IMAGE}) succeeded — a real docker container ran`);
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

// Acceptance 2: UpdateNamespace replaces the whole mutable document
// (internal/api/namespaces.go), so a form that omits a field silently
// erases it. This namespace's labels are created through the raw RPC — the
// Admin create form exposes no labels control (design §6.1's feature list
// does not ask for one) — and then a description-only edit through the UI
// is proven NOT to have erased them.
test('editing a namespace through the Admin UI does not erase its labels', async ({ page }) => {
	await rpc('NamespaceService/CreateNamespace', {
		namespace: {
			name: LABELLED_NS,
			description: 'has labels the UI form does not expose',
			labels: { team: 'payments', tier: 'gold' }
		}
	});
	note(`namespace ${LABELLED_NS} created with labels team=payments tier=gold`);

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	const row = page.getByTestId(`namespace-row-${LABELLED_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-edit-${LABELLED_NS}`).click();
	const editForm = page.getByTestId(`namespace-edit-form-${LABELLED_NS}`);
	await expect(editForm).toBeVisible();
	await editForm
		.getByTestId(`ns-edit-description-${LABELLED_NS}`)
		.fill('edited through the UI — labels were never in this form');
	await editForm.getByTestId(`ns-edit-save-${LABELLED_NS}`).click();
	await expect(editForm).toHaveCount(0);
	await expect(page.getByTestId(`namespace-row-${LABELLED_NS}`)).toContainText(
		'edited through the UI'
	);
	await shot(page, 'screenshot-admin-namespace-labels-preserved.png');

	const after = await rpc<{ namespace: NamespaceWire }>('NamespaceService/GetNamespace', {
		name: LABELLED_NS
	});
	expect(after.namespace.labels).toMatchObject({ team: 'payments', tier: 'gold' });
	note(`namespace ${LABELLED_NS} still carries its labels after a UI edit of the description`);
});

// Acceptance 2 (round-2 review): the stale-etag "reload and re-apply"
// recovery must not clobber a field a CONCURRENT write changed — the
// earlier version of this fix re-read only the etag and replayed the
// `editLabels` this form captured at the ORIGINAL open, so a label change
// made by the very write that caused the conflict was silently reverted by
// the retry. This namespace starts with one label; the out-of-band write
// that produces the conflict ALSO adds a second one, and the assertion is
// that both survive the UI's retry, not just the one present when the form
// was first opened.
test('a stale-etag retry re-reads labels too, so a concurrent label change survives it', async ({
	page
}) => {
	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: LABEL_CONFLICT_NS, description: 'before any edit', labels: { team: 'payments' } }
	});
	note(`namespace ${LABEL_CONFLICT_NS} created with labels team=payments`);

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	const row = page.getByTestId(`namespace-row-${LABEL_CONFLICT_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-edit-${LABEL_CONFLICT_NS}`).click();
	const editForm = page.getByTestId(`namespace-edit-form-${LABEL_CONFLICT_NS}`);
	await expect(editForm).toBeVisible();
	// This form's `openEdit` has now captured labels {team: payments} — the
	// value the fix under test must NOT replay verbatim into the retry.

	// Somebody else changes BOTH the description and the labels underneath
	// this open edit, using the etag this form is no longer holding.
	const current = await rpc<{ namespace: NamespaceWire }>('NamespaceService/GetNamespace', {
		name: LABEL_CONFLICT_NS
	});
	await rpc('NamespaceService/UpdateNamespace', {
		namespace: {
			name: LABEL_CONFLICT_NS,
			description: 'changed out from underneath, with a new label too',
			defaultPool: current.namespace.defaultPool,
			allowedPools: current.namespace.allowedPools,
			labels: { team: 'payments', tier: 'gold' },
			etag: current.namespace.etag
		}
	});
	note(
		`namespace ${LABEL_CONFLICT_NS} updated out-of-band: description changed AND ` +
			`a label (tier=gold) added, underneath the UI's open edit`
	);

	await editForm
		.getByTestId(`ns-edit-description-${LABEL_CONFLICT_NS}`)
		.fill('my UI edit, unrelated to labels');
	await editForm.getByTestId(`ns-edit-save-${LABEL_CONFLICT_NS}`).click();

	const conflict = page.getByTestId(`namespace-edit-conflict-${LABEL_CONFLICT_NS}`);
	await expect(conflict).toBeVisible();
	await shot(page, 'screenshot-admin-namespace-conflict-labels.png');

	await page.getByTestId(`namespace-edit-conflict-retry-${LABEL_CONFLICT_NS}`).click();
	await expect(page.getByTestId(`namespace-edit-form-${LABEL_CONFLICT_NS}`)).toHaveCount(0);
	await expect(page.getByTestId(`namespace-row-${LABEL_CONFLICT_NS}`)).toContainText(
		'my UI edit, unrelated to labels'
	);

	const after = await rpc<{ namespace: NamespaceWire }>('NamespaceService/GetNamespace', {
		name: LABEL_CONFLICT_NS
	});
	// Both labels survive: the ORIGINAL one this form captured at open, and
	// the one added by the write that caused the conflict — which the retry
	// only sees by re-reading, not by replaying what this form opened with.
	expect(after.namespace.labels).toMatchObject({ team: 'payments', tier: 'gold' });
	expect(after.namespace.description).toBe('my UI edit, unrelated to labels');
	note(
		`namespace ${LABEL_CONFLICT_NS}: after the conflict retry, labels are ` +
			`${JSON.stringify(after.namespace.labels)} — the concurrent label change survived`
	);
	await shot(page, 'screenshot-admin-namespace-conflict-labels-resolved.png');
});

// Acceptance 2 (round-3 review, the finding this milestone closes): the
// namespace an open conflict names can be DELETED before "reload and
// re-apply" is ever clicked. Before this fix `reloadEtagAndRetry()`
// (ui/src/routes/admin/namespaces/+page.svelte) only set `editServerError`
// when its own `GetNamespace` reread failed; `editConflict` stayed true, so
// a NotFound reread still rendered the amber conflict panel and its retry
// button — pointed at a namespace nothing can reload. This namespace is
// deleted OUT FROM UNDER the open conflict through the raw RPC, standing in
// for a second operator exactly as the two conflict tests above stand in
// for a second browser tab, and the assertion is that the retry click ends
// on the ordinary error path, never on a dead "reload and re-apply" button.
test('deleting a namespace between a conflict and its retry shows a normal error, not a dead retry button', async ({
	page
}) => {
	await rpc('NamespaceService/CreateNamespace', {
		namespace: {
			name: DELETED_DURING_CONFLICT_NS,
			description: 'about to be deleted while its own conflict panel is open'
		}
	});
	note(
		`namespace ${DELETED_DURING_CONFLICT_NS} created, to be deleted while its own conflict panel is open`
	);

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	const row = page.getByTestId(`namespace-row-${DELETED_DURING_CONFLICT_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-edit-${DELETED_DURING_CONFLICT_NS}`).click();
	const editForm = page.getByTestId(`namespace-edit-form-${DELETED_DURING_CONFLICT_NS}`);
	await expect(editForm).toBeVisible();

	// The same "two tabs" stale-etag setup the tests above use.
	const current = await rpc<{ namespace: { etag: string } }>('NamespaceService/GetNamespace', {
		name: DELETED_DURING_CONFLICT_NS
	});
	await rpc('NamespaceService/UpdateNamespace', {
		namespace: {
			name: DELETED_DURING_CONFLICT_NS,
			description: 'changed from underneath by somebody else',
			etag: current.namespace.etag
		}
	});
	note(
		`namespace ${DELETED_DURING_CONFLICT_NS} updated out-of-band while the UI's edit form held the old etag`
	);

	await editForm
		.getByTestId(`ns-edit-description-${DELETED_DURING_CONFLICT_NS}`)
		.fill('my edit, about to hit a namespace that will be gone');
	await editForm.getByTestId(`ns-edit-save-${DELETED_DURING_CONFLICT_NS}`).click();

	const conflict = page.getByTestId(`namespace-edit-conflict-${DELETED_DURING_CONFLICT_NS}`);
	await expect(conflict).toBeVisible();
	await shot(page, 'screenshot-admin-namespace-conflict-then-deleted.png');

	// The namespace this open conflict names is removed entirely — a real
	// delete, by another caller, while the conflict panel is still on
	// screen — so the reread the retry button is about to perform has
	// nothing left to reread.
	await rpc('NamespaceService/DeleteNamespace', { name: DELETED_DURING_CONFLICT_NS });
	note(`namespace ${DELETED_DURING_CONFLICT_NS} deleted out from under its own open conflict panel`);

	await page.getByTestId(`namespace-edit-conflict-retry-${DELETED_DURING_CONFLICT_NS}`).click();

	// The dead "reload and re-apply" panel is gone — both the amber
	// conflict box and its retry button — and the ordinary error path takes
	// over, naming the same failure a bare GetNamespace would have.
	await expect(conflict).toHaveCount(0);
	await expect(
		page.getByTestId(`namespace-edit-conflict-retry-${DELETED_DURING_CONFLICT_NS}`)
	).toHaveCount(0);
	const normalError = page.getByTestId(`namespace-edit-error-${DELETED_DURING_CONFLICT_NS}`);
	await expect(normalError).toBeVisible();
	await expect(normalError).toContainText(/does not exist/i);
	await shot(page, 'screenshot-admin-namespace-conflict-deleted-normal-error.png');
});

// Acceptance 2: a delete refused by the engine names the orkflow count it was
// refused with, verbatim, and `default`'s delete control remains disabled
// with more than one namespace in the list too — not just when it is alone
// (the previous test above proves the alone case, which this bucket can only
// ever be in once).
test('a delete holding an orkflow is refused naming the count, and default remains undeletable among others', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);

	await expect(page.getByTestId('namespace-delete-default')).toBeDisabled();
	await expect(page.getByTestId('namespace-delete-default-reason')).toBeVisible();

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

// Acceptance 4: the namespace this whole app is scoped to (`?ns=` on every
// route, the picker in the header) cannot survive its own deletion. Before
// this fix, a successful delete only invalidated data — the URL, the
// picker's selection and every link built from it kept naming a namespace
// the registry no longer had.
test('deleting the currently-selected namespace resets the selection to default', async ({
	page
}) => {
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: DOOMED_NS } });
	note(`namespace ${DOOMED_NS} created, holding no orkflow, to prove the self-delete reset`);

	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=${DOOMED_NS}`);
	await expect(page.getByTestId('namespace-picker')).toHaveValue(DOOMED_NS);

	const row = page.getByTestId(`namespace-row-${DOOMED_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-delete-${DOOMED_NS}`).click();
	await page.getByTestId('namespace-delete-confirm-accept').click();
	await expect(page.getByTestId('namespace-delete-confirm')).toHaveCount(0);

	// The registry no longer has an opinion, and neither does this page: the
	// URL and the picker both move to `default` rather than stranding the
	// selection on a name that no longer resolves.
	await expect(page).toHaveURL(/ns=default/);
	await expect(page.getByTestId('namespace-picker')).toHaveValue('default');
	await expect(page.getByTestId(`namespace-row-${DOOMED_NS}`)).toHaveCount(0);
	await shot(page, 'screenshot-admin-namespace-selection-reset.png');
});

// Acceptance 1 & 3: pool health respects the namespace switcher, a pool with
// zero live Orkers is drawn as zero WITH the warning (never silently
// healthy-looking), every LIVE pool's per-pool plugin list is reported —
// including a pool that is not this deployment's default, which is the
// state acceptance 1's "per-pool plugins" clause actually names — and a
// LIVE pool whose plugin question genuinely goes unanswered (`deaf`, a real
// Temporal poller with no activities registered, proof/M11.6/tools/deaf-poller)
// is shown as unknown/incomplete rather than as a report that came back
// empty — the third partial shape acceptance 3 requires, distinct from
// both "zero Orkers" (`cpu`, below) and "the pool could not be reached at
// all" (the real-outage test near the end of this file).
test('pool health is namespace-scoped, and reports live Orker counts and per-pool plugins for every pool', async ({
	page
}) => {
	// OPS_NS is widened to include `gpu` — the second Orker this file's
	// beforeAll started — through the raw RPC (test scaffolding), not the
	// UI: the UI round trip for allowed_pools is already proven by the
	// "creating a namespace…" test above.
	await addPoolToNamespace(OPS_NS, 'gpu');
	await waitFor('the gpu pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{ namespace: OPS_NS }
		);
		return answer.pools?.some((p) => p.name === 'gpu' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});
	note(`namespace ${OPS_NS} widened to allow gpu, which reports a live Orker`);
	// `deaf`: a real Temporal poller with NO activities registered
	// (proof/M11.6/tools/deaf-poller, started in beforeAll) — a genuinely
	// live Orker count, and a genuinely failed plugin question, both
	// produced for real rather than by injecting a response at the network
	// layer. This is the ONLY black-box way to reach
	// internal/engine/pools.go's `r.Plugins.PluginsOnPool` error branch: a
	// pool with zero live Orkers is never asked (see the `cpu` assertions
	// below), and killing a whole Orker only turns its liveness off too
	// (the real-outage test near the end of this file), which is the OTHER
	// partial state, not this one.
	await addPoolToNamespace(OPS_NS, 'deaf');
	await waitFor('the deaf pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{ namespace: OPS_NS }
		);
		return answer.pools?.some((p) => p.name === 'deaf' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});
	note(`namespace ${OPS_NS} widened to allow deaf, a live poller that cannot answer ListPlugins`);

	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pools-scope-caption')).toContainText(OPS_NS);
	await expect(page.getByTestId('pools-scope-caption')).toContainText('honours the namespace');

	// main: a live Orker really is polling for it, and it reports the real
	// plugin set that Orker was built with.
	const mainCard = page.getByTestId('pool-card-main');
	await expect(mainCard).toBeVisible();
	await expect(mainCard.getByTestId('pool-orker-count-main')).not.toHaveText('0');
	await expect(mainCard.getByTestId('pool-default-main')).toBeVisible();
	await expect(mainCard.getByTestId('pool-plugins-main')).toContainText('shell');
	await expect(mainCard.getByTestId('pool-plugins-main')).toContainText('container');

	// cpu: named in this namespace's allow-list, genuinely nobody serves it.
	// Rendered as a real zero, tinted, with the words that say what to do
	// about it — never a bare "0" a reader would skim past as healthy.
	const cpuCard = page.getByTestId('pool-card-cpu');
	await expect(cpuCard).toBeVisible();
	await expect(cpuCard.getByTestId('pool-orker-count-cpu')).toHaveText('0');
	await expect(cpuCard.getByTestId('pool-no-orkers-cpu')).toContainText('no live Orkers');
	// Round-2 review: a pool with zero live Orkers is never asked for
	// plugins at all (internal/engine/pools.go only calls PluginsOnPool
	// when live_orker_count > 0), so its plugin caption must say "unknown",
	// never "sampled" — this is a DIFFERENT reason than the amber
	// unavailable-pool caption below, and gets its own testid.
	await expect(cpuCard.getByTestId('pool-plugins-no-live-orkers-cpu')).toContainText('unknown');
	await expect(cpuCard.getByTestId('pool-plugins-cpu')).toHaveCount(0);
	await expect(cpuCard.getByTestId('pool-plugins-unknown-cpu')).toHaveCount(0);

	// gpu: a SECOND, genuinely live, NON-default pool — this spec's own
	// Orker, started in beforeAll — with its own real plugin list. This is
	// the per-pool plugin assertion the acceptance's "pool health with …
	// per-pool plugins" clause names and an earlier version of this file
	// never made: main and cpu alone cannot distinguish "this page shows
	// SOME pool's plugins" from "this page shows the RIGHT pool's plugins".
	const gpuCard = page.getByTestId('pool-card-gpu');
	await expect(gpuCard).toBeVisible();
	await expect(gpuCard.getByTestId('pool-orker-count-gpu')).not.toHaveText('0');
	await expect(gpuCard.getByTestId('pool-default-gpu')).toHaveCount(0);
	await expect(gpuCard.getByTestId('pool-plugins-gpu')).toContainText('shell');
	await expect(gpuCard.getByTestId('pool-plugins-gpu')).toContainText('container');

	// deaf: a THIRD, genuinely live, non-default pool whose Orker cannot
	// answer ListPlugins (proof/M11.6/tools/deaf-poller) — the live-count/
	// empty-plugin partial state acceptance 3 requires, distinct from `cpu`
	// (never asked, because there is no live Orker) and from the
	// wholly-unreachable-cluster state proven near the end of this file
	// (no live count at all). The Orker count is real and positive; the
	// caption plainly says the inventory is unknown/incomplete rather than
	// implying an answer that happened to be empty.
	const deafCard = page.getByTestId('pool-card-deaf');
	await expect(deafCard).toBeVisible();
	await expect(deafCard.getByTestId('pool-orker-count-deaf')).not.toHaveText('0');
	await expect(deafCard.getByTestId('pool-default-deaf')).toHaveCount(0);
	await expect(deafCard.getByTestId('pool-plugins-deaf')).toHaveCount(0);
	const deafUnknown = deafCard.getByTestId('pool-plugins-unknown-deaf');
	await expect(deafUnknown).toBeVisible();
	await expect(deafUnknown).toContainText('unknown');
	await expect(deafUnknown).not.toContainText('sampled');
	await expect(deafCard.getByTestId('pool-plugins-no-live-orkers-deaf')).toHaveCount(0);
	await expect(deafCard.getByTestId('pool-plugins-unavailable-deaf')).toHaveCount(0);

	await shot(page, 'screenshot-admin-pools.png');
});

// Acceptance 5 (round-2 review): loading states are not just something
// `retrying` (a manual click) turns on — an ordinary CLIENT-SIDE navigation
// stalled on a slow RPC must show one too, both for a navigation BETWEEN
// Admin tabs (the destination route's own `+page.svelte` does not exist
// until its `load` resolves, so this layout's own indicator is what a
// reader sees) and for a navigation that STAYS on the same route (the
// namespace switcher on Pools, which reruns `load` without changing the
// component SvelteKit has mounted).
//
// The delay is injected on the BROWSER's own fetch (`page.route`), exactly
// as the existing "a failed Admin read…" test at the bottom of this file
// does, and for the identical reason: a client-side navigation's `load`
// runs in the browser, unlike the first request of a `page.goto`, which
// runs on the Node SSR server where `page.route` cannot see it.
test('a client-side navigation between Admin tabs shows a loading state until the destination is ready', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin?ns=default`);
	await expect(page.getByTestId('system-info')).toBeVisible();
	await expect(page.getByTestId('admin-nav-loading')).toHaveCount(0);

	let release: () => void = () => {};
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	await page.route('**/orkstack.v1.NamespaceService/ListNamespaces', async (route) => {
		await gate;
		await route.continue();
	});

	await page.getByTestId('admin-tab-namespaces').click();
	await expect(page.getByTestId('admin-nav-loading')).toBeVisible();
	// SvelteKit does not swap the mounted page until the destination's
	// `load` resolves: Overview's own content is still what is on screen.
	await expect(page.getByTestId('system-info')).toBeVisible();
	await shot(page, 'screenshot-admin-nav-loading.png');

	release();
	await expect(page.getByTestId('admin-nav-loading')).toHaveCount(0);
	await expect(page.getByTestId('namespace-list')).toBeVisible();
	await page.unroute('**/orkstack.v1.NamespaceService/ListNamespaces');
});

// The same requirement, for a navigation that does NOT change route: the
// namespace switcher re-navigates `/admin/pools?ns=…` in place, and this
// page's own `pools-loading` text — driven by its own `navigatingHere`, not
// by the manual `retrying` flag `Retry` sets — is what a reader sees, in
// place of its now-stale cards, for the span the fetch is gated.
test('switching namespace while on Pools shows this page\'s own loading state, not the manual Retry flag', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pool-card-main')).toBeVisible();
	await expect(page.getByTestId('pools-loading')).toHaveCount(0);

	let release: () => void = () => {};
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	await page.route('**/orkstack.v1.NamespaceService/ListPools', async (route) => {
		await gate;
		await route.continue();
	});

	await page.getByTestId('namespace-picker').selectOption('default');
	await expect(page.getByTestId('pools-loading')).toBeVisible();
	// This page's OWN loading branch takes over, exactly as the manual
	// Retry path already did before this fix — SvelteKit itself does not
	// swap `data` until the destination `load` resolves, but this
	// component's `busy` branch pre-empts the stale card render the moment
	// the same kind of re-navigation Retry causes is ALSO in flight for a
	// reason other than the Retry click.
	await shot(page, 'screenshot-admin-pools-loading.png');

	release();
	await page.waitForURL(/ns=default/);
	await expect(page.getByTestId('pools-loading')).toHaveCount(0);
	await expect(
		page.getByTestId('pool-list').or(page.getByTestId('pools-empty'))
	).toBeVisible();
	await page.unroute('**/orkstack.v1.NamespaceService/ListPools');
});

// Acceptance 1 & 3: the plugin inventory is global (no namespace scoping in
// the caption), lists what the default pool's Orker reports IN FULL, and
// discloses a schema on request. Then, once the default pool genuinely goes
// quiet, the SAME inventory narrows to name-only entries — a real partial
// answer, produced by killing this spec's own `main` Orker and waiting out
// the real freshness window, not by injecting a response at the network
// layer. See the file-level comment for why this is the only way to produce
// this state without a code change outside SCOPE LOCK: a second Orker of the
// same build running ALONGSIDE a healthy default pool cannot produce it,
// because internal/api/service.go's dedupePlugins deliberately drops a
// name-only report for a plugin the default pool already described in full
// (verified against this exact build before this test was written).
test('the plugin inventory is global, discloses schemas on request, and narrows to name-only entries once the default pool goes quiet', async ({
	page
}) => {
	test.setTimeout(240_000);

	await page.goto(`${UI_ORIGIN}/admin/plugins?ns=${OPS_NS}`);
	await expect(page.getByTestId('plugins-scope-caption')).toContainText('across this whole deployment');

	const shellKey = 'shell@0.1.0';
	const shellRow = page.getByTestId(`plugin-row-${shellKey}`);
	await expect(shellRow).toBeVisible();
	await expect(shellRow).toContainText('shell');
	await expect(shellRow).toContainText('0.1.0');
	await expect(page.getByTestId(`plugin-partial-${shellKey}`)).toHaveCount(0);

	await page.getByTestId(`plugin-schema-toggle-${shellKey}`).click();
	const schema = page.getByTestId(`plugin-schema-${shellKey}`);
	await expect(schema).toBeVisible();
	await expect(page.getByTestId(`plugin-input-schema-${shellKey}`)).toContainText('op');
	await shot(page, 'screenshot-admin-plugins.png');

	// Kill THIS spec's own `main` Orker (captured in beforeAll) — `gpu`
	// stays up. Once the real freshness window (60s,
	// internal/engine/pools.go DefaultPollerFreshness) elapses, ListPlugins
	// stops asking the default pool at all (it is no longer fresh) and
	// reports only what `gpu` answers, by name — genuinely partial, because
	// nothing but a real outage of the default pool produces it.
	mainOrkerProc?.kill('SIGTERM');
	note('killed this spec\'s own main-pool Orker to let the default pool go stale for real');

	await waitFor(
		'the plugin inventory narrows to name-only entries once main is stale',
		async () => {
			const answer = await rpc<{ plugins?: { name: string; version?: string }[] }>(
				'PluginService/ListPlugins',
				{}
			);
			const shell = answer.plugins?.find((p) => p.name === 'shell');
			return shell !== undefined && (shell.version ?? '') === '';
		},
		150_000
	);
	note('the deployment-wide plugin inventory now reports name-only entries from gpu alone');

	// A client-side navigation (not page.goto/page.reload) so the browser's
	// own fetch is the one re-running `load` — the identical reasoning the
	// "A note on Playwright and SSR" section of proof/M11.6/README.md gives
	// for the pool-health test below.
	await page.getByTestId('admin-tab-namespaces').click();
	await page.getByTestId('admin-tab-plugins').click();

	const partialKey = 'shell@unreported';
	const partialRow = page.getByTestId(`plugin-row-${partialKey}`);
	await expect(partialRow).toBeVisible();
	await expect(page.getByTestId(`plugin-partial-${partialKey}`)).toBeVisible();
	await expect(page.getByTestId(`plugin-unreported-${partialKey}`)).toContainText('not reported');
	await expect(page.getByTestId(`plugin-description-unreported-${partialKey}`)).toContainText(
		'not reported'
	);
	await expect(page.getByTestId(`plugin-schema-unreported-${partialKey}`)).toContainText(
		'not reported'
	);
	// The fully-described row is gone: the default pool that reported it is
	// the one that just went quiet.
	await expect(page.getByTestId(`plugin-row-${shellKey}`)).toHaveCount(0);
	await shot(page, 'screenshot-admin-plugins-partial.png');
});

// Acceptance 1 & 3: a pool health read that cannot reach the cluster AT ALL
// renders every pool it asked about as UNKNOWN, never as a zero — proven
// against a REAL Temporal outage, not a fabricated response.
//
// WHY A REAL TEMPORAL NAMESPACE DELETION, AND NOT SOMETHING NARROWER: the
// probe this page depends on (TemporalPoolProbe.ObservePool,
// internal/engine/pool_probe.go) calls DescribeTaskQueueEnhanced, which
// fails only for a cluster-level reason — an unreachable server or an
// invalid/missing Temporal namespace — never for a PER-POOL reason a
// black-box test could aim at one queue and not another: a pool name valid
// under this deployment's own slug rule always resolves to a syntactically
// valid Temporal task queue name (internal/task/pool.go), so nothing about
// an individual pool's name can make the cluster refuse to answer for it
// while answering for its neighbour. Deleting this run's OWN Temporal
// namespace (verified interactively against this SDK version before this
// test was written: DescribeTaskQueueEnhanced returns
// "Namespace ... is not found" immediately after deletion) is a real
// backend failure, produced with the same `temporal` CLI proof/M8.4 and
// proof/M8.5 already depend on, and it is scoped to a namespace nothing but
// this run's own two Orkers and this run's own engine were ever inside —
// AGENTS.md §6's isolation, which is exactly what makes this namespace safe
// to destroy without touching any other agent's cluster state.
//
// Placed last among the tests that need Temporal: nothing after this one
// reads a pool or a plugin, so nothing here strands a later assertion.
test('a pool health read that cannot reach the cluster reports every pool as unknown, never as zero — proven by a real Temporal outage', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pool-card-main')).toBeVisible();

	execFileSync(
		'temporal',
		[
			'operator',
			'namespace',
			'delete',
			'--namespace',
			TEMPORAL_NAMESPACE,
			'--address',
			'127.0.0.1:7233',
			'--yes'
		],
		{ stdio: 'pipe' }
	);
	note(`deleted this run's own Temporal namespace ${TEMPORAL_NAMESPACE} — a real cluster outage`);

	// A client-side navigation forces the re-fetch this assertion depends
	// on: SvelteKit does not always re-run `load` for a URL already visited
	// in this session, and page.route (unused here) would not see the Node
	// SSR server's first fetch of a page.goto anyway. The namespace picker's
	// switch handler already calls goto(..., { invalidateAll: true }).
	await waitFor(
		'pool health reports every pool as unavailable once Temporal is gone',
		async () => {
			await page.getByTestId('namespace-picker').selectOption('default');
			await page.waitForURL(/ns=default/);
			await page.getByTestId('namespace-picker').selectOption(OPS_NS);
			await page.waitForURL(new RegExp(`ns=${OPS_NS}`));
			const unavailable = page.getByTestId('pool-unavailable-main');
			return (await unavailable.count()) > 0;
		},
		60_000
	);

	for (const pool of ['main', 'cpu', 'gpu', 'deaf']) {
		const card = page.getByTestId(`pool-card-${pool}`);
		await expect(card).toBeVisible();
		const unavailable = card.getByTestId(`pool-unavailable-${pool}`);
		await expect(unavailable).toBeVisible();
		await expect(unavailable).toContainText('unknown');
		// The number itself is never drawn as zero for an unreachable pool:
		// there is no pool-orker-count testid on the card at all.
		await expect(card.getByTestId(`pool-orker-count-${pool}`)).toHaveCount(0);
		// Round-2 review: an unavailable pool's plugin caption must say it
		// was never asked, not that it was "sampled" — there was no live
		// count to gate a plugin request on in the first place.
		await expect(card.getByTestId(`pool-plugins-unavailable-${pool}`)).toContainText('unknown');
		await expect(card.getByTestId(`pool-plugins-${pool}`)).toHaveCount(0);
	}
	await shot(page, 'screenshot-admin-pools-unavailable.png');
});

// Acceptance 5: a failed read reports the failure and offers a retry, on an
// Admin route exactly as everywhere else in the app. ListNamespaces never
// touches Temporal (internal/api/namespaces.go reads only the durable
// registry), so this still works after the real outage above.
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
