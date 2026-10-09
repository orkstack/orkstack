import { expect, test, type Page } from './test';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	catalogEnv,
	clusterEnv,
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
	openNamespaceEdit
} from './harness';
import { adminNamespaceUrl, expectNamespace, selectNamespace } from './uiHelpers';

// The Admin section: namespaces, pool health, the activity catalog and
// system information, in a browser, against the real stack.
//
// What is measured here:
//
//   1. /admin is reachable from the main nav, deep-linkable, and its four
//      routes answer from real RPCs;
//   2. writes are HONEST: a create is blocked locally before any RPC for a
//      bad slug, an update sends the etag it read AND preserves fields the
//      form does not expose (labels), a stale etag is shown as a conflict
//      (never silently clobbered or silently retried), a refused delete
//      names the orkflow count it was refused with, `default` is visibly
//      undeletable BEFORE any other namespace exists (not merely among
//      others), and deleting the namespace the app is currently scoped to
//      resets that scope rather than stranding it;
//   3. a partial answer is shown as partial, in every shape this deployment
//      still has: a pool with zero live Orkers (a real zero, with the words
//      that say what to do), a pool health read that could not reach the
//      cluster at all (drawn as unknown, never as a zero), and an Orker
//      probe on the Activities page that goes unanswered while that page's
//      catalog still answers in full — each produced HERE by a genuine
//      outage, not fabricated;
//      confusion — the URL still carries `?ns=`, and the page says whether
//      that selection scopes what it shows;
//   5. loading, empty and error states throughout.
//
// Topology: one engine, THREE Orker-shaped pollers — one on the built-in
// `main` pool, one on a `gpu` pool this spec creates, and one on a `deaf`
// pool that is a real Temporal poller with no activities registered
// (test/tools/deaf-poller) — so pool health has a genuinely
// non-default live pool to report on, and a live pool whose liveness comes
// from polling alone rather than from anything it can answer. The engine and
// its two Orkers (`main`, `gpu`) run against a bucket this run creates and
// this run alone has ever written to (test/tools/bucket-ensure) — the
// shared `orkstack-artifacts` bucket every other e2e spec in this module
// writes to has accumulated a stamped namespace from every earlier run of
// THIS spec too, and "default is the only namespace this deployment has"
// cannot occur there a second time. Isolation per AGENTS.md §11.1: this
// spec's own Temporal namespace, which is a hard partition, so no
// queue-level isolation is additionally required. Each Orker also gets its
// OWN work root, because that is what its identity lock is keyed on and two
// Orkers sharing one root would refuse to boot beside each other.
//
// Two things here are genuinely destructive, and both are placed LAST among
// the tests that need the thing they destroy, in a suite that is `serial`
// specifically so that ordering is the safety rail:
//   - the `main` pool's Orker is killed to prove, for real, that an
//     unanswered DescribeOrker on the Activities page is reported as
//     "nothing was measured" and does not take the catalog down with it.
//     DescribeOrker is dispatched onto the DEFAULT pool's queue (§6.5), so
//     only a genuine outage of that pool produces it — `gpu` staying up is
//     what makes it an unanswered default pool rather than a dead
//     deployment;
//   - this run's OWN Temporal namespace is deleted outright to prove a pool
//     health read that cannot reach the cluster at all — no other black-box
//     lever reproduces that failure (see the comment on that test).

const ENGINE = 'http://127.0.0.1:18085';
const ENGINE_WS = 'ws://127.0.0.1:18085';
const UI_ORIGIN = 'http://127.0.0.1:14177';

const STAMP = Date.now().toString(36);
// Owned by this invocation, never taken from ORK_TEMPORAL_NAMESPACE: the
// outage test deletes it. A fresh namespace also keeps a previous failed
// run's backstop TTL out of this run's initial retention policy. The bucket
// is fresh too, so reusing a namespace would import its already-adjusted TTL
// as the new deployment maximum instead of starting from 72h.
const TEMPORAL_NAMESPACE = `ork-ui-admin-${STAMP}`;
// The image every container task here runs. It is activity-shell rather than a
// bare `alpine:3.20`, because an activity must write
// /orkstack/output/values.json and a stock base image does not — an image that
// exits 0 without one fails VALUES_MISSING. It is still an ordinary OCI image,
// built by `docker build` and pulled by digest.
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const OPS_NS = `admin-ops-${STAMP}`;
const HOLDS_NS = `admin-holds-${STAMP}`;
const HOLDS_ORKFLOW = `admin-holds-orkflow-${STAMP}`;
const LABELLED_NS = `admin-labelled-${STAMP}`;
const LABEL_CONFLICT_NS = `admin-label-conflict-${STAMP}`;
const DOOMED_NS = `admin-doomed-${STAMP}`;
const DELETED_DURING_CONFLICT_NS = `admin-deleted-during-conflict-${STAMP}`;
// This run's own bucket (see the topology note above): created fresh by
// test/tools/bucket-ensure before either binary boots, and never
// shared with another run of this spec.
const BUCKET = process.env.ORK_S3_BUCKET_OVERRIDE ?? `orkstack-admin-${STAMP}`;

// bucketEnv builds the environment `test/tools/bucket-ensure` needs
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

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin')
	: path.join(uiDir, 'test-results', 'admin');
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

// containerOrkflowYaml is a genuinely Docker-backed task, run for real, not
// merely named in a stored definition nobody executes. AGENTS.md's
// integration-test rule ("genuinely exercise real Temporal, MinIO and
// docker") is not satisfied by a document sitting in the catalog —
// ApplyOrkflow only validates and stores YAML, it never runs a task — so this
// orkflow is APPLIED *and submitted*, and the test that does so waits for the
// run to actually succeed before this namespace is used to prove a delete
// refusal.
//
// Under the OCI Activity Contract there is no `container` activity to name,
// because every task already IS a container: the image goes in `uses:` and
// the argv in the task's own `command`.
function containerOrkflowYaml(name: string): string {
	return `name: ${name}
version: "0.1.0"
description: Admin spec — a real docker container, run for real.
tasks:
  - id: probe
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command:
        - echo
        - "admin spec - real Temporal, real MinIO, real docker"
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
			'# The Admin section: namespaces, pool health, activities, system info.',
			'#',
			'# Written by ui/e2e/admin.spec.ts against a real engine + two real Orkers on',
			'# real Temporal + MinIO + Docker.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# artifact bucket:    ${BUCKET} (created fresh by this run alone)`,
			`# ork namespaces:     ${OPS_NS}, ${HOLDS_NS}, ${LABELLED_NS}, ${LABEL_CONFLICT_NS}, ${DOOMED_NS}, ${DELETED_DURING_CONFLICT_NS}, default`,
			`# pools:              main (this run's first Orker), gpu (this run's second), deaf (a real Temporal poller with no activities registered — test/tools/deaf-poller)`,
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
	// commands — the same tool the versioning integration tests already
	// depend on, so this is not a novel prerequisite for this module.
	try {
		execFileSync('temporal', ['--version'], { stdio: 'ignore' });
	} catch {
		throw new Error(
			'the temporal CLI is not on PATH — it is required to induce a genuine ' +
				'pool-unavailable failure (see the last test in this file)'
		);
	}
	// The first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/deaf-poller', './test/tools/deaf-poller'], {
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
	execFileSync('go', ['run', './test/tools/bucket-ensure'], {
		cwd: repoRoot,
		env: bucketEnv('ensure')
	});

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	const store = { ...s3Env, ORK_S3_BUCKET: BUCKET };

	spawnLogged(outPath('engine.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...store,
		ORK_HTTP_ADDR: '127.0.0.1:18085',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	// The default pool's Orker — captured so the last-but-one test can kill
	// THIS process alone, not the `gpu` one, and not through killStarted's
	// shared list (which still cleans it up harmlessly in afterAll: killing
	// an already-dead child is a no-op there).
	mainOrkerProc = spawnLogged(outPath('orker-main.log'), './bin/orker', [], {
		...cluster,
		...store,
		...orkerEnv('admin-main')
	});
	// A second Orker, on a pool NOTHING in this deployment names by default:
	// pool health needs a genuinely live NON-default pool to report on (not
	// just `main`), and the Activities page's unanswered-Orker test needs a
	// pool that is still live once `main` is not, so that the state it proves
	// is "the DEFAULT pool did not answer" and not "this deployment is dead".
	//
	// Its own work root, because that is what the identity lock is keyed on.
	spawnLogged(outPath('orker-gpu.log'), './bin/orker', [], {
		...cluster,
		...store,
		...orkerEnv('admin-gpu'),
		ORK_ORKER_POOL: 'gpu'
	});
	await waitForEngine(ENGINE);
	note(`engine up at ${ENGINE}, temporal namespace ${TEMPORAL_NAMESPACE}, bucket ${BUCKET}`);

	// A THIRD pool, `deaf`: a real Temporal activity poller with NO activity
	// registered (test/tools/deaf-poller). ObservePool counts it as a
	// live Orker exactly like the two above, because liveness is measured
	// from POLLERS and not from anything the poller can answer — so it is
	// what proves a live count does not quietly depend on a working Orker.
	//
	// Started AFTER the engine is up, and that ordering is load-bearing. The
	// engine registers this run's Temporal namespace at boot; the poller has
	// no retry for a namespace that is not there yet and exits immediately
	// with "Namespace ... is not found". The last test in this file DELETES
	// this namespace on purpose, so the first run after a completed run is
	// exactly the one that finds it absent — which is why this raced rarely
	// enough to survive until now, and why it is ordered rather than retried.
	spawnLogged(outPath('deaf-poller.log'), './bin/deaf-poller', [], {
		...cluster,
		ORK_DEAF_QUEUE: 'ork-pool-deaf'
	});

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
	await killStarted();
	// The symmetric half of the `bucket-ensure` call in beforeAll: this
	// run's own bucket is removed once nothing needs it any more, rather
	// than left to accumulate the way the shared `orkstack-artifacts`
	// bucket does (see the topology note at the top of this file). Best
	// effort and non-fatal — a cleanup failure here is a leaked resource to
	// clean up by hand, never a reason to fail a suite that already proved
	// everything it set out to prove.
	try {
		execFileSync('go', ['run', './test/tools/bucket-ensure'], {
			cwd: repoRoot,
			env: bucketEnv('cleanup')
		});
		note(`removed this run's own bucket ${BUCKET}`);
	} catch (err) {
		note(`bucket cleanup for ${BUCKET} failed (leaked, not fatal): ${err}`);
	}
});

// `default`'s delete control is disabled with the reason
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
	// Unfiltered on purpose, the one exception to reaching a row through the
	// filter: what is asserted is the WHOLE registry (one row, on the only
	// page there is), which a filter would narrow away.
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

// Admin joins the main nav, is deep-linkable, and the
// system info page states plainly that it is NOT scoped by the switcher.
test('Admin is reachable from the nav, deep-linkable, and shows this replica\'s system info', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/orkflows?ns=default`);
	await page.locator('nav[aria-label="Primary"]').getByTestId('nav-admin').click();
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
	// deep-linkable.
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
		namespace: { name: HOLDS_NS, description: 'admin spec — holds an orkflow' }
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
	note(`run ${runId} (image ${IMAGE}) succeeded — a real docker container ran`);
});

// Local validation blocks a bad slug before any RPC, a good
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
	await page.getByTestId('ns-create-description').fill('admin spec namespace');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toBeVisible();
	// A create filters the paged list to the created name (q=), so its row
	// is on screen whichever page it would sort onto.
	await expect(page).toHaveURL(new RegExp(`q=${OPS_NS}`));
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toBeVisible();
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('main');
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toContainText('cpu');
	await shot(page, 'screenshot-admin-namespace-created.png');

	// 4. A clean update: open Edit, change the description, save — the etag
	// this form read is still the stored one, so it applies.
	await openNamespaceEdit(page, OPS_NS);
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
	await openNamespaceEdit(page, OPS_NS);
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

// UpdateNamespace replaces the whole mutable document
// (internal/api/namespaces.go), so a form that omits a field silently
// erases it. This namespace's labels are created through the raw RPC — the
// Admin create form exposes no labels control — and then a
// description-only edit through the UI
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

	await page.goto(adminNamespaceUrl(UI_ORIGIN, LABELLED_NS));
	const row = page.getByTestId(`namespace-row-${LABELLED_NS}`);
	await expect(row).toBeVisible();
	await openNamespaceEdit(page, LABELLED_NS);
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

// The stale-etag "reload and re-apply" recovery must not clobber a field a
// CONCURRENT write changed: a retry that re-read only the etag and replayed
// the `editLabels` this form captured at the ORIGINAL open would silently
// revert a label change made by the very write that caused the conflict.
// This namespace starts with one label; the out-of-band write
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

	await page.goto(adminNamespaceUrl(UI_ORIGIN, LABEL_CONFLICT_NS));
	const row = page.getByTestId(`namespace-row-${LABEL_CONFLICT_NS}`);
	await expect(row).toBeVisible();
	await openNamespaceEdit(page, LABEL_CONFLICT_NS);
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

// The namespace an open conflict names can be DELETED before "reload and
// re-apply" is ever clicked. `reloadEtagAndRetry()`
// (ui/src/routes/admin/namespaces/+page.svelte) must then clear
// `editConflict` as well as set `editServerError`, or a NotFound reread
// would still render the amber conflict panel and its retry button —
// pointed at a namespace nothing can reload. This namespace is
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

	await page.goto(adminNamespaceUrl(UI_ORIGIN, DELETED_DURING_CONFLICT_NS));
	const row = page.getByTestId(`namespace-row-${DELETED_DURING_CONFLICT_NS}`);
	await expect(row).toBeVisible();
	await openNamespaceEdit(page, DELETED_DURING_CONFLICT_NS);
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

// A delete refused by the engine names the orkflow count it was
// refused with, verbatim, and `default`'s delete control remains disabled
// with more than one namespace in the list too — not just when it is alone
// (the previous test above proves the alone case, which this bucket can only
// ever be in once).
test('a delete holding an orkflow is refused naming the count, and default remains undeletable among others', async ({
	page
}) => {
	// `default` among others: the registry holds more than it, and its row —
	// reached through the filter, as every row here is — is still
	// undeletable.
	const listed = await rpc<{ namespaces?: { name: string }[] }>('NamespaceService/ListNamespaces', {});
	expect((listed.namespaces ?? []).length).toBeGreaterThan(1);
	await page.goto(adminNamespaceUrl(UI_ORIGIN, 'default'));
	await expect(page.getByTestId('namespace-delete-default')).toBeDisabled();
	await expect(page.getByTestId('namespace-delete-default-reason')).toBeVisible();

	await page.goto(adminNamespaceUrl(UI_ORIGIN, HOLDS_NS));
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

// The namespace this whole app is scoped to (`?ns=` on every route, the
// picker in the header) cannot survive its own deletion: a successful delete
// that only invalidated data would leave the URL, the picker's selection and
// every link built from it naming a namespace the registry no longer has.
test('deleting the currently-selected namespace resets the selection to default', async ({
	page
}) => {
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: DOOMED_NS } });
	note(`namespace ${DOOMED_NS} created, holding no orkflow, to prove the self-delete reset`);

	await page.goto(adminNamespaceUrl(UI_ORIGIN, DOOMED_NS, DOOMED_NS));
	await expectNamespace(page, DOOMED_NS);

	const row = page.getByTestId(`namespace-row-${DOOMED_NS}`);
	await expect(row).toBeVisible();
	await row.getByTestId(`namespace-delete-${DOOMED_NS}`).click();
	await page.getByTestId('namespace-delete-confirm-accept').click();
	await expect(page.getByTestId('namespace-delete-confirm')).toHaveCount(0);

	// The registry no longer has an opinion, and neither does this page: the
	// URL and the picker both move to `default` rather than stranding the
	// selection on a name that no longer resolves.
	await expect(page).toHaveURL(/ns=default/);
	await expectNamespace(page, 'default');
	await expect(page.getByTestId(`namespace-row-${DOOMED_NS}`)).toHaveCount(0);
	await shot(page, 'screenshot-admin-namespace-selection-reset.png');
});

// Pool health respects the namespace switcher, a pool with
// zero live Orkers is drawn as zero WITH the warning (never silently
// healthy-looking), and a live count is reported for every live pool —
// including pools that are not this deployment's default, which is what
// distinguishes "this page shows SOME pool's health" from "this page shows
// the RIGHT pool's health".
//
// There is deliberately no per-pool INVENTORY here. An activity is an image,
// so "what can you run" is not a question for an Orker: which images exist is
// engine configuration read in one place, which is what the Admin Activities
// page shows.
//
// "A partial answer is shown as partial" is therefore proven in two places,
// both real and both produced by genuine outages rather than fabricated: the `cpu` assertions below (zero live
// Orkers, drawn as a real zero with the words that say what to do), the
// wholly-unreachable-cluster test near the end of this file, and the Orker
// half of the Activities page (the test above this one).
test('pool health is namespace-scoped, and reports live Orker counts for every pool', async ({
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
	// (test/tools/deaf-poller, started in beforeAll). It is kept
	// because liveness is measured from POLLERS, not from anything an Orker
	// answers, so a poller that could not run a single task still has to be
	// counted as live — and a page that quietly required a working Orker to
	// show a live count would be wrong in exactly the moment an operator
	// opens it.
	await addPoolToNamespace(OPS_NS, 'deaf');
	await waitFor('the deaf pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{ namespace: OPS_NS }
		);
		return answer.pools?.some((p) => p.name === 'deaf' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});
	note(`namespace ${OPS_NS} widened to allow deaf, a live poller that runs no activities`);

	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pools-scope-caption')).toContainText(OPS_NS);
	await expect(page.getByTestId('pools-scope-caption')).toContainText('honours the namespace');

	// main: a live Orker really is polling for it, and it is the default.
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
	// A zero is a MEASUREMENT, and must not be drawn as the amber
	// "could not be asked" state the unreachable-cluster test proves.
	await expect(cpuCard.getByTestId('pool-unavailable-cpu')).toHaveCount(0);

	// gpu: a SECOND, genuinely live, NON-default pool — this spec's own
	// Orker, started in beforeAll. main and cpu alone cannot distinguish
	// "this page shows SOME pool's health" from "this page shows the RIGHT
	// pool's health".
	const gpuCard = page.getByTestId('pool-card-gpu');
	await expect(gpuCard).toBeVisible();
	await expect(gpuCard.getByTestId('pool-orker-count-gpu')).not.toHaveText('0');
	await expect(gpuCard.getByTestId('pool-default-gpu')).toHaveCount(0);

	// deaf: a THIRD, genuinely live, non-default pool, counted live off its
	// polling alone.
	const deafCard = page.getByTestId('pool-card-deaf');
	await expect(deafCard).toBeVisible();
	await expect(deafCard.getByTestId('pool-orker-count-deaf')).not.toHaveText('0');
	await expect(deafCard.getByTestId('pool-default-deaf')).toHaveCount(0);
	await expect(deafCard.getByTestId('pool-unavailable-deaf')).toHaveCount(0);

	await shot(page, 'screenshot-admin-pools.png');
});

// Loading states are not just something
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

	await selectNamespace(page, 'default');
	await expect(page.getByTestId('pools-loading')).toBeVisible();
	// This page's OWN loading branch takes over, exactly as on the manual
	// Retry path — SvelteKit itself does not
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

// Activity interface manifests (docs/ACTIVITY_IMAGES.md "Declaring the interface"): each
// first-party image carries the org.orkstack.activity.interface label, the
// engine reads it from the image's config during the same resolution that pins
// the digest, and the page shows it — per catalog row, and for any reference
// the Inspect form resolves on demand. Placed BEFORE the catalog test below,
// which kills this spec's `main` Orker; nothing here asks an Orker anything.
test("the Activities page shows each first-party image's declared interface, and Inspect resolves a reference", async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/activities?ns=${OPS_NS}`);

	// The catalog's Interface column, in `orkctl activity list`'s words.
	await expect(page.getByTestId('activity-interface-file')).toContainText('7 ops');
	await expect(page.getByTestId('activity-interface-http')).toContainText('5 keys, 6 ops');
	await expect(page.getByTestId('activity-interface-shell')).toContainText('3 ops');

	// The disclosure opens the full table beneath the row.
	await page.getByTestId('activity-interface-file').click();
	const filePanel = page.getByTestId('activity-interface-panel-file');
	await expect(filePanel.getByTestId('activity-interface-summary')).toContainText(
		'schema 1 · selected by op: (7 ops, op required)'
	);
	await expect(filePanel.getByTestId('activity-interface-row-copy-src')).toContainText('required');
	await expect(filePanel.getByTestId('activity-interface-row-concat-srcs')).toContainText('array');
	await expect(filePanel.getByTestId('activity-interface-value-sha256')).toBeVisible();

	// Inspect a reference: the catalog name resolves to the same pin the row
	// shows, with the same interface.
	const catalogDigest = await page.getByTestId('activity-digest-file').getAttribute('data-digest');
	expect(catalogDigest).toMatch(/^sha256:[a-f0-9]{64}$/);
	await page.getByTestId('activity-inspect-reference').fill('file');
	await page.getByTestId('activity-inspect-submit').click();
	const result = page.getByTestId('activity-inspect-result');
	await expect(result.getByTestId('activity-inspect-activity')).toHaveText('file (catalog)');
	await expect(result.getByTestId('activity-inspect-pinned')).toContainText('sha256:');
	await expect(result.getByTestId('activity-inspect-pinned')).toContainText(`/orkstack/activity-file@${catalogDigest}`);
	await expect(result.getByTestId('activity-interface-row-copy-src')).toContainText('required');
	await expect(result.getByTestId('activity-interface-row-untar-member')).toContainText('required');

	// The CLI equivalent is the same question, asked of `orkctl`.
	await page.getByTestId('activity-inspect-cli-trigger').click();
	await expect(page.getByTestId('activity-inspect-cli-command')).toHaveText('orkctl activity inspect file');
	await shot(page, 'screenshot-admin-activities-interface.png');
	await page.getByTestId('activity-inspect-cli-trigger').click();

	// A reference `uses:` would refuse is refused here too, in the server's
	// own words — never a silently empty result.
	await page.getByTestId('activity-inspect-reference').fill('{{ .x }}');
	await page.getByTestId('activity-inspect-submit').click();
	await expect(page.getByTestId('activity-inspect-error')).toContainText('IMAGE_NOT_PINNED');
	await expect(page.getByTestId('activity-inspect-result')).toHaveCount(0);
	await shot(page, 'screenshot-admin-activities-inspect-refused.png');
	note('activities page: file 7 ops, http 5 keys, 6 ops; inspect file pinned ' + catalogDigest + '; {{ .x }} refused IMAGE_NOT_PINNED');

	// The transform images (docs/ACTIVITY_IMAGES.md): json, git and
	// template reach the page with the interfaces their labels declare — the
	// UI code is unchanged; this is the evidence that catalog and labels flow.
	await expect(page.getByTestId('activity-interface-json')).toContainText('2 ops');
	await expect(page.getByTestId('activity-interface-git')).toContainText('5 keys, 2 ops');
	await expect(page.getByTestId('activity-interface-template')).toContainText('1 op');
	await page.getByTestId('activity-interface-git').click();
	const gitPanel = page.getByTestId('activity-interface-panel-git');
	await expect(gitPanel.getByTestId('activity-interface-summary')).toContainText(
		'schema 1 · selected by op: (2 ops, op required) · 5 keys shared by every op'
	);
	await expect(gitPanel.getByTestId('activity-interface-row-*-url')).toContainText('required');
	await expect(gitPanel.getByTestId('activity-interface-value-commit')).toBeVisible();
	await page.getByTestId('activity-inspect-reference').fill('json');
	await page.getByTestId('activity-inspect-submit').click();
	const jsonResult = page.getByTestId('activity-inspect-result');
	await expect(jsonResult.getByTestId('activity-inspect-activity')).toHaveText('json (catalog)');
	await expect(jsonResult.getByTestId('activity-interface-row-query-filter')).toContainText('required');
	await shot(page, 'screenshot-admin-activities-transform-images.png');
	note('activities page: json 2 ops, git 5 keys, 2 ops (url required, value commit), template 1 op; inspect json: query.filter required');
});

// The activity catalog, and the Orker probe beside it.
//
// The ACTIVITY CATALOG is global (no namespace scoping in the caption) and
// lists what this deployment can run, each entry resolved to an index digest.
// It is engine configuration read in one place (§4.2), so it has no "partial
// row" state at all: there is no Orker to disagree, so a name-only entry
// cannot exist. That is a deliberate simplification of the contract, not a
// coverage gap, and "a partial answer is shown as partial, produced by a
// genuine outage rather than fabricated" is proven on the thing on this page
// that IS asked of a pool: the Orker description.
//
// DescribeOrker is dispatched onto the DEFAULT pool's queue (§6.5), and the
// page reads it INDEPENDENTLY of the catalog. So killing this spec's own
// `main` Orker produces, for real, the exact shape that clause names: one
// half of the page reports that nothing could be measured, in its own
// distinct state, while the other half still answers in full. `gpu` stays up
// throughout, which is what makes this an unanswered DEFAULT pool rather than
// a dead deployment.
test('the activity catalog is global and resolved, and an Orker outage is reported as unmeasured without hiding the catalog', async ({
	page
}) => {
	test.setTimeout(240_000);

	await page.goto(`${UI_ORIGIN}/admin/activities?ns=${OPS_NS}`);
	await expect(page.getByTestId('activities-scope-caption')).toContainText(
		'deployment-wide, not scoped'
	);

	// The catalog, in the RESPONSE's own order — §2.3 makes that order
	// contract in as many words ("the Admin page renders it"), so a
	// client-side sort here would quietly take the guarantee over from the
	// server.
	const names = await page.getByTestId('activity-list').locator('tbody tr').evaluateAll((rows) =>
		rows.map((row) => row.getAttribute('data-testid')?.replace('activity-row-', '') ?? '')
	);
	expect(names).toEqual(['file', 'git', 'http', 'json', 'shell', 'template']);
	for (const name of names) {
		await expect(page.getByTestId(`activity-reference-${name}`)).toContainText(`activity-${name}`);
		// Resolved at APPLY time, so the page shows a digest and not a tag.
		await expect(page.getByTestId(`activity-digest-${name}`)).toContainText('sha256:');
		await expect(page.getByTestId(`activity-digest-unresolved-${name}`)).toHaveCount(0);
	}

	// The Orker half, answered: this is what "measured" looks like, so that
	// the unavailable state below is a CHANGE and not the only thing ever seen.
	await expect(page.getByTestId('orker-card')).toBeVisible();
	await expect(page.getByTestId('orker-pool')).toHaveText('main');
	await expect(page.getByTestId('orker-unavailable')).toHaveCount(0);
	await shot(page, 'screenshot-admin-activities.png');

	// Kill THIS spec's own `main` Orker (captured in beforeAll) — `gpu` stays
	// up. DescribeOrker goes to the default pool's queue, so once `main` is
	// gone there is nobody to answer it, and the engine reports that it could
	// not be measured rather than reporting a zero.
	mainOrkerProc?.kill('SIGTERM');
	note("killed this spec's own main-pool Orker so the default pool cannot answer DescribeOrker");

	await waitFor(
		'DescribeOrker stops being answered once the main-pool Orker is gone',
		async () => {
			try {
				await rpc('ActivityService/DescribeOrker', {});
				return false;
			} catch {
				return true;
			}
		},
		150_000
	);
	note('the default pool no longer answers DescribeOrker');

	// A client-side navigation (not page.goto/page.reload) so the browser's
	// own fetch is the one re-running `load` — the same reasoning as the
	// pool-health test below.
	await page.getByTestId('admin-tab-namespaces').click();
	await page.getByTestId('admin-tab-activities').click();

	// The partial answer, shown as partial: an unanswered probe is its OWN
	// state, and it says nothing was measured rather than showing a zero.
	const unavailable = page.getByTestId('orker-unavailable');
	await expect(unavailable).toBeVisible();
	await expect(unavailable).toContainText('No Orker answered the default pool');
	await expect(unavailable).toContainText('Nothing was measured');
	await expect(page.getByTestId('orker-card')).toHaveCount(0);

	// And the catalog is UNTOUCHED by that outage, which is the whole reason
	// the page reads the two independently: which images exist is engine
	// configuration, and no Orker was ever asked.
	await expect(page.getByTestId('activity-list')).toBeVisible();
	expect(
		await page
			.getByTestId('activity-list')
			.locator('tbody tr')
			.evaluateAll((rows) => rows.length)
	).toBe(names.length);
	await shot(page, 'screenshot-admin-activities-orker-unavailable.png');
});

// Maximum run retention (M-retention-admin): view and change the
// deployment's ceiling on how long any namespace may keep a finished run,
// from the Admin UI, against the real engine and real Temporal this file
// already has running. Placed here — after the activities test above and
// before the pool-health test below that deletes this spec's own Temporal
// namespace outright — because this test needs the underlying
// DescribeNamespace/UpdateNamespace calls to keep working, and nothing after
// it needs a Temporal namespace that still exists.
//
// The write is verified against a SEPARATE, direct `temporal operator
// namespace describe` call — not merely by re-reading through the same
// GetRetentionPolicy RPC the UI itself used — so this test cannot pass on a
// write that only updated in-process state. It proves the underlying
// backstop TTL is max + the enforcement margin (24h, ORK_CLEANUP_RETENTION_BUFFER's
// default) — never the raw requested value, and never something the UI's own
// copy names, since Temporal stays abstracted from the operator.
test('maximum run retention is read from the store and a change through the UI reaches the underlying cluster', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/retention?ns=${OPS_NS}`);
	await expect(page.getByTestId('retention-scope-caption')).toContainText(/deployment-wide/i);
	await expect(page.getByTestId('retention-enforcement')).toBeVisible();
	// EnsureNamespace (internal/task/namespace.go) provisions a fresh
	// namespace at 72h, and archive.RetentionController.Ensure materializes
	// that as the deployment maximum at boot; nothing in this spec has
	// changed it yet.
	await expect(page.getByTestId('retention-current')).toHaveText('72h 0m');
	await shot(page, 'screenshot-admin-retention.png');

	await page.getByTestId('retention-set-hours').fill('2');
	await page.getByTestId('retention-set-submit').click();
	await expect(page.getByTestId('retention-set-success')).toBeVisible();
	await expect(page.getByTestId('retention-current')).toHaveText('2h 0m');
	note('set maximum run retention to 2h through the Admin UI');

	// The independent check: `temporal operator namespace describe` talks to
	// the cluster directly, never through `ork`'s Connect API, so this can
	// only pass if the write actually reached the underlying cluster — and it
	// must report 26h0m0s (2h + the 24h enforcement margin), never the raw 2h.
	const described = execFileSync(
		'temporal',
		[
			'operator',
			'namespace',
			'describe',
			'--namespace',
			TEMPORAL_NAMESPACE,
			'--address',
			'127.0.0.1:7233'
		],
		{ stdio: 'pipe' }
	).toString();
	expect(described).toMatch(/WorkflowExecutionRetentionTtl\s+26h0m0s/);
	note(`verified independently: temporal operator namespace describe reports 26h0m0s (2h maximum + 24h margin) for ${TEMPORAL_NAMESPACE}`);
	await shot(page, 'screenshot-admin-retention-changed.png');

	// A non-positive duration is refused LOCALLY, before any RPC leaves the
	// browser: no request is made, and the stored maximum is untouched.
	await page.getByTestId('retention-set-hours').fill('0');
	await page.getByTestId('retention-set-submit').click();
	await expect(page.getByTestId('retention-set-error')).toContainText('positive');
	await expect(page.getByTestId('retention-current')).toHaveText('2h 0m');
});

// A pool health read that cannot reach the cluster AT ALL
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
// namespace (verified interactively against this SDK version:
// DescribeTaskQueueEnhanced returns
// "Namespace ... is not found" immediately after deletion) is a real
// backend failure, produced with the same `temporal` CLI the versioning
// integration tests already depend on, and it is scoped to a namespace nothing but
// this run's own two Orkers and this run's own engine were ever inside —
// AGENTS.md §11.1's isolation, which is exactly what makes this namespace safe
// to destroy without touching any other agent's cluster state.
//
// Placed last among the tests that need Temporal: nothing after this one
// reads a pool, so nothing here strands a later assertion.
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
	const pools = ['main', 'cpu', 'gpu', 'deaf'];
	await waitFor(
		'pool health reports every pool as unavailable once Temporal is gone',
		async () => {
			await selectNamespace(page, 'default');
			await page.waitForURL(/ns=default/);
			await selectNamespace(page, OPS_NS);
			await page.waitForURL(new RegExp(`ns=${OPS_NS}`));
			// The independent pool probes can observe the deletion at different
			// times. Keep refreshing until the whole asserted snapshot is ready.
			const unavailable = await Promise.all(
				pools.map((pool) => page.getByTestId(`pool-unavailable-${pool}`).isVisible())
			);
			return unavailable.every(Boolean);
		},
		60_000
	);

	for (const pool of pools) {
		const card = page.getByTestId(`pool-card-${pool}`);
		await expect(card).toBeVisible();
		const unavailable = card.getByTestId(`pool-unavailable-${pool}`);
		await expect(unavailable).toBeVisible();
		await expect(unavailable).toContainText('unknown');
		// The number itself is never drawn as zero for an unreachable pool:
		// there is no pool-orker-count testid on the card at all.
		await expect(card.getByTestId(`pool-orker-count-${pool}`)).toHaveCount(0);
		// A pool that could not be asked shows no live-count warning either:
		// "no live Orkers" is a MEASUREMENT and would contradict the amber
		// caption right beside it.
		await expect(card.getByTestId(`pool-no-orkers-${pool}`)).toHaveCount(0);
	}
	await shot(page, 'screenshot-admin-pools-unavailable.png');
});

// A failed read reports the failure and offers a retry, on an
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
	await expect(page.getByTestId('namespace-list')).toBeVisible();
	await expect(page.getByTestId('namespaces-error')).toHaveCount(0);
	// The retried list is the real one: the filter reaches a row it holds.
	await page.getByTestId('namespaces-filter').fill(OPS_NS);
	await page.getByTestId('namespaces-filter').press('Enter');
	await expect(page).toHaveURL(new RegExp(`q=${OPS_NS}`));
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toBeVisible();
});
