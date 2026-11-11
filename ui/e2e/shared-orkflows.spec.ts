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
	noteTo,
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
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('shared-orkflows');

// Sharing across namespaces against a real engine, Orker, Temporal and the
// object store (docs/DSL.md "Sharing across namespaces"): an Orkflow of one
// namespace, shared with another through shared_with, is composed there; the
// parent's composed task names and links the child Orkflow in its owner's
// namespace; the child run's page names the Orkflow as <owner>/<name> with a
// "shared from" plate and draws its definition, read from the owner; and the
// owner's Orkflow page lists who it is shared with.
const ENGINE = 'http://127.0.0.1:18681';
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('shared-orkflows');
const STAMP = Date.now().toString(36);
const OWNER = `platform-${STAMP}`;
const CALLER = `search-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'shared-orkflows')
	: path.join(uiDir, 'test-results', 'shared-orkflows');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

const deploy = `apiVersion: orkstack/v1
kind: Orkflow
name: deploy
version: "1.0.0"
shared_with:
  - namespace: ${CALLER}
  - orkflow: other-${STAMP}/release
tasks:
  - id: ship
    uses: shell
    timeout_seconds: 120
    with:
      op: exec
      command: ["sh", "-c", "echo shipped"]
`;
const release = `apiVersion: orkstack/v1
kind: Orkflow
name: release
version: "1"
tasks:
  - id: deploy
    uses: orkflow:${OWNER}/deploy@1.0.0
`;

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18681)) throw new Error('port 18681 already in use');
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ...uiEnv(), ORK_HTTP_ADDR: '127.0.0.1:18681'
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('shared-orkflows') });
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	for (const name of [OWNER, CALLER]) {
		await rpc('NamespaceService/CreateNamespace', { namespace: { name, description: 'shared orkflows' } });
	}
	await rpc('OrkflowService/ApplyOrkflow', { yaml: deploy, namespace: OWNER });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: release, namespace: CALLER });
	note(`owner ${OWNER} shares deploy@1.0.0 with ${CALLER}, whose release composes it; engine ${ENGINE}`);
});

// Cleanup stops exactly the processes this spec started, by handle
// (killStarted), never by name or pattern.
test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a shared Orkflow is composed from another namespace and named with its owner', async ({ page }) => {
	const resp = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: 'release', namespace: CALLER });
	const runId = resp.run.id;
	await page.goto(`${UI}/runs/${runId}?ns=${CALLER}`);
	const childOrkflow = page.getByTestId('task-deploy-child-orkflow');
	await expect(childOrkflow).toHaveText(`${OWNER}/deploy@1.0.0`);
	await expect(childOrkflow).toHaveAttribute('href', `/orkflows/deploy?version=1.0.0&ns=${OWNER}`);
	const link = page.getByTestId('task-deploy-child-link');
	await expect(link).toBeAttached({ timeout: 120_000 });
	const childId = ((await link.textContent()) ?? '').trim();
	await shot(page, 'parent-run.png');

	// The child runs in the caller's namespace, names its owner, and draws
	// the owner's definition.
	await page.goto(`${UI}/runs/${childId}?ns=${CALLER}`);
	await expect(page.getByTestId('pinned-orkflow-link')).toHaveText(`${OWNER}/deploy`);
	await expect(page.getByTestId('pinned-orkflow-link')).toHaveAttribute('href', `/orkflows/deploy?version=1.0.0&ns=${OWNER}`);
	await expect(page.getByTestId('run-orkflow-namespace')).toContainText(OWNER);
	await expect(page.getByTestId('run-status')).toHaveText(/succeeded/i, { timeout: 120_000 });
	// The graph is drawn from the owner's definition, read from its namespace.
	await expect(page.getByTestId('task-ship-status')).toBeAttached();
	await expect(page.getByTestId('run-drawing')).not.toContainText('the definition was not found');
	await shot(page, 'shared-child-run.png');

	// The runs list of the caller's namespace names it the same way.
	await page.goto(`${UI}/runs?ns=${CALLER}`);
	await expect(page.getByText(`${OWNER}/deploy`, { exact: true })).toBeVisible();
	await shot(page, 'runs-list.png');

	// The owner's Orkflow page lists who it is shared with.
	await page.goto(`${UI}/orkflows/deploy?ns=${OWNER}`);
	await expect(page.getByTestId('orkflow-shared-with')).toHaveText(`namespace ${CALLER} · Orkflow other-${STAMP}/release`);
	await shot(page, 'orkflow-shared-with.png');
	note(`child run ${childId} of ${OWNER}/deploy ran in ${CALLER}`);
});

test('composing an Orkflow that is not shared is refused at apply, naming the reason', async () => {
	const unshared = `apiVersion: orkstack/v1
kind: Orkflow
name: private
version: "1"
tasks:
  - id: t
    uses: shell
    with: { op: eval, command: ["true"] }
`;
	await rpc('OrkflowService/ApplyOrkflow', { yaml: unshared, namespace: OWNER });
	const composing = `apiVersion: orkstack/v1
kind: Orkflow
name: uses-private
version: "1"
tasks:
  - id: p
    uses: orkflow:${OWNER}/private@1
`;
	const answer = await rpc<{ issues?: { field?: string; message?: string }[] }>('OrkflowService/ValidateOrkflow', {
		yaml: composing,
		namespace: CALLER
	});
	const issue = (answer.issues ?? []).find((i) => i.field === 'tasks[0].uses');
	expect(issue?.message ?? '').toContain('ORKFLOW_NOT_SHARED');
	note(`validate: ${issue?.message}`);
});
