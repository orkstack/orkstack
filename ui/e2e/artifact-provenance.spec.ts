import { expect, test } from './test';
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

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('artifact-provenance');

// The Provenance disclosure on a recorded artifact, against a real engine,
// Orker, Temporal and MinIO: the bytes are present, the run's own reference is
// held, the only archived run recording the object is this one, and the
// copyable command is exactly the one runCommand quotes. A second run of the
// same orkflow writes identical bytes and still lists only itself — on the S3
// layout an object key embeds its run id.
const ENGINE = 'http://127.0.0.1:18098';
const ENGINE_WS = 'ws://127.0.0.1:18098';
const UI_ORIGIN = 'http://127.0.0.1:14198';
const TEMPORAL_NAMESPACE = temporalNamespace('artifact-provenance');
const NAME = `artifact-provenance-${Date.now().toString(36)}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'artifact-provenance')
	: path.join(uiDir, 'test-results', 'artifact-provenance');
const out = proofOut(PROOF_DIR);

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function completedWorkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${NAME}
version: "0.1.0"
description: Artifact provenance coverage.
tasks:
  - id: completed
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        printf REPORT > /orkstack/output/artifacts/report.txt
        printf '{"answer":7}' > /orkstack/output/values.json
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18098, 14198]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), bin('ork'), [], {
		...catalogEnv, ...cluster, ...storeEnv(BUCKET), ORK_HTTP_ADDR: '127.0.0.1:18098', ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), bin('orker'), [], { ...cluster, ...storeEnv(BUCKET), ...orkerEnv('artifact-provenance') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14198', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: completedWorkflow() });
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

/** Submits the orkflow and returns once the run's archive holds its marker. */
async function submitArchived(): Promise<string> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME });
	const runId = submitted.run.id;
	// The disclosure answers ABSENT until the terminal event's archive write
	// publishes the run's markers; the page is opened after that, so what it
	// shows is the settled answer rather than a race with the archiver.
	await waitFor(`run ${runId} archived with its reference held`, async () => {
		try {
			const answer = await rpc<{ ownReference?: string }>('RunService/GetArtifactProvenance', {
				runId, namespace: 'default', taskId: 'completed', kind: 'ARTIFACT_KIND_OUTPUT', name: 'report.txt'
			});
			return answer.ownReference === 'REFERENCE_STATE_HELD';
		} catch {
			return false;
		}
	});
	return runId;
}

async function openProvenance(page: import('@playwright/test').Page, runId: string): Promise<void> {
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await page.getByTestId('task-inspect-toggle-completed').click();
	await expect(page.getByTestId('task-artifacts-completed')).toBeVisible();
	// On demand, like Inspect: before the toggle nothing has been asked and no
	// provenance panel exists.
	await expect(
		page.locator('[data-testid^="artifact-provenance-"]:not([data-testid^="artifact-provenance-toggle-"])')
	).toHaveCount(0);
	const toggle = page.getByTestId('artifact-provenance-toggle-completed-report.txt');
	await expect(toggle).toHaveAttribute('aria-expanded', 'false');
	await toggle.click();
	await expect(toggle).toHaveAttribute('aria-expanded', 'true');
}

test('the provenance disclosure shows present bytes, a held reference, this run alone, and the exact CLI command', async ({ page }) => {
	const runId = await submitArchived();
	await openProvenance(page, runId);

	const panel = 'artifact-provenance-completed-report.txt';
	await expect(page.getByTestId(panel)).toBeVisible();
	await expect(page.getByTestId(`${panel}-bytes`)).toHaveText('Bytes present in the object store');
	await expect(page.getByTestId(`${panel}-reference`)).toContainText('held');
	await expect(page.getByTestId(`${panel}-reference`)).toHaveText(
		"held — this run's archive holds its marker on the object"
	);
	const rows = page.getByTestId(panel).getByTestId(`${panel}-reference-row`);
	await expect(rows).toHaveCount(1);
	await expect(rows.first().getByTestId(`${panel}-reference-link`)).toHaveText(runId);
	await expect(rows.first().getByTestId(`${panel}-reference-link`)).toHaveAttribute('href', `/runs/${runId}?ns=default`);
	await expect(rows.first().getByTestId(`${panel}-this-run`)).toHaveText('this run');
	await expect(rows.first()).toContainText(`${NAME}@0.1.0`);
	await expect(rows.first()).toContainText('OUTPUT');
	await expect(rows.first()).toContainText('SUCCEEDED');
	await expect(page.getByTestId(`${panel}-error`)).toHaveCount(0);
	await expect(page.getByTestId(`${panel}-unreadable`)).toHaveCount(0);

	await page.screenshot({ path: out('screenshot-artifact-provenance-panel.png'), fullPage: true });

	await page.getByTestId('artifact-provenance-cli-completed-report.txt-trigger').click();
	await expect(page.getByTestId('artifact-provenance-cli-completed-report.txt-command')).toHaveText(
		`orkctl --namespace 'default' 'run' 'artifact' 'provenance' '${runId}' '--task' 'completed' '--kind' 'output' '--name' 'report.txt'`
	);
	await page.screenshot({ path: out('screenshot-artifact-provenance.png'), fullPage: true });
	await page.getByTestId('artifact-provenance-cli-completed-report.txt-trigger').click();

	// Closing and re-opening asks again: nothing is cached across opens.
	const asked: string[] = [];
	page.on('request', (request) => {
		if (request.url().includes('GetArtifactProvenance')) asked.push(request.url());
	});
	const toggle = page.getByTestId('artifact-provenance-toggle-completed-report.txt');
	await toggle.click();
	await expect(page.getByTestId(panel)).toHaveCount(0);
	await toggle.click();
	await expect(page.getByTestId(`${panel}-reference`)).toContainText('held');
	expect(asked.length).toBe(1);

	// ---- a second run of the same orkflow, identical bytes ------------------
	const secondId = await submitArchived();
	expect(secondId).not.toBe(runId);
	await openProvenance(page, secondId);
	const secondRows = page.getByTestId(panel).getByTestId(`${panel}-reference-row`);
	await expect(page.getByTestId(`${panel}-bytes`)).toHaveText('Bytes present in the object store');
	await expect(secondRows).toHaveCount(1);
	await expect(secondRows.first().getByTestId(`${panel}-reference-link`)).toHaveText(secondId);
	await expect(secondRows.first().getByTestId(`${panel}-reference-link`)).toHaveAttribute('href', `/runs/${secondId}?ns=default`);
	await expect(secondRows.first().getByTestId(`${panel}-this-run`)).toHaveText('this run');
	await page.screenshot({ path: out('screenshot-artifact-provenance-second-run.png'), fullPage: true });
});
