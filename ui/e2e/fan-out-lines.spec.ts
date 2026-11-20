import { expect, test, type Page } from './test';
import { selectTask } from './uiHelpers';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
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
	startTranscript,
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

/** This spec's own bucket (harness.ts ownBucket). */
const BUCKET = ownBucket('fan-out-lines');

// A fan-out over an artifact's lines on the run page (docs/DSL.md "Fan-out
// over an artifact's lines"): its card names the source and the batch, reads
// "Indexing …" while an Orker of its pool has its line index in flight, and
// then fills its grid like any fan-out.
//
// The first run's producer, `list`, builds the line index as it commits its
// artifact, so its fan-out never reads "Indexing". `list` is cached: the
// second run's is a memo hit, which builds no index, so that run's fan-out
// indexes the lines itself — and that is the state this spec holds.
//
// Indexing is held observable without a timer: the only Orker runs one task
// at a time (ORK_ORKER_MAX_CONCURRENT=1), and `convert` also needs `go`, an
// await this spec resolves only once `hold` is RUNNING on that one slot. The
// index is then scheduled while the slot is taken, so it waits — RUNNING and
// not expanded — until `hold` ends. (Declaration order decides nothing: with
// both ready at once, Temporal may hand the index over first.)

const ENGINE = 'http://127.0.0.1:18691';
const UI_ORIGIN = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('fan-out-lines');
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');
const STAMP = Date.now().toString(36);
const LINES = `fan-out-lines-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'fan-out-lines')
	: path.join(uiDir, 'test-results', 'fan-out-lines');
const TRANSCRIPT = 'fan-out-lines-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));
const shot = (page: Page, name: string) => page.screenshot({ path: outPath(name), fullPage: true });
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

function linesOrkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${LINES}
version: "0.1.0"
description: Four JSON lines in batches of two, indexed while a sibling holds the Orker.

tasks:
  - id: list
    uses: shell
    timeout_seconds: 120
    cache:
      enabled: true
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        for n in 1 2 3 4; do printf '{"n": %s}\\n' "$n"; done > /orkstack/output/artifacts/rows.jsonl

  - id: hold
    uses: shell
    needs:
      - list
    timeout_seconds: 120
    with:
      op: exec
      command:
        - sh
        - -c
        - "echo holding the only slot; sleep 20"

  - id: go
    needs:
      - list
    await:
      timeout_seconds: 600

  - id: convert
    uses: shell
    needs:
      - list
      - go
    timeout_seconds: 120
    each:
      over_artifact: list/rows.jsonl
      format: jsonl
      batch: 2
      max_items: 10
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "batch {{ .each.index }} of {{ .each.count }}: $(wc -l < /orkstack/input/each/lines.jsonl) lines"
`;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	startTranscript(
		outPath(TRANSCRIPT),
		[
			'# A fan-out over an artifact\'s lines on the run page: the source label,',
			'# the indexing state while its index waits for the Orker, then its items.',
			'#',
			'# Written by ui/e2e/fan-out-lines.spec.ts.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${LINES}`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18691)) throw new Error('port 18691 already in use (stray process?)');
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: '127.0.0.1:18691'
	});
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...cluster,
		...storeEnv(BUCKET),
		...orkerEnv('fan-out-lines'),
		ORK_ORKER_MAX_CONCURRENT: '1'
	});
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: linesOrkflow() });
	note(`applied ${LINES}`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

/** submitHeld submits a run and resolves `go` once `hold` holds the slot. */
async function submitHeld(): Promise<string> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: LINES });
	const runId = submitted.run.id;
	note(`submitted run ${runId}`);
	await waitFor('hold holds the only slot', async () => {
		const got = await rpc<{ run?: { taskRuns?: { taskId: string; status?: string }[] } }>('RunService/GetRun', {
			runId,
			namespace: 'default'
		});
		return (got.run?.taskRuns ?? []).some((t) => t.taskId === 'hold' && t.status === 'STATUS_RUNNING');
	});
	await rpc('RunService/ResolveAwait', { namespace: 'default', runId, taskId: 'go', valuesJson: '{}' });
	note(`resolved go of ${runId} while hold ran`);
	return runId;
}

test('a lines fan-out names its source, reads indexing, then fills its batches', async ({ page }) => {
	// The first run: list's Orker indexes rows.jsonl as it commits it.
	const first = await submitHeld();
	await page.goto(`${UI_ORIGIN}/runs/${first}?ns=default`);
	await selectTask(page, 'convert');
	await expect(page.getByTestId('task-convert-each-summary')).toContainText('2 succeeded', { timeout: 120_000 });
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 60_000 });
	note('the first run: list indexed its own lines, and the card filled two batches');

	// The second run: list is a memo hit, so the fan-out indexes the lines.
	const runId = await submitHeld();
	await page.goto(`${UI_ORIGIN}/runs/${runId}?ns=default`);
	await selectTask(page, 'convert');
	const summary = page.getByTestId('task-convert-each-summary');

	// Before the index answers: the source and the batch, and "Indexing".
	await expect(summary).toHaveText(/Indexing batches of 2 lines of list\/rows\.jsonl…/, { timeout: 120_000 });
	// Progress: hold, the task holding the slot, is the one running.
	await expect(page.getByTestId('run-status')).toHaveText('Running');
	await shot(page, 'fan-out-lines-indexing.png');
	note('the card read "Indexing batches of 2 lines of list/rows.jsonl…" while hold ran');

	// After: two batches, both settled, and the run done.
	// Once it has items the card counts them by state (eachCounts), as any fan-out does.
	await expect(summary).toContainText('2 succeeded', { timeout: 120_000 });
	await expect(page.getByTestId('task-convert-item-1')).toHaveAttribute('data-status', 'SUCCEEDED');
	await expect(page.getByTestId('run-status')).toHaveText('Succeeded', { timeout: 60_000 });
	await shot(page, 'fan-out-lines-expanded.png');
	note('the card filled two batches, both succeeded');
});
