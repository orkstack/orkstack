import { expect, test } from './test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { runCommand } from '../src/lib/runDetail';
import {
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
	waitForEngine
} from './harness';

// The run comparison, against a real engine, Orker, Temporal and MinIO: the
// run page's Compare action pre-selects the orkflow's last successful run, the
// comparison page is a deep link that swaps, its CLI equivalent is the command
// runCommand quotes, its input diff is byte-for-byte the RPC's answer, and a
// run of another namespace is an error — never an empty table.
const ENGINE = 'http://127.0.0.1:18099';
const ENGINE_WS = 'ws://127.0.0.1:18099';
const UI_ORIGIN = 'http://127.0.0.1:14199';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE
	? `${process.env.ORK_TEMPORAL_NAMESPACE}-run-diff`
	: 'ork-ui-run-diff';
const STAMP = Date.now().toString(36);
const NAME = `run-diff-${STAMP}`;
const OTHER_NS = `rd-other-${STAMP}`;
const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-diff')
	: path.join(uiDir, 'test-results', 'run-diff');
const out = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, 'transcript.txt'));

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

let succeeded = '';
let failed = '';

function probeWorkflow(): string {
	return `name: ${NAME}
version: "0.1.0"
description: Run diff coverage.
inputs:
  fail:
    type: boolean
    default: false
  date:
    type: string
    default: "2026-09-21"
tasks:
  - id: work
    uses: shell
    timeout_seconds: 120
    retry:
      maximum_attempts: 1
    with:
      op: script
      interpreter: sh
      script: |
        printf '{"date": "{{ .inputs.date }}"}' > /orkstack/output/values.json
        if [ "{{ .inputs.fail }}" = "true" ]; then exit 3; fi
`;
}

async function submitTerminal(input: Record<string, unknown>): Promise<{ id: string; status: string }> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: NAME, input: { data: input } });
	const id = submitted.run.id;
	let status = '';
	await waitFor(`run ${id} terminal`, async () => {
		const got = await rpc<{ run?: { status?: string } }>('RunService/GetRun', { runId: id });
		status = got.run?.status ?? '';
		return ['STATUS_SUCCEEDED', 'STATUS_FAILED', 'STATUS_CANCELED'].includes(status);
	});
	return { id, status };
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18099, 14199]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) throw new Error('UI build output missing');

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('engine.log'), './bin/ork', [], {
		...catalogEnv, ...cluster, ...s3Env, ORK_HTTP_ADDR: '127.0.0.1:18099', ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(out('orker.log'), './bin/orker', [], { ...cluster, ...s3Env, ...orkerEnv('run-diff') });
	spawnLogged(out('ui.log'), 'node', ['build'], {
		HOST: '127.0.0.1', PORT: '14199', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE_WS
	}, uiDir);
	await waitForEngine(ENGINE);
	await waitFor('UI', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { liveOrkerCount?: number }[] }>('NamespaceService/ListPools', {});
		return (answer.pools ?? []).some((pool) => (pool.liveOrkerCount ?? 0) > 0);
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: probeWorkflow() });

	const ok = await submitTerminal({ fail: false, date: '2026-09-21' });
	const bad = await submitTerminal({ fail: true, date: '2026-09-22' });
	expect(ok.status).toBe('STATUS_SUCCEEDED');
	expect(bad.status).toBe('STATUS_FAILED');
	succeeded = ok.id;
	failed = bad.id;
	// The dialog lists runs from ListRuns, which reads the cluster's index:
	// wait until both are there, so what the dialog pre-selects is the settled
	// answer rather than a race with visibility.
	await waitFor('both runs listed', async () => {
		const listed = await rpc<{ runs?: { id: string }[] }>('RunService/ListRuns', {
			filters: [{ field: 'orkflow_name', op: 'FILTER_OPERATOR_EQUALS', values: [NAME] }],
			pageSize: 20
		});
		const ids = (listed.runs ?? []).map((run) => run.id);
		return ids.includes(succeeded) && ids.includes(failed);
	});
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: OTHER_NS, description: 'run-diff: another namespace' } });
	note(`orkflow ${NAME}@0.1.0; succeeded=${succeeded} failed=${failed}; other namespace ${OTHER_NS}`);
});

test.afterAll(async () => {
	try {
		await rpc('NamespaceService/DeleteNamespace', { name: OTHER_NS });
	} catch {
		// best effort: the namespace is empty and stamped
	}
	await killStarted();
});

const compareURL = (from: string, to: string, ns = 'default') => `/runs/${from}/compare?to=${to}&ns=${ns}`;

test('P1: Compare pre-selects the last successful run and opens the comparison', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/runs/${failed}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('FAILED', { timeout: 60_000 });
	await page.getByTestId('compare-run').click();
	await expect(page.getByTestId('compare-dialog')).toBeVisible();
	await expect(page.getByTestId('compare-target')).toContainText(succeeded);
	await expect(page.getByTestId('compare-target')).toContainText('SUCCEEDED');
	await expect(page.getByTestId('compare-open')).toBeEnabled();
	// animations: 'disabled' settles the dialog's entrance before the capture,
	// so the screenshot shows the dialog rather than a frame of its fade-in.
	await page.screenshot({ path: out('compare-dialog.png'), animations: 'disabled' });
	note(`P1 compare-target pre-selected: ${await page.getByTestId('compare-target').innerText()}`);

	await page.getByTestId('compare-open').click();
	await page.waitForURL(`**${compareURL(failed, succeeded)}`);
	expect(new URL(page.url()).pathname + new URL(page.url()).search).toBe(compareURL(failed, succeeded));
	await expect(page.getByTestId('run-diff-row-work-status-from')).toHaveText('FAILED');
	await expect(page.getByTestId('run-diff-row-work-status-to')).toHaveText('SUCCEEDED');
	await expect(page.getByTestId('run-diff-input')).not.toBeEmpty();
	await expect(page.getByTestId('run-diff-summary')).toHaveText('1 of 1 tasks differ; 0 identical.');
	await page.screenshot({ path: out('run-diff.png'), fullPage: true, animations: 'disabled' });
	note(`P1 ${page.url()} summary: ${await page.getByTestId('run-diff-summary').innerText()}`);
});

test('P2: the comparison is a deep link, and Swap exchanges the sides', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}${compareURL(failed, succeeded)}`);
	await expect(page.getByTestId('run-diff-from')).toHaveText(failed);
	const rowsBefore = await page.locator('[data-testid^="run-diff-row-"][data-testid$="-from"]').allInnerTexts();
	await page.reload();
	await expect(page.getByTestId('run-diff-from')).toHaveText(failed);
	expect(await page.locator('[data-testid^="run-diff-row-"][data-testid$="-from"]').allInnerTexts()).toEqual(rowsBefore);

	await page.getByTestId('run-diff-swap').click();
	await page.waitForURL(`**${compareURL(succeeded, failed)}`);
	await expect(page.getByTestId('run-diff-from')).toHaveText(succeeded);
	await expect(page.getByTestId('run-diff-to')).toHaveText(failed);
	await expect(page.getByTestId('run-diff-row-work-status-from')).toHaveText('SUCCEEDED');
	await expect(page.getByTestId('run-diff-row-work-status-to')).toHaveText('FAILED');
	note(`P2 swapped to ${page.url()}`);
});

test('P3: the CLI equivalent is the command runCommand quotes', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}${compareURL(failed, succeeded)}`);
	await page.getByTestId('run-diff-cli-trigger').click();
	const expected = runCommand('', ['run', 'diff', failed, succeeded]);
	await expect(page.getByTestId('run-diff-cli-command')).toHaveText(expected);
	note(`P3 ${expected}`);
});

test('P4: the input diff in the DOM is byte-for-byte the RPC answer', async ({ page }) => {
	const answer = await rpc<{ inputUnifiedDiff?: string }>('RunService/DiffRuns', {
		fromRunId: failed,
		toRunId: succeeded,
		namespace: 'default'
	});
	const diff = answer.inputUnifiedDiff ?? '';
	expect(diff).toContain('"date": "2026-09-22"');
	await page.goto(`${UI_ORIGIN}${compareURL(failed, succeeded)}`);
	const rendered = await page.getByTestId('run-diff-input').textContent();
	// DiffView documents that a trailing newline is the only thing it may drop.
	expect((rendered ?? '').replace(/\n$/, '')).toBe(diff.replace(/\n$/, ''));
	note(`P4 run-diff-input textContent === DiffRuns.inputUnifiedDiff (${diff.length} bytes):\n${diff}`);
});

test('P5: a run of another namespace is an error, not an empty table', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}${compareURL(failed, succeeded, OTHER_NS)}`);
	const error = page.getByTestId('run-diff-error');
	await expect(error).toBeVisible();
	await expect(error).toContainText(failed);
	await expect(error).toContainText(OTHER_NS);
	await expect(page.getByTestId('run-diff-table')).toHaveCount(0);
	await expect(page.getByTestId('run-diff-empty')).toHaveCount(0);
	note(`P5 ${await error.innerText()}`);
});
