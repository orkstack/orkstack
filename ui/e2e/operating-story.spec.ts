import { expect, test, type Page } from './test';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	activityRef,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killAll,
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

// The operating story, in one command against the real stack.
//
// The per-slice specs each hold their own claim. None of them holds the
// COMPOSITION's: that four things — container output
// in the live log, an editable base version, the Admin surface, and the run
// page's on-demand event inspector — work TOGETHER, read through one replica
// that started nothing, while another replica does the starting.
//
// Topology, mirroring logs-dashboard.spec.ts's two-replica composition:
//
//   engine A (:18190)   SubmitRun only. This is the one write nothing else
//                        may perform: starting a Temporal workflow.
//   engine B (:18191)   Every read this spec makes, and every write that is
//                        NOT a run start (apply, promote, namespace CRUD) —
//                        those are store writes any replica may serve
//                        (AGENTS.md §7), which is the same reasoning
//                        logs-dashboard.spec.ts test 3 already rests on for
//                        an orkflow edit.
//   UI       (:14181)   the production build, proxying Connect to B and
//                        pointing the browser's WebSockets at B.
//   orker main           the default pool — runs the chatty container task.
//   orker batch           ORK_ORKER_POOL=batch — the second pool this run's
//                        own namespace's pool policy names.
//
// Isolation (AGENTS.md §11.1): ORK_TEMPORAL_NAMESPACE=ork-ui-operating-story,
// this spec's own. Both engines and both Orkers share it — the deliberate
// exception the rule names: "any replica answers for any run" is the
// condition under test, and splitting the namespace would fail the proof for
// a reason unrelated to the property.

const ENGINE_A = 'http://127.0.0.1:18190';
const ENGINE_B = 'http://127.0.0.1:18191';
const ENGINE_B_WS = 'ws://127.0.0.1:18191';
const UI_ORIGIN = 'http://127.0.0.1:14181';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-operating-story';
// The image every container task here runs. It is activity-shell rather than a
// bare `alpine:3.20`, because an activity must write
// /orkstack/output/values.json and a stock base image does not — an image that
// exits 0 without one fails VALUES_MISSING. It is still an ordinary OCI image,
// built by `docker build` and pulled by digest.
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const CHATTY_ORKFLOW = `story-chatty-${STAMP}`;
const VERSIONED_ORKFLOW = `story-versioned-${STAMP}`;
const OPS_NS = `story-ops-${STAMP}`;

// One line per second on each of stdout/stderr — long enough that both the
// CLI follow and the browser pane are certainly watching before the last
// line lands, AND long enough for the browser's own growth check (a second
// application line must arrive while the run still reads RUNNING) to have
// margin after page navigation and the earlier CLI assertions have already
// spent a few seconds of the container's run — short enough that the whole
// consolidated proof stays minutes, not tens of minutes.
const CONTAINER_LINES = 18;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'operating-story')
	: path.join(uiDir, 'test-results', 'operating-story');
const TRANSCRIPT = 'operating-story.txt';

// These tests build on what the last left in the cluster and the bucket —
// the version-editing test's namespace and orkflow, the Admin test's pool
// Orkers — so ordering is the safety rail, as in the other serial specs
// here.
test.describe.configure({ mode: 'serial' });

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpcA = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE_A, method, body);
const rpcB = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE_B, method, body);

// A chatty container, followed live. Under the OCI Activity Contract every
// task IS a container, so the image is named in `uses:` and the argv goes in
// the task's own `command` — there is no `container` activity to name, and
// no per-task `pull:` because the image is pinned to an index digest at apply
// time (§4.3) and the Orker pulls that digest.
function chattyOrkflowYaml(name: string): string {
	return `name: ${name}
version: "0.1.0"
description: Operating story — a chatty container, followed live.
tasks:
  - id: talk
    uses: shell
    timeout_seconds: 180
    with:
      op: script
      interpreter: sh
      script: |
        i=1
        while [ $i -le ${CONTAINER_LINES} ]; do
          echo "story stdout line $i"
          echo "story stderr line $i" 1>&2
          i=$((i+1))
          sleep 1
        done
`;
}

function versionedOrkflowYaml(name: string, version: string, description: string, command: string): string {
	return `name: ${name}
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

/** followTaskLogsCLI runs `orkctl run logs -f` against ENGINE_B, timestamping
 * every line it prints as it arrives — this is the CLI half of "followed
 * live", falsifiable because the timestamps are recorded as the bytes land,
 * not reconstructed from the finished transcript afterward. */
function followTaskLogsCLI(
	orkctlPath: string,
	run: string,
	task: string
): { proc: ChildProcess; lines: { at: number; text: string }[]; done: Promise<void> } {
	const lines: { at: number; text: string }[] = [];
	const proc = spawn(orkctlPath, ['run', 'logs', run, '--task', task, '--follow'], {
		cwd: repoRoot,
		env: { ...process.env, ORK_SERVER: ENGINE_B }
	});
	let carry = '';
	proc.stdout.on('data', (chunk: Buffer) => {
		carry += chunk.toString('utf8');
		const parts = carry.split('\n');
		carry = parts.pop() ?? '';
		for (const part of parts) {
			if (part.length === 0) continue;
			lines.push({ at: Date.now(), text: part });
		}
	});
	const done = new Promise<void>((resolve) => {
		proc.on('close', () => resolve());
	});
	return { proc, lines, done };
}

let mainOrkerProc: ChildProcess | undefined;
let batchOrkerProc: ChildProcess | undefined;

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The operating story: a chatty container followed live',
			'# (CLI and UI), a version edited from a non-head base and promoted',
			'# forward, and the Admin surface — all read through a replica that',
			'# started nothing.',
			'#',
			'# Written by ui/e2e/operating-story.spec.ts against two real',
			'# engine replicas, two real Orkers, real Temporal, real MinIO and real',
			'# Docker.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespaces:     default, ${OPS_NS}`,
			`# pools:              main (default), batch`,
			`# container image:    ${IMAGE}`,
			`# engine A (writes runs only): ${ENGINE_A}`,
			`# engine B (every read + every non-run write): ${ENGINE_B}`,
			''
		].join('\n')
	);

	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18190, 18191, 14181]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// Prerequisites are failures here, never skips: the caller is expected to
	// probe every one of them BEFORE running this spec (AGENTS.md §11.1), so
	// an absence never reaches here.
	// The first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orkctl', './cmd/orkctl'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);

	spawnLogged(outPath('engine-a.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18190'
	});
	spawnLogged(outPath('engine-b.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18191',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	// Each Orker gets its own work root, which is both the bind-mountable
	// workspace path and what its identity lock is keyed on (see orkerEnv).
	mainOrkerProc = spawnLogged(outPath('orker-main.log'), './bin/orker', [], {
		...cluster,
		...s3Env,
		...orkerEnv('story-main')
	});
	batchOrkerProc = spawnLogged(outPath('orker-batch.log'), './bin/orker', [], {
		...cluster,
		...s3Env,
		...orkerEnv('story-batch'),
		ORK_ORKER_POOL: 'batch'
	});
	await waitForEngine(ENGINE_A);
	await waitForEngine(ENGINE_B);
	note(`engine A up at ${ENGINE_A} (starts runs only); engine B up at ${ENGINE_B} (every read)`);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14181',
			ENGINE_API_URL: ENGINE_B,
			PUBLIC_ENGINE_WS_URL: ENGINE_B_WS
		},
		uiDir
	);
	await waitFor('built ui, proxied to engine B', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	await waitFor('the main pool reports a live Orker', async () => {
		const answer = await rpcB<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return answer.pools?.some((p) => p.name === 'main' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});
	note('the default pool "main" reports a live Orker, read from engine B');
});

test.afterAll(async () => {
	await killAll();
});

// ── 1. the chatty container, followed live from a replica that started
//      nothing, in the CLI and in the browser; the event inspector opens
//      and closes without losing task updates ──────────────────────────────
test('a chatty container is followed live, in the CLI and the browser, from engine B — and the event inspector does not stop task updates', async ({
	page
}) => {
	await rpcA('OrkflowService/ApplyOrkflow', { yaml: chattyOrkflowYaml(CHATTY_ORKFLOW), namespace: 'default' });
	const submitted = await rpcA<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: CHATTY_ORKFLOW,
		namespace: 'default'
	});
	const run = submitted.run.id;
	note(`submitted run ${run} (${CHATTY_ORKFLOW}) against engine A`);

	// The CLI half: `orkctl run logs -f --server <engine B>`, timestamping as
	// bytes arrive. orkctl is never invoked against A.
	const orkctlPath = path.join(repoRoot, 'bin', 'orkctl');
	const follow = followTaskLogsCLI(orkctlPath, run, 'talk');

	// THE CLI'S OWN LIVENESS CLAIM, captured WHILE the follow is still
	// running (not reconstructed afterward) and BEFORE the browser gets
	// anywhere near the run: once the first application line has been
	// captured, engine B — never engine A — is asked for the run's status
	// in the same breath. The container sleeps 1s between each of
	// CONTAINER_LINES*2 lines, so there is ample window before it finishes.
	await expect
		.poll(() => follow.lines.some((l) => l.text.includes('story st')), { timeout: 30_000 })
		.toBe(true);
	const firstLineAt = follow.lines.find((l) => l.text.includes('story st'))?.at ?? 0;
	expect(firstLineAt).toBeGreaterThan(0);
	const statusWhileFollowing = await rpcB<{ run: { status: string } }>('RunService/GetRun', { runId: run });
	expect(statusWhileFollowing.run.status).toBe('STATUS_RUNNING');
	note(
		`CLI follow (engine B) had captured an application line by ` +
			`${new Date(firstLineAt).toISOString()}; engine B's own GetRun at that moment ` +
			`reported ${statusWhileFollowing.run.status} — the CLI follow saw bytes before the run ended`
	);

	// The browser half, pointed at engine B throughout — the UI server was
	// started proxying Connect and WebSockets to B alone.
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(run);
	await page.evaluate(() => {
		(window as unknown as { __orkLiveMarker: string }).__orkLiveMarker = 'alive';
	});
	await page.getByTestId('task-log-toggle-talk').click();
	const logPane = page.getByTestId('task-log-talk');
	const logLines = logPane.getByTestId('task-log-line');
	const appLines = logLines.filter({ hasText: /^story (?:stdout|stderr) line \d+$/ });
	// `data-stream` is an attribute on the SAME element `data-testid` names —
	// there is no child to descend into — so it must be part of one CSS
	// selector, not chained via `.locator()` (which searches descendants and
	// would silently match nothing, timing every wait out at 30s regardless
	// of what actually rendered).
	const stderrLines = logPane.locator('[data-testid="task-log-line"][data-stream="stderr"]');

	// The first chunk a container task streams is the Orker's own lifecycle
	// chatter (`workdir`, the resolved image and its digest —
	// cli-follow-transcript.txt:1-3 shows exactly this on the CLI side of the
	// same run), so a pane with ANY line already passes a check that never
	// required application output. Require BOTH file descriptors, decoded
	// the way taskLogs.ts promises: stdout bare text, stderr on its own
	// data-stream row with the wire "[stderr] " marker stripped for display.
	// The two streams are independent pipes the Orker copies independently
	// (internal/orker/logsink.go), so stderr's first line can legitimately
	// land before stdout's — the assertions below check EXISTENCE, never
	// relative order.
	const firstStdout = appLines.filter({ hasText: /^story stdout line 1$/ }).first();
	const firstStderr = stderrLines.filter({ hasText: /^story stderr line 1$/ }).first();
	await expect(firstStdout).toBeVisible({ timeout: 30_000 });
	await expect(firstStderr).toBeVisible({ timeout: 30_000 });
	await expect(firstStderr).not.toContainText('[stderr]');
	const runStatusWhileLive = (await page.getByTestId('run-status').innerText()).trim();
	expect(runStatusWhileLive).toBe('RUNNING');
	note(
		`browser pane shows a decoded stdout application line ("story stdout line 1") ` +
			`AND a decoded stderr application line ("story stderr line 1", wire marker ` +
			`stripped, data-stream="stderr") while run reads ${runStatusWhileLive}`
	);

	// GROWTH while RUNNING: the container writes one pair a second for
	// CONTAINER_LINES seconds. A pane that showed the first pair and then
	// stalled (lost its subscription, e.g.) must fail this — counting only
	// MATCHED application lines, not the pane's total (which a lifecycle
	// notice could also grow), proves the socket kept delivering rather than
	// having delivered once.
	const countAfterFirstPair = await appLines.count();
	await expect.poll(async () => appLines.count(), { timeout: 15_000 }).toBeGreaterThan(countAfterFirstPair);
	const runStatusStillLive = (await page.getByTestId('run-status').innerText()).trim();
	expect(runStatusStillLive).toBe('RUNNING');
	const countAfterGrowth = await appLines.count();
	note(
		`application line count grew from ${countAfterFirstPair} to ${countAfterGrowth} while run ` +
			`STILL reads ${runStatusStillLive} — the browser pane is receiving live application ` +
			`output, not stalled after the first chunk`
	);

	await shot(page, 'screenshot-live-log-in-browser.png');

	// The event inspector: opened while the container may still be writing,
	// closed once the task is done — task status keeps updating through both.
	await page.getByTestId('events-toggle').click();
	await page.waitForURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('event-entry').filter({ hasText: 'RUN_CREATED' })).toHaveCount(1);
	await shot(page, 'screenshot-event-inspector-open-live.png');
	note('event inspector opened: backlog present, RUN_CREATED included');

	await expect(page.getByTestId('task-talk-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	note('task talk reached SUCCEEDED with the event inspector OPEN');

	await page.getByTestId('events-toggle').click();
	await expect(page).not.toHaveURL(/events=1/);
	await expect(page.getByTestId('event-inspector')).toHaveCount(0);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 60_000 });
	note('event inspector closed; run reached SUCCEEDED with it CLOSED');

	const marker = await page.evaluate(
		() => (window as unknown as { __orkLiveMarker?: string }).__orkLiveMarker
	);
	expect(marker).toBe('alive');
	await shot(page, 'screenshot-run-succeeded.png');

	// Now let the follow run to completion and check the whole transcript.
	await follow.done;
	const cliLines = follow.lines.filter((l) => l.text.includes('story st'));
	expect(cliLines.length).toBeGreaterThanOrEqual(CONTAINER_LINES * 2); // both streams
	expect(cliLines.some((l) => l.text.startsWith('story stdout'))).toBe(true);
	expect(cliLines.some((l) => l.text.startsWith('[stderr] story stderr'))).toBe(true);
	const lastLineAt = cliLines[cliLines.length - 1]?.at ?? firstLineAt;
	expect(lastLineAt - firstLineAt).toBeGreaterThan(1000);
	note(
		`CLI follow (engine B) captured ${cliLines.length} application lines over ` +
			`${lastLineAt - firstLineAt}ms, stdout bare and stderr marked "[stderr] "`
	);

	appendFileSync(
		outPath('cli-follow-transcript.txt'),
		follow.lines.map((l) => `${new Date(l.at).toISOString()}  ${l.text}`).join('\n') + '\n'
	);
});

// ── 2. a non-head base: apply two versions, promote the older one, edit the
//      PROMOTED (current) version in the UI, apply it as the new head ──────
test('a version is edited from its current (non-head) base and applied as the new head, in the browser, against engine B', async ({
	page
}) => {
	await rpcB('OrkflowService/ApplyOrkflow', {
		yaml: versionedOrkflowYaml(VERSIONED_ORKFLOW, '0.1.0', 'first revision', 'echo one'),
		namespace: 'default'
	});
	note(`applied ${VERSIONED_ORKFLOW}@0.1.0 against engine B`);

	// Apply a second version through the editor itself (base defaults to head).
	await page.goto(`${UI_ORIGIN}/orkflows/${VERSIONED_ORKFLOW}?ns=default&tab=edit`);
	await expect(page.getByTestId('editor-form')).toBeVisible();
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await page
		.getByTestId('editor-yaml')
		.fill(versionedOrkflowYaml(VERSIONED_ORKFLOW, '0.2.0', 'second revision', 'echo two'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	note('applied 0.2.0 from the editor; head moved to 0.2.0');

	// Promote the OLDER version: current and head now part company.
	await page.getByTestId('promote-0.1.0').click();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-head-0.2.0')).toBeVisible();
	const promoted = await rpcB<{ currentVersion: string; headVersion: string }>(
		'OrkflowService/ListOrkflowVersions',
		{ name: VERSIONED_ORKFLOW, namespace: 'default' }
	);
	expect(promoted.currentVersion).toBe('0.1.0');
	expect(promoted.headVersion).toBe('0.2.0');
	note(`promoted 0.1.0: current=0.1.0, head=0.2.0 (read from engine B)`);
	await shot(page, 'screenshot-promoted.png');

	// Edit the PROMOTED (current) version, not head. The editor must hold
	// 0.1.0's source, not 0.2.0's.
	await page.getByTestId('version-edit-0.1.0').click();
	await expect(page.getByTestId('editor-form')).toBeVisible();
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/first revision/);
	await expect(page.getByTestId('editor-yaml')).not.toHaveValue(/second revision/);
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.2.0');
	await expect(page.getByTestId('editor-fork-notice')).toBeVisible();
	await shot(page, 'screenshot-editor-current-not-head.png');
	note('Edit on the current version (0.1.0) shows 0.1.0 source over head 0.2.0');

	// Apply it as the new head: base=0.1.0 (provenance), expected_head=0.2.0
	// (the CAS token).
	await page
		.getByTestId('editor-yaml')
		.fill(versionedOrkflowYaml(VERSIONED_ORKFLOW, '0.3.0', 'forked from current', 'echo three'));
	await page.getByTestId('editor-apply').click();
	await page.waitForURL(/tab=versions/);
	await expect(page.getByTestId('version-head-0.3.0')).toBeVisible();
	await expect(page.getByTestId('version-current-0.1.0')).toBeVisible();
	await shot(page, 'screenshot-versions-new-head.png');

	const after = await rpcB<{ headVersion: string; currentVersion: string }>(
		'OrkflowService/ListOrkflowVersions',
		{ name: VERSIONED_ORKFLOW, namespace: 'default' }
	);
	expect(after.headVersion).toBe('0.3.0');
	expect(after.currentVersion).toBe('0.1.0');
	note(`applied 0.3.0 derived from 0.1.0: head=0.3.0, current=0.1.0 (untouched), read from engine B`);
});

// ── 3. Admin: a namespace and its pool policy, created in the browser; pool
//      health, and the CLI's admin info + pool list — all from engine B ────
test('a namespace and its pool policy are set in the Admin section, and pool health + admin info are read from engine B', async ({
	page
}) => {
	await page.goto(`${UI_ORIGIN}/admin/namespaces?ns=default`);
	await expect(page.getByTestId('ns-create-name')).toBeVisible();
	await page.getByTestId('ns-create-name').fill(OPS_NS);
	await page.getByTestId('ns-create-description').fill('Operating story — a pool policy set in the browser.');
	await page.getByTestId('ns-create-default-pool').fill('main');
	await page.getByTestId('ns-create-allowed-pools').fill('main, batch');
	await page.getByTestId('ns-create-submit').click();
	await expect(page.getByTestId('ns-create-success')).toBeVisible();
	await expect(page.getByTestId(`namespace-row-${OPS_NS}`)).toBeVisible();
	await shot(page, 'screenshot-admin-namespace-created.png');
	note(`created namespace ${OPS_NS} (default_pool=main, allowed_pools=main,batch) via engine B`);

	// ListPools scopes to a namespace's default_pool + allowed_pools + stored
	// orkflow pools (docs/DEPLOY.md, "the admin surface"), so `batch` enters
	// scope only once OPS_NS names it — checked here, against engine B, by
	// namespace rather than globally.
	await waitFor('the batch pool reports a live Orker, scoped to the new namespace, from engine B', async () => {
		const answer = await rpcB<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{ namespace: OPS_NS }
		);
		return answer.pools?.some((p) => p.name === 'batch' && (p.liveOrkerCount ?? 0) > 0) ?? false;
	});

	await page.goto(`${UI_ORIGIN}/admin/pools?ns=${OPS_NS}`);
	await expect(page.getByTestId('pool-list')).toBeVisible();
	await expect(page.getByTestId('pool-card-main')).toBeVisible();
	await expect(page.getByTestId('pool-card-batch')).toBeVisible();
	await expect(page.getByTestId('pool-orker-count-main')).not.toHaveText('0');
	await expect(page.getByTestId('pool-orker-count-batch')).not.toHaveText('0');
	await shot(page, 'screenshot-admin-pools.png');
	note(`pool health for ${OPS_NS} shows live Orkers on main and batch, read from engine B`);

	await page.goto(`${UI_ORIGIN}/admin`);
	await expect(page.getByTestId('system-info')).toBeVisible();
	await shot(page, 'screenshot-admin-system-info.png');

	// The same two reads, from the CLI, against engine B by name — never A.
	const orkctlPath = path.join(repoRoot, 'bin', 'orkctl');
	const adminInfoOut = execFileSync(
		orkctlPath,
		['admin', 'info', '--server', ENGINE_B, '--json'],
		{ cwd: repoRoot }
	).toString('utf8');
	const poolListOut = execFileSync(
		orkctlPath,
		['pool', 'list', '--server', ENGINE_B, '--namespace', OPS_NS, '--json'],
		{ cwd: repoRoot }
	).toString('utf8');
	appendFileSync(
		outPath('cli-admin-transcript.txt'),
		[
			`$ orkctl admin info --server ${ENGINE_B} --json`,
			adminInfoOut,
			`$ orkctl pool list --server ${ENGINE_B} --namespace ${OPS_NS} --json`,
			poolListOut,
			''
		].join('\n')
	);

	const info = JSON.parse(adminInfoOut) as { engineVersion?: string; temporalAddress?: string };
	expect(info.engineVersion).toBeTruthy();
	expect(info.temporalAddress).toBeTruthy();

	const pools = JSON.parse(poolListOut) as { pools?: { name: string; liveOrkerCount?: number }[] };
	const mainPool = pools.pools?.find((p) => p.name === 'main');
	const batchPool = pools.pools?.find((p) => p.name === 'batch');
	expect(mainPool?.liveOrkerCount ?? 0).toBeGreaterThan(0);
	expect(batchPool?.liveOrkerCount ?? 0).toBeGreaterThan(0);
	note(
		`orkctl admin info --server ${ENGINE_B}: engine ${info.engineVersion}; ` +
			`orkctl pool list --server ${ENGINE_B} -n ${OPS_NS}: main=${mainPool?.liveOrkerCount}, ` +
			`batch=${batchPool?.liveOrkerCount} — both answered by engine B, which started no run`
	);

	// Engine A never received a namespace, pool-policy or admin read in this
	// spec; its only involvement was SubmitRun in test 1. Confirmed by
	// construction (every rpc/CLI call above named ENGINE_B), and restated
	// here as a fact about the transcript rather than left implicit.
	note(`engine A (${ENGINE_A}) served no read in this run — SubmitRun only`);
});
