import { expect, test, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
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
	rpc,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// M11.4's before and after, on ONE run, in ONE browser session.
//
// Two builds of the same application answer the same engine about the same
// finished run: the tree as it was before this milestone
// (`ORK_BEFORE_UI_DIR`, built by proof/M11.4/reproduce.sh from the merge base)
// and the tree as it is now. That is what makes the pair of screenshots a
// comparison rather than two pictures — same data, same engine, same browser,
// one difference.
//
// It is also a test, and it fails for the right reason on either side: the
// BEFORE build must render the event log unconditionally, show the wire's
// `[stderr] ` marker as text, and offer no attempt control; the AFTER build
// must do none of those things.

const ENGINE = 'http://127.0.0.1:18086';
const ENGINE_WS = 'ws://127.0.0.1:18086';
const AFTER_ORIGIN = 'http://127.0.0.1:14178';
const BEFORE_ORIGIN = 'http://127.0.0.1:14179';

const TEMPORAL_NAMESPACE =
	process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-m11-4-run-page-logs-events';
// The image every container task here runs. It is activity-shell rather than a
// bare `alpine:3.20`, because an activity must write
// /orkstack/output/values.json and a stock base image does not — an image that
// exits 0 without one fails VALUES_MISSING. It is still an ordinary OCI image,
// built by `docker build` and pulled by digest.
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');
const BEFORE_UI_DIR = process.env.ORK_BEFORE_UI_DIR ?? '';

const STAMP = Date.now().toString(36);
const ORKFLOW = `m11-4-shots-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof-m11-4');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('before-after.txt'));

let runId = '';

const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });

// A container that writes to both file descriptors and stops. The run is
// FINISHED before either browser opens it: this spec compares two renderings,
// and a live stream would make them two different sets of bytes.
//
// Under the OCI Activity Contract this is an ORDINARY task: every task is a
// container, so the image goes in `uses:` and there is no `container` activity
// to name. `command` is argv, so the two-stream program is handed to an
// explicit `sh -c`. The old `capture_stdout_as`/`capture_stderr_as` keys are
// gone and nothing replaces them, because the Orker now captures both streams
// for every task as the `stdout.log`/`stderr.log` system artifacts (§3.6) —
// which is what this spec was asking for by hand.
const orkflow = () => `name: ${ORKFLOW}
version: "0.1.0"
description: M11.4 before/after — a container that writes to both streams.

tasks:
  - id: talk
    uses: shell
    timeout_seconds: 300
    with:
      op: script
      interpreter: sh
      script: |
        echo "building the thing"
        echo "warning: this line came from stderr" 1>&2
        echo "built the thing"
        echo "error: and so did this one" 1>&2
`;

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		out('before-after.txt'),
		[
			'# M11.4 — the same run, the same engine, two builds of the UI.',
			'#',
			'# Written by ui/e2e/run-page-before-after.spec.ts.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# before build:       ${BEFORE_UI_DIR}`,
			`# after build:        ${uiDir}/build`,
			''
		].join('\n')
	);

	if (!BEFORE_UI_DIR || !existsSync(path.join(BEFORE_UI_DIR, 'build', 'index.js'))) {
		// Not a skip: without the other half there is no comparison to make,
		// and a proof that quietly published only the "after" picture would be
		// claiming something it never measured. reproduce.sh builds this.
		throw new Error(
			'ORK_BEFORE_UI_DIR must name a built pre-M11.4 UI (see proof/M11.4/reproduce.sh)'
		);
	}
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18086, 14178, 14179]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// The three first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(out('shots-engine.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18086',
		ORK_WS_ALLOWED_ORIGINS: `${AFTER_ORIGIN},${BEFORE_ORIGIN}`
	});
	spawnLogged(out('shots-orker.log'), './bin/orker', [], {
		...cluster,
		...s3Env,
		...orkerEnv('m11-4-shots')
	});
	await waitForEngine(ENGINE);

	for (const [name, dir, port] of [
		['shots-ui-after.log', uiDir, '14178'],
		['shots-ui-before.log', BEFORE_UI_DIR, '14179']
	] as const) {
		spawnLogged(
			out(name),
			'node',
			['build'],
			{
				HOST: '127.0.0.1',
				PORT: port,
				ENGINE_API_URL: ENGINE,
				PUBLIC_ENGINE_WS_URL: ENGINE_WS
			},
			dir
		);
	}
	for (const origin of [AFTER_ORIGIN, BEFORE_ORIGIN]) {
		await waitFor(`ui at ${origin}`, async () => (await fetch(`${origin}/orkflows`)).ok);
	}

	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			ENGINE,
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc(ENGINE, 'OrkflowService/ApplyOrkflow', { yaml: orkflow() });
	const submitted = await rpc<{ run: { id: string } }>(ENGINE, 'RunService/SubmitRun', {
		orkflowName: ORKFLOW
	});
	runId = submitted.run.id;
	await waitFor(
		'the run finishes',
		async () => {
			const answer = await rpc<{ run?: { status?: string } }>(ENGINE, 'RunService/GetRun', {
				runId
			});
			return answer.run?.status === 'STATUS_SUCCEEDED';
		},
		180_000
	);
	note(`run ${runId} (${ORKFLOW}) finished; both UIs will be asked about it`);
});

test.afterAll(async () => {
	await killAll();
});

test('the page before this milestone: an event log on load, a marker in the pane', async ({
	page
}) => {
	await page.goto(`${BEFORE_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(runId);

	// The event log occupies the page the moment it loads, and there is no
	// control to put it away.
	await expect(page.getByTestId('event-log')).toBeVisible();
	await expect(page.getByTestId('event-entry').first()).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId('events-toggle')).toHaveCount(0);
	await shot(page, 'screenshot-before-run-page.png');

	await page.getByTestId('task-log-toggle-talk').click();
	const lines = page.getByTestId('task-log-talk').getByTestId('task-log-line');
	await expect(lines.first()).toBeVisible({ timeout: 60_000 });
	// The run is FINISHED, so its whole log replays and the stream then ends.
	// Wait for that end before reading the pane: a first visible line is not
	// the last one, and every assertion below is about the SET of lines.
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('ENDED', { timeout: 60_000 });
	const texts = await lines.allInnerTexts();
	// The wire, rendered as text: a reader sees the marker and both streams in
	// one colour.
	expect(texts.some((t) => t.includes('[stderr] '))).toBe(true);
	const streams = await lines.evaluateAll((nodes) =>
		nodes.map((n) => n.getAttribute('data-stream'))
	);
	expect(streams.every((s) => s === null)).toBe(true);
	// And no attempt control exists at all.
	await expect(page.getByTestId('task-log-attempt-select-talk')).toHaveCount(0);
	await shot(page, 'screenshot-before-log-pane.png');
	note(
		`before: the event log is on the page at load, the pane renders ${
			texts.filter((t) => t.includes('[stderr] ')).length
		} lines with a visible "[stderr] " marker, and no attempt control exists`
	);
});

test('the page after it: tasks first, the marker decoded, the events one click away', async ({
	page
}) => {
	await page.goto(`${AFTER_ORIGIN}/runs/${runId}?ns=default`);
	await expect(page.getByTestId('run-id')).toHaveText(runId);

	// No event list in the DOM, and a control that opens one.
	await expect(page.getByTestId('event-log')).toHaveCount(0);
	await expect(page.getByTestId('event-entry')).toHaveCount(0);
	await expect(page.getByTestId('events-toggle')).toBeVisible();
	await shot(page, 'screenshot-after-run-page.png');

	await page.getByTestId('task-log-toggle-talk').click();
	const lines = page.getByTestId('task-log-talk').getByTestId('task-log-line');
	await expect(lines.first()).toBeVisible({ timeout: 60_000 });
	await expect(page.getByTestId('task-log-state-talk')).toHaveText('ENDED', { timeout: 60_000 });
	const rows = await lines.evaluateAll((nodes) =>
		nodes.map((n) => ({
			stream: n.getAttribute('data-stream'),
			text: (n.textContent ?? '').trim(),
			colour: getComputedStyle(n).color
		}))
	);
	// The same two lines, decoded: no marker on screen, stderr in its own
	// colour, and the legend that says so.
	expect(rows.some((r) => r.text.includes('[stderr] '))).toBe(false);
	const stderr = rows.filter((r) => r.stream === 'stderr');
	const stdout = rows.filter((r) => r.stream === 'stdout');
	expect(stderr.map((r) => r.text)).toEqual([
		'warning: this line came from stderr',
		'error: and so did this one'
	]);
	expect(stdout.some((r) => r.text === 'building the thing')).toBe(true);
	expect(stderr[0].colour).not.toBe(stdout[0].colour);
	await expect(page.getByTestId('task-log-stderr-legend-talk')).toBeVisible();
	await shot(page, 'screenshot-after-log-pane.png');
	note(
		`after: no event list on load, ${stderr.length} stderr lines rendered ${stderr[0].colour} ` +
			`against ${stdout[0].colour} for stdout, marker stripped, legend shown`
	);

	// And the events are one click away, over the same socket the tasks use.
	await page.getByTestId('events-toggle').click();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('event-entry').first()).toBeVisible({ timeout: 60_000 });
	await shot(page, 'screenshot-after-events-open.png');
	note('after: ?events=1 opens the redesigned inspector on the same page');
});
