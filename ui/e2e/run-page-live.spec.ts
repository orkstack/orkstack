import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { appendFileSync, mkdirSync } from 'node:fs';
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
	repoRoot,
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
const BUCKET = ownBucket('run-page-live');

// Live attempt introspection on the run detail page, in a browser, against
// the real stack: what is this running task doing right now?
//
//   P1  before any Orker polls, the card's live line says it is waiting for
//       one;
//   P2  once an Orker runs it, the line reads the phase, the bytes logged and
//       the heartbeat's age, the bytes grow, `Running for` grows, and the
//       Orker the heartbeat names is shown;
//   P3  the card's CLI equivalent is `orkctl run inspect <run> --task talk`,
//       and the CLI's own output for the same run is recorded beside the
//       screenshots;
//   P4  after Cancel the line reads `Cancelling` before the run turns
//       CANCELED, and once it has no live line is left;
//   P5  the live poll pauses while the page is hidden and resumes when it is
//       shown again.
//
// Topology: one engine, the production UI build, and the Orker started AFTER
// the first submission, so the wait is real. Ports and a Temporal namespace of
// this spec's own (AGENTS.md §11.1).

const ENGINE = 'http://127.0.0.1:18311';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI_ORIGIN = ENGINE;

const TEMPORAL_NAMESPACE = temporalNamespace('run-live');
const STAMP = Date.now().toString(36);
const ORKFLOW = `run-page-live-${STAMP}`;
const ORKER_ID = 'orker-e2e-live';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-page-live')
	: path.join(uiDir, 'test-results', 'run-page-live');
const TRANSCRIPT = 'run-page-live-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

let orkctl = '';
let orkerStarted = false;

function startOrker() {
	if (orkerStarted) return;
	orkerStarted = true;
	spawnLogged(outPath('orker.log'), bin('orker'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...orkerEnv('run-page-live'),
		ORK_ORKER_ID: ORKER_ID,
		ORK_ORKER_HEARTBEAT_INTERVAL: '1s'
	});
	note(`orker started as ${ORKER_ID} with a 1s heartbeat`);
}

// A task that writes a line every 200 ms for 60 s. It keeps writing through
// SIGTERM, so a cancellation is visible for the whole grace window rather than
// for the instant a well-behaved workload takes to exit.
function orkflow(): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${ORKFLOW}
version: "0.1.0"
description: Live attempt introspection — a task that keeps talking.

tasks:
  - id: talk
    uses: shell
    timeout_seconds: 120
    cancellation_grace_period_seconds: 15
    with:
      op: exec
      command: ["sh", "-c", "trap 'echo got-term' TERM; i=0; while [ $i -lt 300 ]; do echo \\"live line $i\\"; i=$((i+1)); sleep 0.2; done"]
`;
}

/** runInspectCLI records what `orkctl run inspect` prints for the same run. */
function runInspectCLI(runId: string): string {
	const args = ['--server', ENGINE, '--namespace', 'default', 'run', 'inspect', runId, '--task', 'talk'];
	const out = execFileSync(orkctl, args, { cwd: repoRoot, encoding: 'utf8' });
	note(`$ orkctl ${args.join(' ')}\n${out}`);
	return out;
}

/** bytesOf reads the byte figure out of a live line, in bytes. */
function bytesOf(text: string): number {
	const match = /· ([0-9.]+) (B|KiB|MiB|GiB) logged/.exec(text);
	if (!match) throw new Error(`no byte figure in: ${text}`);
	const scale = { B: 1, KiB: 1024, MiB: 1024 ** 2, GiB: 1024 ** 3 }[match[2] as 'B' | 'KiB' | 'MiB' | 'GiB'];
	return Number(match[1]) * scale;
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(PROOF_DIR, { recursive: true });
	appendFileSync(
		outPath(TRANSCRIPT),
		[
			'# Live attempt introspection on the run detail page: waiting, the',
			'# live phase/bytes/heartbeat line, cancelling, and the',
			'# `orkctl run inspect` output for the same run.',
			'#',
			'# Written by ui/e2e/run-page-live.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflow:            ${ORKFLOW} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	// Prerequisites are failures, never skips (AGENTS.md §11.1).
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18311)) throw new Error('port 18311 already in use (stray process?)');

	ensureActivityImages();
	orkctl = bin('orkctl');

	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18311',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/_app/version.json`)).ok);

	await rpc('OrkflowService/ApplyOrkflow', { yaml: orkflow() });
	note(`applied ${ORKFLOW}; no Orker is running yet`);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a running task shows what it is doing now: waiting, phase and bytes, cancelling', async ({ page }) => {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', {
		orkflowName: ORKFLOW,
		allowEmptyPool: true
	});
	const run = submitted.run.id;
	note(`submitted run ${run} with no Orker polling`);
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);

	// P1 — nobody polls the pool: the attempt is waiting for an Orker.
	const live = page.getByTestId('task-talk-live');
	await expect(live).toHaveAttribute('data-live', 'waiting', { timeout: 60_000 });
	await expect(live).toContainText('Waiting for an orker');
	note(`P1 task-talk-live data-live=waiting text="${await live.innerText()}"`);
	await expect(page.getByTestId('task-talk-running-for')).toHaveCount(0);
	await shot(page, 'live-waiting.png');

	// P2 — the Orker picks it up.
	startOrker();
	await expect(live).toHaveAttribute('data-live', 'started', { timeout: 120_000 });
	await expect(live).toContainText('Phase run', { timeout: 30_000 });
	await expect(live).toContainText('logged');
	await expect(live).toContainText('heartbeat');
	await expect(live).toHaveAttribute('data-stale', 'false');
	const firstText = await live.innerText();
	const firstAt = Date.now();
	const firstBytes = bytesOf(firstText);
	await expect
		.poll(async () => bytesOf(await live.innerText()) > firstBytes && Date.now() - firstAt >= 2_000, {
			timeout: 20_000
		})
		.toBe(true);
	const secondText = await live.innerText();
	note(`P2 read 1: "${firstText}"`);
	note(`P2 read 2 (${Date.now() - firstAt} ms later): "${secondText}"`);
	expect(bytesOf(secondText)).toBeGreaterThan(firstBytes);

	const runningFor = page.getByTestId('task-talk-running-for');
	await expect(runningFor).toHaveText(/^Running for /);
	const runningFirst = await runningFor.innerText();
	await expect.poll(() => runningFor.innerText(), { timeout: 10_000 }).not.toBe(runningFirst);
	note(`P2 ${runningFirst} -> ${await runningFor.innerText()}`);
	await expect(page.getByTestId('task-talk-live-orker')).toHaveText(`Orker ${ORKER_ID}`);
	note(`P2 task-talk-live-orker: "${await page.getByTestId('task-talk-live-orker').innerText()}"`);
	await shot(page, 'live-started.png');

	// P3 — the card's CLI equivalent, and what the CLI prints for the same run.
	await page.getByTestId('run-task-talk-inspect-cli-trigger').click();
	await expect(page.getByTestId('run-task-talk-inspect-cli-command')).toHaveText(
		`orkctl --namespace default run inspect ${run} --task talk`
	);
	note(`P3 card command: ${await page.getByTestId('run-task-talk-inspect-cli-command').innerText()}`);
	await page.getByTestId('run-task-talk-inspect-cli-trigger').click();
	const cliOut = runInspectCLI(run);
	expect(cliOut).toContain('talk');
	expect(cliOut).toContain('STARTED');

	// P4 — cancel through the existing dialog; the line says Cancelling while
	// the Orker's SIGTERM grace runs, and disappears once the run is terminal.
	await page.getByTestId('cancel-run').click();
	await expect(page.getByTestId('cancel-run-confirm')).toBeVisible();
	await page.getByTestId('cancel-run-accept').click();
	await expect(live).toHaveAttribute('data-live', 'cancelling', { timeout: 30_000 });
	await expect(live).toContainText('Cancelling');
	note(`P4 task-talk-live before CANCELED: "${await live.innerText()}" (run-status ${await page.getByTestId('run-status').innerText()})`);
	await shot(page, 'live-cancelling.png');
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED', { timeout: 90_000 });
	await expect(page.getByTestId('task-talk-live')).toHaveCount(0);
	note('P4 run CANCELED; no task-talk-live element remains');
});

test('the live poll pauses while the page is hidden and resumes when shown', async ({ page }) => {
	startOrker();
	// A controllable document visibility: no such stub exists in the harness,
	// so it lives here.
	await page.addInitScript(() => {
		let hidden = false;
		Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden });
		Object.defineProperty(document, 'visibilityState', {
			configurable: true,
			get: () => (hidden ? 'hidden' : 'visible')
		});
		(window as unknown as { __setHidden: (next: boolean) => void }).__setHidden = (next: boolean) => {
			hidden = next;
			document.dispatchEvent(new Event('visibilitychange'));
		};
	});
	let inspects = 0;
	page.on('request', (request) => {
		if (request.url().includes('RunService/InspectRun')) inspects += 1;
	});

	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW });
	const run = submitted.run.id;
	note(`P5 submitted run ${run}`);
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	await expect(page.getByTestId('task-talk-live')).toBeVisible({ timeout: 120_000 });
	await expect.poll(() => inspects, { timeout: 15_000 }).toBeGreaterThan(0);

	await page.evaluate(() => (window as unknown as { __setHidden: (next: boolean) => void }).__setHidden(true));
	const hiddenAt = inspects;
	// The measurement itself, not synchronization: twelve seconds is more than
	// two poll intervals, and not one InspectRun may leave the page in them.
	await page.waitForTimeout(12_000);
	expect(inspects).toBe(hiddenAt);
	note(`P5 hidden for 12s: ${inspects - hiddenAt} InspectRun requests (total ${inspects})`);

	const shownAt = Date.now();
	await page.evaluate(() => (window as unknown as { __setHidden: (next: boolean) => void }).__setHidden(false));
	await expect.poll(() => inspects, { timeout: 6_000 }).toBeGreaterThan(hiddenAt);
	note(`P5 shown again: an InspectRun request within ${Date.now() - shownAt} ms`);

	await rpc('RunService/CancelRun', { runId: run });
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED', { timeout: 90_000 });
});

// F-ui-ux-7: a run whose event socket can never be served stops saying
// CONNECTING. After its bounded reconnects it says live updates stopped, with
// the reason, and Retry resumes the stream from the last event it applied.
test('a dead event socket ends in a stopped banner, and Retry resumes live updates', async ({ page }) => {
	startOrker();
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: ORKFLOW });
	const run = submitted.run.id;
	let blocked = true;
	const opened: string[] = [];
	await page.routeWebSocket(
		(url) => url.pathname === `/ws/runs/${run}`,
		(ws) => {
			opened.push(ws.url());
			if (blocked) {
				ws.close({ code: 1011, reason: 'blocked by the e2e' });
				return;
			}
			ws.connectToServer();
		}
	);
	await page.goto(`${UI_ORIGIN}/runs/${run}?ns=default`);
	// Six reconnects with a doubling backoff: about half a minute.
	await expect(page.getByTestId('live-stopped')).toBeVisible({ timeout: 90_000 });
	await expect(page.getByTestId('live-stopped')).toContainText('Live updates stopped:');
	await expect(page.getByTestId('live-badge')).toHaveText('STOPPED');
	expect(opened.length).toBe(7);
	note(`P6 stopped after ${opened.length} sockets: ${await page.getByTestId('live-stopped').innerText()}`);
	await shot(page, 'live-stopped.png');

	blocked = false;
	await page.getByTestId('live-retry').click();
	await expect(page.getByTestId('live-badge')).toHaveText('LIVE', { timeout: 30_000 });
	await expect(page.getByTestId('live-stopped')).toHaveCount(0);
	expect(opened.length).toBe(8);
	await shot(page, 'live-resumed.png');

	await rpc('RunService/CancelRun', { runId: run });
	await expect(page.getByTestId('run-status')).toHaveText('CANCELED', { timeout: 90_000 });
	note(`P6 Retry resumed from ${opened[opened.length - 1]}; the CANCELED transition arrived on the resumed stream`);
});
