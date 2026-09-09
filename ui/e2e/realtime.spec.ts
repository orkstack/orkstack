import { expect, test } from '@playwright/test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync, readFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { catalogEnv, containerHost, ensureActivityImages, orkerEnv, s3Env } from './harness';

// Realtime proof: real engine + real Orker + built UI, driven by a browser.
// The spec submits hello-ork through the UI form and asserts the run page
// transitions PENDING -> RUNNING -> SUCCEEDED with all three tasks succeeded,
// updating in place over the WebSocket bridge (no page refresh).

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(uiDir, '..');

const ENGINE_HTTP = 'http://127.0.0.1:18080';
const ENGINE_WS = 'ws://127.0.0.1:18080';
const UI_ORIGIN = 'http://127.0.0.1:14173';

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'realtime')
	: path.join(uiDir, 'test-results', 'realtime');

const children: ChildProcess[] = [];
let stubServer: Server | undefined;
let stubPort = 0;
let runUrl = '';

function logPath(name: string): string {
	mkdirSync(PROOF_DIR, { recursive: true });
	return path.join(PROOF_DIR, name);
}

function spawnLogged(
	name: string,
	command: string,
	args: string[],
	env: NodeJS.ProcessEnv,
	cwd: string = repoRoot
): ChildProcess {
	const out = createWriteStream(logPath(name), { flags: 'w' });
	const child = spawn(command, args, {
		cwd,
		env: { ...process.env, ...env },
		stdio: ['ignore', 'pipe', 'pipe']
	});
	child.stdout?.pipe(out);
	child.stderr?.pipe(out);
	children.push(child);
	return child;
}

async function waitFor(
	label: string,
	probe: () => Promise<boolean>,
	timeoutMs = 60_000
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			if (await probe()) return;
		} catch {
			// not ready yet
		}
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await new Promise((r) => setTimeout(r, 250));
	}
}

function tcpOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
	return new Promise((resolve) => {
		const sock = net.connect({ port, host });
		sock.once('connect', () => {
			sock.destroy();
			resolve(true);
		});
		sock.once('error', () => resolve(false));
		sock.setTimeout(1500, () => {
			sock.destroy();
			resolve(false);
		});
	});
}

async function killAll(): Promise<void> {
	const survivors = children.splice(0);
	for (const child of survivors) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 8_000;
	for (const child of survivors) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
	// Backstop per AGENTS.md §11.1: never leave stray ork processes behind.
	try {
		execSync("pkill -f 'bin/orker' || true; pkill -f 'bin/ork' || true", {
			timeout: 5_000
		});
	} catch {
		// pkill exit status is informational only
	}
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });

	// Infra preconditions: Temporal + MinIO must be live (make infra-up).
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18080)) throw new Error('port 18080 already in use (stray engine?)');
	if (await tcpOpen(14173)) throw new Error('port 14173 already in use (stray UI?)');

	// Build the real binaries and the production UI.
	// The three first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// Hermetic target for the hello-ork fetch task (no external network).
	//
	// It listens on ALL interfaces, and is addressed below through the daemon's
	// host alias, because the task that calls it is a CONTAINER: `127.0.0.1`
	// inside one is that container's own loopback, not this process's.
	stubServer = createServer((req, res) => {
		res.writeHead(200, { 'content-type': 'application/json' });
		res.end(JSON.stringify({ slideshow: { title: 'orkstack realtime proof' }, path: req.url }));
	});
	await new Promise<void>((resolve) => stubServer!.listen(0, '0.0.0.0', resolve));
	const addr = stubServer.address();
	if (!addr || typeof addr === 'string') throw new Error('stub server has no port');
	stubPort = addr.port;

	// Engine: Connect API + WS bridge, with the UI origin allowed for the WS.
	//
	// It is given the SAME artifact backend as the Orker below, and that is
	// load-bearing rather than tidiness. The engine resolves the run's commit
	// root at submit and pins it onto the replay-pinned plan (§5.2); the Orker
	// then reads and writes `commit.json` there. An engine left on the default
	// local backend beside an S3 Orker pins a `file://` root that the Orker's
	// S3 commit store cannot parse, and every task fails COMMIT_READ_FAILED
	// before it runs.
	spawnLogged('engine.log', './bin/ork', [], {
		...catalogEnv,
		...s3Env,
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		ORK_HTTP_ADDR: '127.0.0.1:18080',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	await waitFor('engine connect api', async () => {
		const res = await fetch(`${ENGINE_HTTP}/orkstack.v1.OrkflowService/ListOrkflows`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: '{}'
		});
		return res.ok;
	});

	// Orker: the generic worker that runs any activity image, against MinIO.
	spawnLogged('orker.log', './bin/orker', [], {
		...s3Env,
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		...orkerEnv('realtime')
	});

	// Built UI served by adapter-node, proxying RPC to the engine.
	spawnLogged(
		'ui.log',
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14173',
			ENGINE_API_URL: ENGINE_HTTP,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	// Register hello-ork through the UI proxy (the same path the browser uses).
	//
	// Its `version:` is stamped for this run. The example pins a fixed version,
	// and an Orkflow version is IMMUTABLE — but the compiled definition now
	// embeds the index digest each `uses:` resolved to (§4.4), and that digest
	// is minted afresh every time the activity images are pushed. Re-applying
	// the unchanged file after a re-push is therefore a genuinely different
	// definition under a version that already exists, which the store correctly
	// refuses. Stamping the version says what is true: this is a new definition.
	const yaml = readFileSync(path.join(repoRoot, 'examples', 'hello.ork.yaml'), 'utf8').replace(
		/^version:.*$/m,
		`version: "0.0.${Date.now() % 100000}"`
	);
	const apply = await fetch(`${UI_ORIGIN}/orkstack.v1.OrkflowService/ApplyOrkflow`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ yaml })
	});
	if (!apply.ok) throw new Error(`apply failed: ${apply.status} ${await apply.text()}`);
	const applied = (await apply.json()) as { orkflow?: { name?: string } };
	if (applied.orkflow?.name !== 'hello-ork') {
		throw new Error(`unexpected apply response: ${JSON.stringify(applied)}`);
	}
});

test.afterAll(async () => {
	await killAll();
	stubServer?.close();
});

test('run page updates live from PENDING to SUCCEEDED over the WebSocket bridge', async ({
	page
}) => {
	// 1. Orkflow list shows the applied orkflow (screenshot for proof).
	await page.goto(`${UI_ORIGIN}/orkflows`);
	await expect(page.getByTestId('orkflow-row-hello-ork')).toBeVisible();
	await expect(page.getByTestId('orkflow-taskcount-hello-ork')).toHaveText('3');
	await page.screenshot({ path: logPath('screenshot-orkflows.png'), fullPage: true });

	// 2. Orkflow detail renders the DAG with needs edges.
	await page.goto(`${UI_ORIGIN}/orkflows/hello-ork`);
	await expect(page.getByTestId('orkflow-dag')).toBeVisible();
	// SVG edges are stroke-only paths (zero-area bbox when horizontal), so
	// assert DOM attachment rather than visibility.
	await expect(page.getByTestId('dag-edge-fetch-transform')).toBeAttached();
	await expect(page.getByTestId('dag-edge-transform-verify')).toBeAttached();
	await page.screenshot({ path: logPath('screenshot-orkflow-dag.png'), fullPage: true });

	// 3. Submit hello-ork via the UI form with the hermetic stub URL.
	//
	//    Submitting is a dialog on the orkflow's OWN page — the page step 2
	//    just rendered — rather than a standalone route that would ask again
	//    which orkflow this is. There is no orkflow picker any more for the
	//    same reason: the answer is the page the button was pressed on.
	await page.goto(`${UI_ORIGIN}/orkflows/hello-ork`);
	await page.getByTestId('orkflow-submit-run').click();
	await expect(page.getByTestId('orkflow-submit-dialog')).toBeVisible();
	await expect(page.getByTestId('run-submit-panel')).toBeVisible();
	await page.getByTestId('run-submit-input-json-mode').click();
	await page
		.getByTestId('run-submit-inputs-json')
		.fill(JSON.stringify({ url: `http://${containerHost}:${stubPort}/data.json` }, null, 2));
	await page.getByTestId('run-submit-submit').click();

	// 4. The browser lands on the run page; from here on NO navigation or
	//    refresh may occur — all updates must arrive over the WebSocket.
	await page.waitForURL(/\/runs\/.+/, { timeout: 15_000 });
	runUrl = page.url();
	await expect(page.getByTestId('run-id')).toBeVisible();
	// The marker survives only if the page never reloads.
	await page.evaluate(() => {
		(window as unknown as { __orkLiveMarker: string }).__orkLiveMarker = 'alive';
	});
	await page.screenshot({ path: logPath('screenshot-run-mid-run.png'), fullPage: true });

	// 5. The run status reaches SUCCEEDED in place; sample the pill meanwhile.
	const seenStatuses = new Set<string>();
	await expect
		.poll(
			async () => {
				const text = await page.getByTestId('run-status').innerText();
				seenStatuses.add(text.trim());
				return text.trim();
			},
			{ timeout: 90_000, intervals: [50, 100, 200, 500] }
		)
		.toBe('SUCCEEDED');
	console.log(`run-status pill sampled states: ${[...seenStatuses].join(' -> ')}`);

	// 5b. The status trail is built from the streamed run-level events, so it
	//     records every transition even when two land in the same frame. This
	//     is the PENDING -> RUNNING -> SUCCEEDED proof, straight off the wire.
	await page.getByTestId('inspect-toggle').click();
	await expect(page.getByTestId('run-status-trail')).toHaveText('PENDING -> RUNNING -> SUCCEEDED');

	// 6. All three tasks show succeeded — updated in place by the stream.
	for (const taskId of ['fetch', 'transform', 'verify']) {
		await expect(page.getByTestId(`task-${taskId}-status`)).toHaveText('SUCCEEDED');
	}
	await expect(page.getByTestId('task-summary')).toHaveText('3/3 succeeded');

	// 7. The event log holds the ordered lifecycle: proof the
	//    PENDING -> RUNNING -> SUCCEEDED transition streamed live.
	//
	//    The event list is an inspection view rather than part of the page,
	//    so it is asked for first. What it renders is the buffer the
	//    socket has been filling since the page loaded — every event below
	//    arrived long before this click, which is the property that lets the
	//    list be on demand at all.
	await expect(page.getByTestId('event-log')).toHaveCount(0);
	await page.getByTestId('events-toggle').click();
	await expect(page.getByTestId('event-inspector')).toBeVisible();
	await expect(page.getByTestId('event-entry').filter({ hasText: 'RUN_SUCCEEDED' })).toHaveCount(
		1
	);
	const logText = await page.getByTestId('event-log').innerText();
	const idxCreated = logText.indexOf('RUN_CREATED');
	const idxStarted = logText.indexOf('RUN_STARTED');
	const idxSucceeded = logText.indexOf('RUN_SUCCEEDED');
	expect(idxCreated).toBeGreaterThanOrEqual(0);
	expect(idxStarted).toBeGreaterThan(idxCreated);
	expect(idxSucceeded).toBeGreaterThan(idxStarted);
	for (const taskId of ['fetch', 'transform', 'verify']) {
		expect(logText).toContain(`[${taskId}]`);
	}

	// 8. The page never reloaded during the transitions, and never left this
	//    run. The URL gained `?events=1` above, because that is where the
	//    event view's state lives and step 7 asked for it — a
	//    same-route navigation whose loader was never re-entered, which is
	//    what the marker says. Everything else about the address is
	//    unchanged: same page, same namespace, one parameter this test set
	//    itself.
	const marker = await page.evaluate(
		() => (window as unknown as { __orkLiveMarker?: string }).__orkLiveMarker
	);
	expect(marker).toBe('alive');
	const now = new URL(page.url());
	const landed = new URL(runUrl);
	expect(now.origin + now.pathname).toBe(landed.origin + landed.pathname);
	expect(now.searchParams.get('ns')).toBe(landed.searchParams.get('ns'));
	expect(now.searchParams.get('events')).toBe('1');

	await page.screenshot({ path: logPath('screenshot-run-succeeded.png'), fullPage: true });

	// 9. The runs list reflects the finished run (fresh navigation is fine here).
	//
	//    The row is found by THIS RUN'S ID, not by `hello-ork@<version>`. The
	//    version a run pins is the orkflow's CURRENT version, which lives in
	//    the shared bucket and is moved by any promotion any other proof runs
	//    — so a hard-coded `@0.1.0` asserts something this spec does not
	//    control and did not measure: on a bucket where something had promoted
	//    hello-ork@0.1.1, the row was there, correct, and unmatched. The id is
	//    the run this test started, and nothing else can produce it.
	const runId = new URL(runUrl).pathname.split('/').pop() ?? '';
	expect(runId).not.toBe('');
	await page.goto(`${UI_ORIGIN}/runs`);
	const row = page.getByTestId(`run-row-${runId}`);
	await expect(row).toBeVisible();
	await expect(row.getByText('SUCCEEDED')).toBeVisible();
});
