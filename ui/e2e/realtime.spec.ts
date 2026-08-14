import { expect, test } from '@playwright/test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { createWriteStream, existsSync, mkdirSync, readFileSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Realtime proof: real engine + real Orker + built UI, driven by a browser.
// The spec submits hello-ork through the UI form and asserts the run page
// transitions PENDING -> RUNNING -> SUCCEEDED with all three tasks succeeded,
// updating in place over the WebSocket bridge (no page refresh).

const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repoRoot = path.resolve(uiDir, '..');

const ENGINE_HTTP = 'http://127.0.0.1:18080';
const ENGINE_WS = 'ws://127.0.0.1:18080';
const UI_ORIGIN = 'http://127.0.0.1:14173';

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof');

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
	// Backstop per AGENTS.md §6: never leave stray ork processes behind.
	try {
		execSync("pkill -f 'bin/orker' || true; pkill -f 'bin/orkengine' || true", {
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
	execFileSync('go', ['build', '-o', 'bin/orkengine', './cmd/orkengine'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	// Hermetic target for the hello-ork fetch task (no external network).
	stubServer = createServer((req, res) => {
		res.writeHead(200, { 'content-type': 'application/json' });
		res.end(JSON.stringify({ slideshow: { title: 'orkstack realtime proof' }, path: req.url }));
	});
	await new Promise<void>((resolve) => stubServer!.listen(0, '127.0.0.1', resolve));
	const addr = stubServer.address();
	if (!addr || typeof addr === 'string') throw new Error('stub server has no port');
	stubPort = addr.port;

	// Engine: Connect API + WS bridge, with the UI origin allowed for the WS.
	spawnLogged('engine.log', './bin/orkengine', [], {
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		ORK_HTTP_ADDR: '127.0.0.1:18080',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	await waitFor('engine connect api', async () => {
		const res = await fetch(`${ENGINE_HTTP}/orkstack.v1.WorkflowService/ListWorkflows`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: '{}'
		});
		return res.ok;
	});

	// Orker: generic worker with http/shell/file plugins against MinIO.
	spawnLogged('orker.log', './bin/orker', [], {
		ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233',
		ORK_ARTIFACT_BACKEND: 's3',
		ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
		ORK_S3_REGION: 'us-east-1',
		ORK_S3_BUCKET: 'orkstack-artifacts',
		ORK_S3_ACCESS_KEY: 'orkstack',
		ORK_S3_SECRET_KEY: 'orkstack-secret',
		ORK_S3_PATH_STYLE: 'true'
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
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/workflows`)).ok);

	// Register hello-ork through the UI proxy (the same path the browser uses).
	const yaml = readFileSync(path.join(repoRoot, 'examples', 'hello.ork.yaml'), 'utf8');
	const apply = await fetch(`${UI_ORIGIN}/orkstack.v1.WorkflowService/Apply`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ yaml })
	});
	if (!apply.ok) throw new Error(`apply failed: ${apply.status} ${await apply.text()}`);
	const applied = (await apply.json()) as { workflow?: { name?: string } };
	if (applied.workflow?.name !== 'hello-ork') {
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
	// 1. Workflow list shows the applied workflow (screenshot for proof).
	await page.goto(`${UI_ORIGIN}/workflows`);
	await expect(page.getByTestId('workflow-row-hello-ork')).toBeVisible();
	await expect(page.getByTestId('workflow-taskcount-hello-ork')).toHaveText('3');
	await page.screenshot({ path: logPath('screenshot-workflows.png'), fullPage: true });

	// 2. Workflow detail renders the DAG with needs edges.
	await page.goto(`${UI_ORIGIN}/workflows/hello-ork`);
	await expect(page.getByTestId('workflow-dag')).toBeVisible();
	// SVG edges are stroke-only paths (zero-area bbox when horizontal), so
	// assert DOM attachment rather than visibility.
	await expect(page.getByTestId('dag-edge-fetch-transform')).toBeAttached();
	await expect(page.getByTestId('dag-edge-transform-save')).toBeAttached();
	await page.screenshot({ path: logPath('screenshot-workflow-dag.png'), fullPage: true });

	// 3. Submit hello-ork via the UI form with the hermetic stub URL.
	await page.goto(`${UI_ORIGIN}/submit?workflow=hello-ork`);
	await expect(page.getByTestId('submit-form')).toBeVisible();
	await page.getByTestId('workflow-select').selectOption('hello-ork');
	await page
		.getByTestId('inputs-json')
		.fill(JSON.stringify({ url: `http://127.0.0.1:${stubPort}/data.json` }, null, 2));
	await page.getByTestId('submit-run').click();

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
	await expect(page.getByTestId('run-status-trail')).toHaveText('PENDING -> RUNNING -> SUCCEEDED');

	// 6. All three tasks show succeeded — updated in place by the stream.
	for (const taskId of ['fetch', 'transform', 'save']) {
		await expect(page.getByTestId(`task-${taskId}-status`)).toHaveText('SUCCEEDED');
	}
	await expect(page.getByTestId('task-summary')).toHaveText('3/3 succeeded');

	// 7. The event log holds the ordered lifecycle: proof the
	//    PENDING -> RUNNING -> SUCCEEDED transition streamed live.
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
	for (const taskId of ['fetch', 'transform', 'save']) {
		expect(logText).toContain(`[${taskId}]`);
	}

	// 8. The page never reloaded during the transitions.
	const marker = await page.evaluate(
		() => (window as unknown as { __orkLiveMarker?: string }).__orkLiveMarker
	);
	expect(marker).toBe('alive');
	expect(page.url()).toBe(runUrl);

	await page.screenshot({ path: logPath('screenshot-run-succeeded.png'), fullPage: true });

	// 9. The runs list reflects the finished run (fresh navigation is fine here).
	await page.goto(`${UI_ORIGIN}/runs`);
	const row = page.locator('[data-testid^="run-row-"]', { hasText: 'hello-ork@0.1.0' }).first();
	await expect(row).toBeVisible();
	await expect(row.getByText('SUCCEEDED')).toBeVisible();
});
