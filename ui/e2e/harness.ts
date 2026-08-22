import { execSync, spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, createWriteStream, mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// The bits every live e2e in this directory needs: start a process and keep
// its log, probe a port, wait for a condition, call one engine by name, and
// leave nothing running afterwards.
//
// It is a MODULE and not a copy in each spec on purpose. M7.5 and M7.6 each
// added a `requireMinIO` to their own build-tagged file and the merged package
// had it twice, invisible to every default check (AGENTS.md §7). A Playwright
// spec is not build-tagged, but the lesson is the same one: a helper that
// exists twice drifts twice.
//
// Playwright collects `*.spec.ts`, so this file is imported and never run as a
// test.

export const uiDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const repoRoot = path.resolve(uiDir, '..');

/** The processes started through spawnLogged, so killAll can end all of them. */
const children: ChildProcess[] = [];

/** proofOut returns a writer for one proof directory. */
export function proofOut(dir: string) {
	return (name: string): string => {
		mkdirSync(dir, { recursive: true });
		return path.join(dir, name);
	};
}

/** note appends one stamped line to a transcript. */
export function noteTo(file: string): (line: string) => void {
	return (line: string) => appendFileSync(file, `${new Date().toISOString()}  ${line}\n`);
}

export function spawnLogged(
	logFile: string,
	command: string,
	args: string[],
	env: NodeJS.ProcessEnv,
	cwd: string = repoRoot
): ChildProcess {
	const out = createWriteStream(logFile, { flags: 'w' });
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

export async function waitFor(
	label: string,
	probe: () => Promise<boolean>,
	timeoutMs = 120_000
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

export function tcpOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
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

export async function killAll(): Promise<void> {
	const survivors = children.splice(0);
	for (const child of survivors) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGTERM');
	}
	const deadline = Date.now() + 10_000;
	for (const child of survivors) {
		while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
	// AGENTS.md §6: never leave a stray engine or Orker behind, and anchor both
	// patterns — unanchored, `bin/ork` also matches `bin/orkctl`.
	try {
		execSync("pkill -f '/bin/orker$' || true; pkill -f '/bin/ork$' || true", { timeout: 5_000 });
	} catch {
		// pkill exit status is informational only
	}
}

/** rpc calls one named engine, so a test can always say WHICH replica. */
export async function rpc<T>(base: string, method: string, body: unknown): Promise<T> {
	const res = await fetch(`${base}/orkstack.v1.${method}`, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
	const text = await res.text();
	if (!res.ok) throw new Error(`${method} on ${base} failed: ${res.status} ${text}`);
	return JSON.parse(text) as T;
}

/** waitForEngine returns once an engine answers a read. */
export function waitForEngine(base: string): Promise<void> {
	return waitFor(`connect api at ${base}`, async () => {
		const res = await fetch(`${base}/orkstack.v1.OrkflowService/ListOrkflows`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: '{}'
		});
		return res.ok;
	});
}

/** The store and cluster environments both binaries are started with. */
export const s3Env = {
	ORK_ARTIFACT_BACKEND: 's3',
	ORK_S3_ENDPOINT: 'http://127.0.0.1:9000',
	ORK_S3_REGION: 'us-east-1',
	ORK_S3_BUCKET: 'orkstack-artifacts',
	ORK_S3_ACCESS_KEY: 'orkstack',
	ORK_S3_SECRET_KEY: 'orkstack-secret',
	ORK_S3_PATH_STYLE: 'true'
};

export function clusterEnv(namespace: string) {
	return { ORK_TEMPORAL_ADDRESS: '127.0.0.1:7233', ORK_TEMPORAL_NAMESPACE: namespace };
}
