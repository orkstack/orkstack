import { expect, type Page } from '@playwright/test';
import { execFileSync, execSync, spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, createWriteStream, mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// The bits every live e2e in this directory needs: start a process and keep
// its log, probe a port, wait for a condition, call one engine by name, and
// leave nothing running afterwards.
//
// It is a MODULE and not a copy in each spec on purpose: a helper that exists
// twice drifts twice.
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

// openNamespaceEdit opens Admin → Namespaces' edit form for one namespace and
// returns once THAT namespace's form is on the page.
//
// One click is not always enough, and the page is the reason: it lists every
// namespace the deployment has — on the shared development store, well over a
// thousand rows on one very tall page — and a click on one row's Edit that
// lands anywhere but that row's handler (a lost click, or the row beside it
// after the table moved) leaves this namespace's form closed. Proof
// phase-3-merged failed exactly so: the click on `namespace-edit-<ns>`
// succeeded and `ns-edit-allowed-secrets-<ns>` never appeared. Why that click
// missed is NOT established: that run kept no trace (playwright.config.ts now
// keeps a proof's failures), it did not reproduce in repeated runs, and it was
// not a click before hydration — the Edit handler is attached by the time
// `goto` returns, measured at 1x, 4x and 8x CPU throttling. So the click is
// retried until this namespace's form is visible — never once it is, since
// opening it again would reset what the form captured — and every assertion
// about the form itself is untouched.
export async function openNamespaceEdit(page: Page, name: string): Promise<void> {
	const form = page.getByTestId(`namespace-edit-form-${name}`);
	await expect(async () => {
		if (!(await form.isVisible())) await page.getByTestId(`namespace-edit-${name}`).click();
		await expect(form).toBeVisible({ timeout: 2_000 });
	}).toPass({ timeout: 30_000 });
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

/**
 * killStarted ends exactly the processes this spec started through
 * spawnLogged — by handle, never by name — and waits for them to exit. It is
 * killAll without the name-pattern sweep, for a spec that shares its host
 * with other stacks and so may only ever signal its own children.
 */
export async function killStarted(): Promise<void> {
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
}

export async function killAll(): Promise<void> {
	await killStarted();
	// AGENTS.md §11.1: never leave a stray engine or Orker behind, and anchor both
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

/**
 * orkerEnv is what one Orker in this directory is started with.
 *
 * It carries the work root, for two reasons that happen to have one answer.
 *
 * FIRST, the per-attempt workspace is BIND-MOUNTED into the activity
 * container, and the Orker's default work root is the OS temp directory. On
 * macOS that is `/var/folders/...`, which the Docker VM does not share, so
 * every attempt would fail `WORKDIR_NOT_BINDABLE` before the workload starts.
 * `scripts/stack.sh` already points ORK_ORKER_WORKDIR at `$REPO/.data/work`
 * for the running stack, and this harness does the same.
 *
 * SECOND, an Orker's default identity is derived from its work root
 * (`hostname-<hash of the root>`, internal/orker/activity.go), and an Orker
 * takes an EXCLUSIVE LOCK on that identity at boot so that its startup reaper
 * can force-remove its own dead predecessor's containers without ever
 * touching a live peer's. Two Orkers sharing one root would therefore share
 * one identity, and the second would refuse to start. Several specs here run
 * two Orkers at once (`main` and `gpu`, `main` and `batch`), so each one is
 * given its OWN root and gets its own identity for free.
 *
 * `name` is the pool or role the spec calls this Orker, so a leftover
 * directory says which Orker left it.
 */
export function orkerEnv(name: string) {
	const root = path.join(repoRoot, '.data', 'work', 'e2e', name);
	// Created HERE, before the Orker boots, rather than left to the Orker's own
	// MkdirAll. The Orker creates the root and then immediately asks the daemon
	// to bind-mount it, and on a macOS daemon the share is a virtiofs mount of
	// the host filesystem: a directory that came into existence microseconds
	// earlier has been observed to fail that probe with a bare
	// `docker exited -1`. Creating it up front removes the race rather than
	// retrying around it.
	mkdirSync(root, { recursive: true });
	return { ORK_ORKER_WORKDIR: root };
}

/**
 * The registry the first-party activity images are built and pushed to. It is
 * `registry:3` from deploy/docker-compose.yml, the same one images/Makefile
 * pushes to.
 */
export const testRegistry = process.env.ORK_TEST_REGISTRY ?? 'localhost:5001';
const imageNamespace = process.env.ORK_TEST_IMAGE_NAMESPACE ?? 'orkstack';
const imageTag = process.env.ORK_TEST_IMAGE_TAG ?? 'dev';

/** activityRef spells one first-party image reference, as images/Makefile does. */
export const activityRef = (name: string) =>
	`${testRegistry}/${imageNamespace}/activity-${name}:${imageTag}`;

/**
 * catalogEnv is what every ENGINE in this directory is started with.
 *
 * `uses: shell` is a CATALOG name, and the catalog is engine configuration
 * (§4.2). Its built-in default points at `ghcr.io/orkstack/activity-shell:0.1`,
 * which is not published, so an engine booted without this resolves nothing
 * and every apply fails IMAGE_RESOLVE_UNAUTHORIZED — apply-time resolution is
 * a real registry GET (§4.3), not a lookup in a table. The catalog must point
 * at the local registry: an engine booted against ghcr.io resolves nothing.
 */
export const catalogEnv = {
	ORK_ACTIVITY_CATALOG: `activities:
  shell: ${activityRef('shell')}
  http: ${activityRef('http')}
  file: ${activityRef('file')}
`
};

/**
 * ensureActivityImages builds and pushes the three first-party images.
 *
 * Pushed, not merely built: a never-pushed tag has no registry manifest
 * digest, so apply-time resolution cannot pin it and `uses: shell` fails
 * (§4.6). Idempotent, so a spec that runs after another is a no-op.
 */
export function ensureActivityImages(): void {
	execSync('make -C images push', { cwd: repoRoot, stdio: 'ignore', timeout: 600_000 });
}

/**
 * containerHost is how a WORKLOAD addresses a server running in this test
 * process.
 *
 * Every activity is a container now, so `127.0.0.1` inside one is the
 * container's own loopback and never the host's. Docker Desktop publishes the
 * host as `host.docker.internal`; on a Linux daemon the same name works when
 * the container is started with `--add-host`, and ORK_TEST_CONTAINER_HOST is
 * the override for anything else.
 *
 * A server a container must reach therefore has to listen on all interfaces
 * as well: binding 127.0.0.1 and advertising this name would still refuse the
 * connection.
 */
export const containerHost = process.env.ORK_TEST_CONTAINER_HOST ?? 'host.docker.internal';

/**
 * cosignSign runs test/tools/cosign-sign (built into bin/ on each call) — the test
 * tool that writes cosign key-based signatures in exactly the layout `cosign
 * sign --key` writes and the engine and Orker verify. `keygen DIR` writes
 * DIR/cosign.key and DIR/cosign.pub; `sign --key KEY REF` signs REF's digest
 * in its (plain-HTTP, loopback) registry. Test tooling only: it holds a
 * private key, which nothing OrkStack ships ever does.
 */
export function cosignSign(args: string[]): string {
	// Rebuilt every call: a binary left by another branch's tree must never
	// be the one that writes this run's signatures.
	const bin = path.join(repoRoot, 'bin', 'cosign-sign');
	execFileSync('go', ['build', '-o', 'bin/cosign-sign', './test/tools/cosign-sign'], { cwd: repoRoot });
	return execFileSync(bin, args, { cwd: repoRoot, encoding: 'utf8' });
}
