import { expect, type Page } from '@playwright/test';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { appendFileSync, createWriteStream, mkdirSync, mkdtempSync } from 'node:fs';
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

/**
 * The processes this spec file started (spawnLogged, or a spec's own spawn
 * passed to trackChild), so killStarted can end exactly those. Nothing in the
 * e2e suite ever signals a process by name or pattern: several stacks, proofs
 * and agents share one host, and a pattern like `/bin/ork$` reaches every
 * engine on it, not just this spec's.
 */
const children: ChildProcess[] = [];

/**
 * trackChild registers a process this spec started, so killStarted and the
 * exit backstop below end it.
 */
export function trackChild(child: ChildProcess): ChildProcess {
	children.push(child);
	return child;
}

// Backstop: if the worker exits without its afterAll running (a crashed hook,
// an uncaught error), still end this spec's own children — by handle, never by
// name. 'exit' handlers must be synchronous; kill() is.
process.on('exit', () => {
	for (const child of children) {
		if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
	}
});

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
	return trackChild(child);
}

/**
 * waitFor polls `probe` until it answers true, every `intervalMs` (250 ms by
 * default), and fails naming `label` once `timeoutMs` has passed. A probe
 * that is itself costly for the system under test — a GetRun of a run whose
 * history holds tens of thousands of events — passes a longer interval.
 */
export async function waitFor(
	label: string,
	probe: () => Promise<boolean>,
	timeoutMs = 120_000,
	intervalMs = 250
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			if (await probe()) return;
		} catch {
			// not ready yet
		}
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
		await new Promise((r) => setTimeout(r, intervalMs));
	}
}

// openNamespaceEdit opens Admin → Namespaces' edit form for one namespace and
// returns once THAT namespace's form is on the page.
//
// One click is not always enough, and the page is the reason: it lists every
// namespace the deployment has — on the shared development store, well over a
// thousand rows on one very tall page — and a click on one row's Edit that
// lands anywhere but that row's handler (a lost click, or the row beside it
// after the table moved) leaves this namespace's form closed. One proof run
// failed exactly so: the click on `namespace-edit-<ns>`
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
 * killStarted ends exactly the processes this spec started (spawnLogged or
 * trackChild) — by handle, never by name — and waits for them to exit:
 * SIGTERM, then SIGKILL after 10 s. It is the only way the e2e suite stops a
 * process.
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

/**
 * temporalAddress is the Temporal every engine and Orker in this directory
 * talks to: the proof's own (scripts/proof.sh runs a Temporal of its own on a
 * free port and exports ORK_TEMPORAL_ADDRESS), or `make infra-up`'s.
 */
export function temporalAddress(): string {
	return process.env.ORK_TEMPORAL_ADDRESS || '127.0.0.1:7233';
}

/** temporalReachable probes temporalAddress(). */
export function temporalReachable(): Promise<boolean> {
	const address = temporalAddress();
	const colon = address.lastIndexOf(':');
	return tcpOpen(Number(address.slice(colon + 1)), address.slice(0, colon) || '127.0.0.1');
}

export function clusterEnv(namespace: string) {
	return { ORK_TEMPORAL_ADDRESS: temporalAddress(), ORK_TEMPORAL_NAMESPACE: namespace };
}

/**
 * bin is the path of one binary this suite runs: `ork`, `orker`, `orkctl`,
 * `cosign-sign`, `deaf-poller`. They are built ONCE per run, from this tree,
 * by e2e/global-setup.ts — into the proof's own directory under a proof
 * (ORK_TEST_BIN_DIR), into a temporary one otherwise — and never into the
 * repository's bin/, which every worktree's dev stack and every concurrent
 * proof would share (AGENTS.md §11.1).
 */
export function bin(name: string): string {
	const dir = process.env.ORK_TEST_BIN_DIR;
	if (!dir) throw new Error('ORK_TEST_BIN_DIR is unset: e2e/global-setup.ts builds the binaries this suite runs');
	return path.join(dir, name);
}

/** The work root orkerEnv made for each name, in this process. */
const orkerRoots = new Map<string, string>();

/**
 * orkerEnv is what one Orker in this directory is started with.
 *
 * It carries the work root, for three reasons that happen to have one answer.
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
 * THIRD, the root is named like a Go test's (`<label>-p<pid>-<suffix>`,
 * internal/testsupport/proof.WorkRootPattern): `e2e-<name>-p<this process's
 * pid>-<suffix>`. An Orker killed mid-task leaves its activity containers and
 * its root behind, and `proofrun clean` removes them once the process the
 * root names is gone — a container by the root its workspace mounts. A fixed
 * root names no process, so its leftovers could never be told apart from a
 * live run's (AGENTS.md §11.1).
 *
 * `name` is the pool or role the spec calls this Orker, so a leftover
 * directory says which Orker left it. One process gets one root per name, so
 * an Orker the spec boots again, and a spec reading its root, find the same
 * one.
 */
export function orkerEnv(name: string) {
	let root = orkerRoots.get(name);
	if (!root) {
		const parent = path.join(repoRoot, '.data', 'work');
		mkdirSync(parent, { recursive: true });
		// Created HERE, before the Orker boots, rather than left to the Orker's
		// own MkdirAll. The Orker creates the root and then immediately asks the
		// daemon to bind-mount it, and on a macOS daemon the share is a virtiofs
		// mount of the host filesystem: a directory that came into existence
		// microseconds earlier has been observed to fail that probe with a bare
		// `docker exited -1`. Creating it up front removes the race rather than
		// retrying around it.
		root = mkdtempSync(path.join(parent, `e2e-${name}-p${process.pid}-`));
		orkerRoots.set(name, root);
	}
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
  json: ${activityRef('json')}
  git: ${activityRef('git')}
  template: ${activityRef('template')}
`
};

/** The first-party activity images, as images/Makefile's ACTIVITIES names them. */
export const activityNames = ['shell', 'http', 'file', 'json', 'git', 'template'];

/**
 * ensureActivityImages requires every first-party image in the registry
 * under this run's tag, and fails naming the one that is not there.
 *
 * It never pushes. A proof pushes this tree's images ONCE, under a tag of its
 * own (ORK_TEST_IMAGE_TAG), before any spec runs; a push from a spec would
 * move a tag every other process on the host resolves (AGENTS.md §11.1).
 * Outside a proof the tag is `dev`: push this tree's images first with
 * `make -C images push`. Pushed, not merely built: a never-pushed tag has no
 * registry manifest digest, so apply-time resolution cannot pin it (§4.6).
 */
export function ensureActivityImages(): void {
	const accept = [
		'application/vnd.oci.image.index.v1+json',
		'application/vnd.oci.image.manifest.v1+json',
		'application/vnd.docker.distribution.manifest.list.v2+json',
		'application/vnd.docker.distribution.manifest.v2+json'
	].join(', ');
	for (const name of activityNames) {
		const url = `http://${testRegistry}/v2/${imageNamespace}/activity-${name}/manifests/${imageTag}`;
		try {
			execFileSync('curl', ['-sf', '-o', '/dev/null', '-H', `Accept: ${accept}`, url], { stdio: 'ignore', timeout: 30_000 });
		} catch {
			throw new Error(
				`${activityRef(name)} is not in the registry: push this tree's images first (make -C images push${
					imageTag === 'dev' ? '' : ` TAG=${imageTag}`
				})`
			);
		}
	}
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
 * cosignSign runs test/tools/cosign-sign — the test tool that writes cosign
 * key-based signatures in exactly the layout `cosign sign --key` writes and
 * the engine and Orker verify. `keygen DIR` writes DIR/cosign.key and
 * DIR/cosign.pub; `sign --key KEY REF` signs REF's digest in its (plain-HTTP,
 * loopback) registry. Test tooling only: it holds a private key, which
 * nothing OrkStack ships ever does. The binary is this run's own, built from
 * this tree by e2e/global-setup.ts, so one left by another branch's tree can
 * never be the one that writes this run's signatures.
 */
export function cosignSign(args: string[]): string {
	return execFileSync(bin('cosign-sign'), args, { cwd: repoRoot, encoding: 'utf8' });
}

/**
 * ownBucket names an object-store bucket for THIS spec file alone:
 * `orkstack-<label>-<pid>-<suffix>`, the shape internal/testsupport/s3bucket
 * gives every Go integration test and scripts/proof-clean.sh recognises (it
 * removes one only once the pid in its name is gone).
 *
 * Every spec used to share the development bucket, `orkstack-artifacts`, and
 * with it that bucket's deployment-wide documents — retention, image policy,
 * audit, webhooks, the namespace registry — and every earlier run's leftovers,
 * which made an engine boot cost time proportional to the bucket's history
 * and made spec files unable to run beside each other (AGENTS.md §11.1). A
 * bucket of the file's own starts empty, is the file's to write, and is
 * removed by removeBucket once the file is done with it.
 */
export function ownBucket(label: string): string {
	if (!/^[a-z][a-z0-9-]{0,19}$/.test(label)) throw new Error(`ownBucket: label ${JSON.stringify(label)} must be ^[a-z][a-z0-9-]{0,19}$`);
	return `orkstack-${label}-${process.pid}-${Math.floor(Math.random() * 1_000_000_000)}`;
}

/** storeEnv is s3Env against `bucket`: what an engine or Orker of this spec is started with. */
export function storeEnv(bucket: string): Record<string, string> {
	return { ...s3Env, ORK_S3_BUCKET: bucket };
}

/** bucketToolEnv is the environment test/tools/bucket-ensure needs for `action` on `bucket`. */
function bucketToolEnv(bucket: string, action: 'ensure' | 'cleanup'): NodeJS.ProcessEnv {
	return { ...process.env, ...storeEnv(bucket), ORK_BUCKET_ACTION: action };
}

/** ensureBucket creates `bucket` (idempotently) before the spec's engine and Orker boot against it. */
export function ensureBucket(bucket: string): void {
	execFileSync(bin('bucket-ensure'), [], { cwd: repoRoot, env: bucketToolEnv(bucket, 'ensure'), stdio: 'pipe' });
}

/**
 * removeBucket empties and deletes `bucket` once the spec is done with it.
 * A failure is reported and never thrown: the spec's own assertions are its
 * verdict, and a bucket left behind is what scripts/proof-clean.sh lists.
 */
export function removeBucket(bucket: string, report: (line: string) => void = console.warn): void {
	try {
		execFileSync(bin('bucket-ensure'), [], { cwd: repoRoot, env: bucketToolEnv(bucket, 'cleanup'), stdio: 'pipe' });
	} catch (err) {
		report(`e2e: bucket ${bucket} was not removed (left for scripts/proof-clean.sh): ${err}`);
	}
}

/**
 * temporalNamespace names a Temporal namespace for THIS run of THIS spec file
 * alone: `<prefix>-<label>-<pid>-<stamp>`, where the prefix is
 * ORK_TEMPORAL_NAMESPACE (`ork-ui` by default), extended as AGENTS.md §11.1
 * asks of every integration test. A spec used to name a FIXED namespace, and
 * against a long-lived Temporal (`make infra-up`'s) its lists then held the
 * runs of every earlier run of the spec — whose snapshots and commits live in
 * an earlier run's bucket, gone with it — so a page could pick a run the
 * engine could no longer read. A proof's Temporal is its own and fresh; a
 * developer's is not, and the spec must not care which it is given.
 */
export function temporalNamespace(label: string): string {
	return `${process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui'}-${label}-${process.pid}-${Date.now().toString(36)}`;
}
