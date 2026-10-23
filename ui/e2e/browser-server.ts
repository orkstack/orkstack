import { execFileSync } from 'node:child_process';
import { readFileSync, realpathSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// The browser the specs drive runs in a container of its own, and therefore in
// a NETWORK NAMESPACE of its own.
//
// WHY. Docker gives every activity container a veth interface in the host's
// network namespace, added when it starts and removed when it stops. Chromium
// watches its namespace's addresses and, on any change, aborts every request
// still waiting for a connection with net::ERR_NETWORK_CHANGED (e2e/test.ts
// says what that did to a page load of the built UI). While the browser shared
// the host's namespace, every container started anywhere on the host could
// fail a spec, so spec files ran one at a time and one proof's Playwright gate
// excluded every other's. A browser in its own namespace sees only `lo` and
// its own `eth0`, both fixed before Chromium starts: the host's churn never
// reaches it, so spec files can run beside each other and beside another
// proof's (network-change.spec.ts N5/N6 prove it, with a host-namespace
// browser as the positive control).
//
// HOW. e2e/global-setup.ts starts Playwright's own browser server
// (`playwright run-server`) in the pinned image e2e/browser-image.txt names,
// mounting THIS tree's playwright-core so the protocol the server speaks is
// exactly the one the client speaks, and exports its endpoint as
// ORK_E2E_BROWSER_WS. playwright.config.ts connects every worker to it with
// `connectOptions.exposeNetwork: '<loopback>'`, so a spec's
// `http://127.0.0.1:<port>` reaches the engine and UI that spec started on the
// host, through the connection, with nothing in the spec changed. The
// container is this run's own — labelled with the proof id and the runner's
// pid and start time, the same labels scripts/proof.sh gives a proof's Temporal
// — stopped by e2e/global-teardown.ts by its recorded id, and reaped by the
// next proof's sweep if this runner was killed outright. ORK_E2E_BROWSER=host
// drives the host's own Chromium instead, for a developer without the image;
// a proof never sets it.

const e2eDir = path.dirname(fileURLToPath(import.meta.url));
const uiDir = path.resolve(e2eDir, '..');

/** The file naming the browser-server image: one line, `repo:tag@sha256:digest`. */
export const browserImageFile = path.join(e2eDir, 'browser-image.txt');

/** browserImage reads browser-image.txt: the one non-comment line. */
export function browserImage(file = browserImageFile): string {
	const lines = readFileSync(file, 'utf8')
		.split('\n')
		.map((l) => l.trim())
		.filter((l) => l && !l.startsWith('#'));
	if (lines.length !== 1) throw new Error(`${file}: want exactly one image reference, got ${lines.length} lines`);
	return lines[0];
}

/** The server's port inside the container; the host side is published on a free port. */
const SERVER_PORT = 3000;

/** procStart is this process's start time in clock ticks since boot (field 22 of /proc/self/stat), as scripts/proof.sh records it. */
function procStart(): string {
	try {
		const stat = readFileSync('/proc/self/stat', 'utf8');
		return stat.slice(stat.lastIndexOf(')') + 2).split(' ')[19] ?? '';
	} catch {
		return '';
	}
}

export interface BrowserServer {
	/** The WebSocket endpoint a client connects to, on the host's loopback. */
	wsEndpoint: string;
	/** The container's id, what global-teardown.ts stops. */
	container: string;
	/** The image reference, as browser-image.txt names it. */
	image: string;
}

/**
 * startBrowserServer starts the browser-server container and waits until it
 * listens. It throws, naming the image, when the image is not on the host:
 * `docker pull` it first (a proof's images step does), because an unpinned
 * fetch at run time is what the pinned reference exists to prevent.
 */
export function startBrowserServer(proofId: string): BrowserServer {
	const image = browserImage();
	try {
		execFileSync('docker', ['image', 'inspect', image], { stdio: 'ignore' });
	} catch {
		throw new Error(`browser-server image ${image} is not on this host: docker pull it first (ORK_E2E_BROWSER=host drives the host's own Chromium instead)`);
	}
	// Bun's linker may place the package behind a link; the daemon mounts a
	// real path.
	const core = realpathSync(path.join(uiDir, 'node_modules', 'playwright-core'));
	const owner = `${os.hostname()}:${process.pid}:${procStart()}`;
	const container = execFileSync(
		'docker',
		[
			'run',
			'-d',
			'--rm',
			'--init',
			'--user',
			'pwuser',
			'--shm-size=1g',
			'--label',
			`orkstack.proof=${proofId}`,
			'--label',
			`orkstack.proof.owner=${owner}`,
			'--label',
			'orkstack.e2e=browser-server',
			'-p',
			`127.0.0.1::${SERVER_PORT}`,
			'-v',
			`${core}:/pw:ro`,
			image,
			'node',
			'/pw/cli.js',
			'run-server',
			'--port',
			String(SERVER_PORT),
			'--host',
			'0.0.0.0'
		],
		{ encoding: 'utf8' }
	).trim();
	try {
		const published = execFileSync('docker', ['port', container, `${SERVER_PORT}/tcp`], { encoding: 'utf8' })
			.split('\n')
			.map((l) => l.trim())
			.filter(Boolean)[0];
		const hostPort = published?.slice(published.lastIndexOf(':') + 1);
		if (!hostPort) throw new Error(`docker published no port for ${container}`);
		// The server prints its endpoint once it listens; its path is whatever
		// the server chose, so it is read rather than assumed.
		const deadline = Date.now() + 60_000;
		let listening = '';
		while (!listening) {
			const logs = execFileSync('docker', ['logs', container], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
			const m = /Listening on (ws:\/\/\S+)/.exec(logs);
			if (m) listening = m[1];
			else if (Date.now() > deadline) throw new Error(`browser server ${container} never reported it was listening:\n${logs}`);
			else execFileSync('sleep', ['0.25']);
		}
		const inside = new URL(listening);
		const wsEndpoint = `ws://127.0.0.1:${hostPort}${inside.pathname}${inside.search}`;
		return { wsEndpoint, container, image };
	} catch (err) {
		stopBrowserServer(container);
		throw err;
	}
}

/** stopBrowserServer stops exactly the container startBrowserServer started; `--rm` removes it. */
export function stopBrowserServer(container: string): void {
	try {
		execFileSync('docker', ['stop', '-t', '5', container], { stdio: 'ignore' });
	} catch {
		// Already gone: the daemon restarted, or a sweep took it.
	}
}
