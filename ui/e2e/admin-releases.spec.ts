import { expect, test, type Page } from './test';
import { type ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	noteTo,
	orkerEnv,
	proofOut,
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	waitFor,
	waitForEngine,
	bin,
	temporalAddress,
	temporalReachable
} from './harness';

// Admin → Releases (docs/DEPLOY.md §3), against real engines and Orkers
// started with ORK_BUILD_ID, real Temporal (a private namespace) and real
// MinIO. The UI is served by an engine started WITHOUT ORK_BUILD_ID that
// shares the deployment name: it is the "suite's unversioned engine" the
// first scenario reads before any release exists, and it keeps answering for
// the deployment as releases come and go — a replica of no release answers
// like any other.
//
//   1. an unversioned deployment says versioning is off;
//   2. the first release is promoted from the page;
//   3. a release missing a pool cannot be promoted, and says why;
//   4. promoting moves new runs, and the previous release shows what it is
//      finishing, its run linking to the run's own page;
//   5. a decision made on a stale read is refused in the dialog, and the
//      Ramp dialog offers a CLI equivalent only for a percent it accepts.
//
// Process hygiene: this spec signals ONLY the processes it started, by
// handle.

const PORT_UI_ENGINE = 18461;
const PORT_V1 = 18462;
const PORT_V2 = 18463;
const UI_PORT = 14461;
const ENGINE = `http://127.0.0.1:${PORT_UI_ENGINE}`;
const UI_ORIGIN = `http://127.0.0.1:${UI_PORT}`;

const STAMP = Date.now().toString(36);
const TEMPORAL_NAMESPACE = `ork-ui-releases-${STAMP}`;
const DEPLOYMENT = `e2e-rel-${STAMP}`;
const V1 = 'e2e-v1';
const V2 = 'e2e-v2';
const ORKER_V1 = `orker-rel-v1-${STAMP}`;
const ORKER_V2 = `orker-rel-v2-${STAMP}`;
const GATED = `rel-gated-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'admin-releases')
	: path.join(uiDir, 'test-results', 'admin-releases');
const TRANSCRIPT = 'admin-releases-session.txt';
const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

test.describe.configure({ mode: 'serial' });

const mine: ChildProcess[] = [];
let gatedRun = '';

async function stop(child: ChildProcess | undefined): Promise<void> {
	if (!child || child.exitCode !== null || child.signalCode) return;
	child.kill('SIGTERM');
	const deadline = Date.now() + 15_000;
	while (child.exitCode === null && !child.signalCode && Date.now() < deadline) {
		await new Promise((r) => setTimeout(r, 100));
	}
	if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL');
}

const deploymentEnv = (build: string) => ({
	...clusterEnv(TEMPORAL_NAMESPACE),
	...s3Env,
	ORK_DEPLOYMENT_NAME: DEPLOYMENT,
	ORK_BUILD_ID: build
});

async function startEngine(name: string, port: number, build: string): Promise<void> {
	mine.push(
		spawnLogged(outPath(`${name}.log`), bin('ork'), [], {
			...deploymentEnv(build),
			...catalogEnv,
			ORK_HTTP_ADDR: `127.0.0.1:${port}`,
			ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
		})
	);
	await waitForEngine(`http://127.0.0.1:${port}`);
	note(`engine ${name} up on :${port} (ORK_BUILD_ID=${JSON.stringify(build)})`);
}

const workRoot = (id: string) => orkerEnv(id).ORK_ORKER_WORKDIR;

function startOrker(id: string, build: string): void {
	mine.push(
		spawnLogged(outPath(`${id}.log`), bin('orker'), [], {
			...deploymentEnv(build),
			...orkerEnv(id),
			ORK_ORKER_ID: id
		})
	);
	note(`orker ${id} started (ORK_BUILD_ID=${build})`);
}

interface ReleaseAnswer {
	buildId: string;
	role?: string;
	engineReplicas?: number;
	orkers?: { orkerId: string; runningTasks?: number }[];
	blockers?: { message: string }[];
	runsInFlight?: number;
}
interface ListReleasesAnswer {
	versioned?: boolean;
	releases?: ReleaseAnswer[];
	revision?: string;
}

const listReleases = (body: unknown = {}) => callEngine<ListReleasesAnswer>(ENGINE, 'SystemService/ListReleases', body);
const release = (answer: ListReleasesAnswer, build: string) => (answer.releases ?? []).find((r) => r.buildId === build);

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

async function openReleases(page: Page): Promise<void> {
	await page.goto(`${UI_ORIGIN}/admin/releases?ns=default`);
	await expect(page.getByTestId('releases-scope')).toContainText('the namespace switcher does not scope this page');
}

async function refresh(page: Page): Promise<void> {
	await page.getByTestId('releases-refresh').click();
	await expect(page.getByTestId('releases-refresh')).toHaveText('Refresh');
}

/** openGates writes the gate file into every attempt tmp directory under root. */
function openGates(root: string): number {
	let opened = 0;
	const walk = (dir: string, depth: number) => {
		if (depth > 6) return;
		let entries: string[] = [];
		try {
			entries = readdirSync(dir);
		} catch {
			return;
		}
		for (const entry of entries) {
			const full = path.join(dir, entry);
			let isDir = false;
			try {
				isDir = statSync(full).isDirectory();
			} catch {
				continue;
			}
			if (!isDir) continue;
			if (entry === 'tmp' && existsSync(path.join(dir, 'input'))) {
				try {
					writeFileSync(path.join(full, 'gate'), 'open\n');
					opened++;
				} catch {
					// a workspace being torn down
				}
				continue;
			}
			walk(full, depth + 1);
		}
	};
	walk(root, 0);
	return opened;
}

test.beforeAll(async () => {
	test.setTimeout(900_000);
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# Admin → Releases: release status, promotion and its refusals in a browser.',
			'#',
			'# Written by ui/e2e/admin-releases.spec.ts against real engines and Orkers started',
			'# with ORK_BUILD_ID, on real Temporal + MinIO.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# deployment:         ${DEPLOYMENT} (releases ${V1}, ${V2})`,
			`# ui engine:          ${ENGINE} (no ORK_BUILD_ID)`,
			''
		].join('\n')
	);
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [PORT_UI_ENGINE, PORT_V1, PORT_V2, UI_PORT]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}
	ensureActivityImages();

	await startEngine('engine-unversioned', PORT_UI_ENGINE, '');
	mine.push(
		spawnLogged(
			outPath('ui.log'),
			'node',
			['build'],
			{ HOST: '127.0.0.1', PORT: String(UI_PORT), ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
			uiDir
		)
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);
	note(`ui at ${UI_ORIGIN}, reading ${ENGINE}`);
});

test.afterAll(async () => {
	for (const id of [ORKER_V1, ORKER_V2]) openGates(workRoot(id));
	for (const child of mine.splice(0)) await stop(child);
});

test('an unversioned deployment says versioning is off', async ({ page }) => {
	await openReleases(page);
	await expect(page.getByTestId('releases-versioning-off')).toContainText(
		'No release of this deployment has registered with the cluster, so versioning is off'
	);
	await shot(page, 'admin-releases-versioning-off.png');
	note('Releases page: versioning off (no release has registered)');
});

test('the first release is promoted from the page', async ({ page }) => {
	test.setTimeout(300_000);
	await startEngine('engine-v1', PORT_V1, V1);
	startOrker(ORKER_V1, V1);
	await waitFor(
		'v1 registered with an engine and an Orker, and no blockers',
		async () => {
			const r = release(await listReleases(), V1);
			return !!r && (r.engineReplicas ?? 0) >= 1 && (r.orkers ?? []).length >= 1 && (r.blockers ?? []).length === 0;
		},
		180_000
	);
	await openReleases(page);
	await expect(page.getByTestId('releases-no-current')).toBeVisible();
	await expect(page.getByTestId(`release-role-${V1}`)).toHaveText('candidate');
	await expect(page.getByTestId(`release-state-${V1}`)).toHaveText('ready');

	await page.getByTestId(`release-promote-${V1}`).click();
	await expect(page.getByTestId('release-dialog')).toBeVisible();
	await expect(page.getByTestId('release-dialog-blockers')).toHaveText('No blockers: this release can take new runs.');
	await page.getByTestId('release-dialog-cli-trigger').click();
	await expect(page.getByTestId('release-dialog-cli-popover')).toContainText(`orkctl release promote '${V1}'`);
	await expect(page.getByTestId('release-dialog-accept')).toBeEnabled();
	await shot(page, 'admin-releases-first-promote-dialog.png');
	await page.getByTestId('release-dialog-accept').click();
	await expect(page.getByTestId('release-dialog')).toBeHidden({ timeout: 30_000 });
	await expect(page.getByTestId(`release-role-${V1}`)).toHaveText('current');
	await expect(page.getByTestId('releases-result')).toContainText(`release ${V1} is current.`);
	await expect(page.getByTestId('releases-no-current')).toBeHidden();
	await shot(page, 'admin-releases-first-promoted.png');
	note(`promoted ${V1} from the page: ${await page.getByTestId('releases-result').textContent()}`);
});

test('a release missing a pool cannot be promoted, and says why', async ({ page }) => {
	test.setTimeout(300_000);
	await startEngine('engine-v2', PORT_V2, V2);
	await waitFor('v2 registered with its engine', async () => {
		const r = release(await listReleases(), V2);
		return !!r && (r.engineReplicas ?? 0) >= 1 && (r.blockers ?? []).some((b) => b.message.includes('pool "main" is in use'));
	});
	await openReleases(page);
	await refresh(page);
	const sentence = `pool "main" is in use (${ORKER_V1} of release ${V1} serves it) and no Orker of release ${V2} serves it`;
	await expect(page.getByTestId(`release-blockers-${V2}`)).toContainText(sentence);
	await expect(page.getByTestId(`release-state-${V2}`)).toHaveText('not ready (1 blocker)');
	await page.getByTestId(`release-promote-${V2}`).click();
	await expect(page.getByTestId('release-dialog-blockers')).toContainText(sentence);
	await expect(page.getByTestId('release-dialog-accept')).toBeDisabled();
	await shot(page, 'admin-releases-missing-pool.png');
	await page.getByTestId('release-dialog-cancel').click();
	note(`v2 with an engine only: blocked — ${sentence}`);

	startOrker(ORKER_V2, V2);
	await waitFor(
		'v2 passes once its Orker serves main',
		async () => {
			const r = release(await listReleases(), V2);
			return !!r && (r.orkers ?? []).length >= 1 && (r.blockers ?? []).length === 0;
		},
		180_000
	);
	await refresh(page);
	await expect(page.getByTestId(`release-blockers-${V2}`)).toBeHidden();
	await expect(page.getByTestId(`release-state-${V2}`)).toHaveText('ready');
	await shot(page, 'admin-releases-pool-served.png');
	note('v2 Orker started: blockers gone, v2 ready');
});

test('promoting moves new runs and the previous release shows what it is finishing', async ({ page }) => {
	test.setTimeout(300_000);
	const yaml = `apiVersion: orkstack/v1
kind: Orkflow
name: ${GATED}
description: a run held in flight by a gate.
tasks:
  - id: gate
    uses: shell
    timeout_seconds: 900
    with:
      op: exec
      command: ["sh", "-c", "i=0; while [ ! -e /orkstack/tmp/gate ] && [ $i -lt 1200 ]; do sleep 0.5; i=$((i+1)); done"]
`;
	await callEngine(ENGINE, 'OrkflowService/ApplyOrkflow', { yaml });
	const submitted = await callEngine<{ run: { id: string } }>(ENGINE, 'RunService/SubmitRun', { orkflowName: GATED });
	gatedRun = submitted.run.id;
	await waitFor(
		'the gated run is executing on v1',
		async () => {
			const r = release(await listReleases(), V1);
			return (r?.orkers ?? []).some((o) => (o.runningTasks ?? 0) >= 1) && (r?.runsInFlight ?? 0) === 1;
		},
		180_000
	);
	note(`run ${gatedRun} is in flight on ${V1}`);

	await openReleases(page);
	await page.getByTestId(`release-promote-${V2}`).click();
	await expect(page.getByTestId('release-dialog-blockers')).toHaveText('No blockers: this release can take new runs.');
	await page.getByTestId('release-dialog-accept').click();
	await expect(page.getByTestId('release-dialog')).toBeHidden({ timeout: 30_000 });
	await expect(page.getByTestId(`release-role-${V2}`)).toHaveText('current');
	await expect(page.getByTestId('releases-result')).toContainText(`release ${V2} is current (was ${V1}).`);
	await waitFor('v1 previous, still running the gated run', async () => {
		const r = release(await listReleases(), V1);
		return r?.role === 'RELEASE_ROLE_PREVIOUS' && (r?.runsInFlight ?? 0) === 1;
	});
	await refresh(page);
	await expect(page.getByTestId(`release-role-${V1}`)).toHaveText('previous');
	await expect(page.getByTestId(`release-runs-${V1}`)).toHaveText('1');
	await page.getByTestId(`release-holds-toggle-${V1}`).click();
	const link = page.getByTestId(`release-run-${V1}-${gatedRun}`);
	await expect(link).toBeVisible();
	await shot(page, 'admin-releases-previous-finishing.png');
	await link.click();
	await expect(page).toHaveURL(new RegExp(`/runs/${gatedRun}\\?ns=default`));
	note(`promoted ${V2}; ${V1} is previous with run ${gatedRun}, whose link opened /runs/${gatedRun}?ns=default`);
});

test('a decision made on a stale read is refused', async ({ page }) => {
	test.setTimeout(300_000);
	await openReleases(page);
	await page.getByTestId(`release-rollback-${V1}`).click();
	await expect(page.getByTestId('release-dialog-blockers')).toHaveText('No blockers: this release can take new runs.');
	await expect(page.getByTestId('release-dialog-accept')).toBeEnabled();
	// Meanwhile, someone else ramps v1 through the engine directly.
	const ramped = await callEngine<{ changed?: boolean }>(ENGINE, 'SystemService/RampRelease', { buildId: V1, percent: 20 });
	note(`ramped ${V1} to 20% behind the open dialog: ${JSON.stringify(ramped)}`);
	await page.getByTestId('release-dialog-accept').click();
	await expect(page.getByTestId('release-dialog-error')).toContainText('the releases changed since you read them');
	await expect(page.getByTestId(`release-role-${V1}`)).toHaveText('ramping 20%');
	await shot(page, 'admin-releases-stale-refused.png');
	note(`the stale roll back was refused in the dialog: ${await page.getByTestId('release-dialog-error').textContent()}`);
	await page.getByTestId('release-dialog-cancel').click();

	// The Ramp dialog offers a CLI equivalent only for a percent it would
	// accept: `--percent 0` beside an invalid input would be a stop-the-ramp,
	// not the ramp being typed. Nothing is accepted here.
	await page.getByTestId(`release-ramp-${V1}`).click();
	await expect(page.getByTestId('release-dialog')).toBeVisible();
	await page.getByTestId('release-ramp-percent').fill('0');
	await expect(page.getByTestId('release-ramp-percent-error')).toBeVisible();
	await expect(page.getByTestId('release-dialog-accept')).toBeDisabled();
	await expect(page.getByTestId('release-dialog-cli-trigger')).toHaveCount(0);
	await shot(page, 'admin-releases-ramp-invalid-percent.png');
	await page.getByTestId('release-ramp-percent').fill('15');
	await expect(page.getByTestId('release-ramp-percent-error')).toBeHidden();
	await page.getByTestId('release-dialog-cli-trigger').click();
	await expect(page.getByTestId('release-dialog-cli-popover')).toContainText(`orkctl release ramp '${V1}' --percent 15`);
	await page.getByTestId('release-dialog-cli-trigger').click();
	await expect(page.getByTestId('release-dialog-cli-popover')).toBeHidden();
	await page.getByTestId('release-dialog-cancel').click();
	await expect(page.getByTestId('release-dialog')).toBeHidden();
	note('Ramp dialog: no CLI equivalent while the percent is invalid; --percent 15 once it is');

	const opened = openGates(workRoot(ORKER_V1));
	note(`opened ${opened} gate(s) for run ${gatedRun}`);
});
