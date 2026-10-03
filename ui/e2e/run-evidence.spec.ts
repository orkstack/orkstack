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
	rpc as callEngine,
	s3Env,
	spawnLogged,
	tcpOpen,
	uiDir,
	repoRoot,
	waitFor,
	waitForEngine
} from './harness';

// The Evidence dialog on the run page, in a browser, against the real stack
// (docs/RUN_EVIDENCE.md).
//
// What is measured is the client surface of ExportRunEvidence: the manifest is
// shown BEFORE any bundle byte is fetched, the include toggles change both the
// manifest and the CLI equivalent, the browser download completes only on the
// engine's `last` chunk — and the file it saves is checked by the CLI's own
// offline verifier, so the browser and the terminal are proven to hand an
// auditor the same, verifiable file. A live run is not offered evidence.
//
// Topology: one engine, one Orker, the production UI build. Isolation per
// AGENTS.md §11.1 — a Temporal namespace of this spec's own, ports of its own
// and stamped orkflow names.

const ENGINE = 'http://127.0.0.1:18537';
const ENGINE_WS = 'ws://127.0.0.1:18537';
const UI_ORIGIN = 'http://127.0.0.1:14537';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-run-evidence';
const IMAGE = process.env.ORK_TEST_CONTAINER_IMAGE ?? activityRef('shell');

const STAMP = Date.now().toString(36);
const COMPLETED = `run-evidence-${STAMP}`;
const SLEEPER = `run-evidence-live-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'run-evidence')
	: path.join(uiDir, 'test-results', 'run-evidence');
const TRANSCRIPT = 'run-evidence-session.txt';

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));
const ORKCTL = path.join(repoRoot, 'bin', 'orkctl');

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

/** The run-operations completedWorkflow shape: one artifact, values, logs. */
function completedWorkflow(): string {
	return `name: ${COMPLETED}
version: "0.1.0"
description: Run evidence coverage.
tasks:
  - id: completed
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        echo "evidence line"
        printf REPORT > /orkstack/output/artifacts/report.txt
        printf '{"answer":7}' > /orkstack/output/values.json
`;
}

function sleeperWorkflow(): string {
	return `name: ${SLEEPER}
version: "0.1.0"
description: A run that is still running when the page is asked about it.
tasks:
  - id: sleeper
    uses: shell
    timeout_seconds: 120
    with:
      op: script
      interpreter: sh
      script: |
        sleep 20
        printf '{}' > /orkstack/output/values.json
`;
}

let completedRun = '';

async function submit(name: string): Promise<string> {
	const submitted = await rpc<{ run: { id: string } }>('RunService/SubmitRun', { orkflowName: name });
	note(`submitted run ${submitted.run.id} of ${name}`);
	return submitted.run.id;
}

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The Evidence dialog on the run page: the manifest before the bytes,',
			'# include toggles that change the manifest and the CLI equivalent, a',
			'# download complete only on `last`, verified by orkctl offline.',
			'#',
			'# Written by ui/e2e/run-evidence.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# orkflows:           ${COMPLETED}, ${SLEEPER} (ork namespace: default)`,
			`# engine:             ${ENGINE}`,
			`# container image:    ${IMAGE}`,
			''
		].join('\n')
	);

	// Prerequisites are failures, not skips (AGENTS.md §11.1).
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18537, 14537]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orker', './cmd/orker'], { cwd: repoRoot });
	execFileSync('go', ['build', '-o', 'bin/orkctl', './cmd/orkctl'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });
	if (!existsSync(path.join(uiDir, 'build', 'index.js'))) {
		throw new Error('UI build output missing: ui/build/index.js');
	}

	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(outPath('engine.log'), './bin/ork', [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18537',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	spawnLogged(outPath('orker.log'), './bin/orker', [], {
		...cluster,
		...s3Env,
		...orkerEnv('run-evidence')
	});
	await waitForEngine(ENGINE);

	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14537',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	await waitFor('the default pool reports a live Orker', async () => {
		const answer = await rpc<{ pools?: { name: string; liveOrkerCount?: number }[] }>(
			'NamespaceService/ListPools',
			{}
		);
		return (answer.pools ?? []).some((p) => (p.liveOrkerCount ?? 0) > 0);
	});

	await rpc('OrkflowService/ApplyOrkflow', { yaml: completedWorkflow() });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: sleeperWorkflow() });
	note(`applied ${COMPLETED} and ${SLEEPER}`);
});

test.afterAll(async () => {
	await killAll();
});

/** Opens the finished run's page and waits until the archive answers. */
async function openFinished(page: Page): Promise<void> {
	if (!completedRun) {
		completedRun = await submit(COMPLETED);
	}
	await page.goto(`${UI_ORIGIN}/runs/${completedRun}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 180_000 });
}

/** Opens the dialog and waits for a summary (retrying while the archive write lands). */
async function openDialog(page: Page): Promise<string> {
	await waitFor('the run is archived', async () => {
		if (await page.getByTestId('run-evidence-dialog').isVisible()) {
			await page.getByTestId('run-evidence-dialog-close').click();
		}
		await page.getByTestId('run-evidence').click();
		await expect(page.getByTestId('run-evidence-dialog')).toBeVisible();
		const summary = page.getByTestId('run-evidence-summary');
		const error = page.getByTestId('run-evidence-error');
		await expect(summary.or(error)).toBeVisible({ timeout: 15_000 });
		if (await summary.isVisible()) return true;
		note(`not yet: ${await error.textContent()}`);
		await page.getByTestId('run-evidence-dialog-close').click();
		return false;
	});
	return (await page.getByTestId('run-evidence-summary').textContent())?.trim() ?? '';
}

function presentCount(summary: string): number {
	const match = /(\d+) present/.exec(summary);
	if (!match) throw new Error(`summary has no present count: ${summary}`);
	return Number(match[1]);
}

test('P1 the Evidence dialog shows the manifest before the bytes', async ({ page }) => {
	await openFinished(page);
	const summary = await openDialog(page);
	expect(summary).toMatch(/^\d+ present( · .*)? · [\d.]+ (B|KiB|MiB|GiB) to download$/);
	note(`P1 summary: ${summary}`);
	await expect(page.getByTestId('run-evidence-definition')).toContainText('source.yaml present');

	const toggle = page.getByTestId('run-evidence-manifest-toggle');
	await expect(toggle).toHaveAttribute('aria-expanded', 'false');
	await toggle.click();
	await expect(toggle).toHaveAttribute('aria-expanded', 'true');
	const manifest = page.getByTestId('run-evidence-manifest');
	await expect(manifest).toContainText('"schema_version": 1');
	await expect(manifest).toContainText(completedRun);
	await expect(page.getByTestId('run-evidence-download')).toBeEnabled();
	await shot(page, 'screenshot-run-evidence-dialog.png');

	// PARITY: the dialog's manifest is the same document the API answers for
	// the same request — the bytes `orkctl run evidence export --json` prints.
	const text = (await manifest.textContent()) ?? '';
	const shown = JSON.parse(text) as { members: { path: string }[]; counts: { present: number } };
	expect(shown.counts.present).toBe(presentCount(summary));
	const cli = execFileSync(
		ORKCTL,
		['--server', ENGINE, '--json', 'run', 'evidence', 'export', completedRun],
		{ encoding: 'utf8' }
	);
	const fromCli = JSON.parse(cli) as { members: { path: string }[] };
	expect(fromCli.members.map((m) => m.path)).toEqual(shown.members.map((m) => m.path));
	note(`P1 manifest members: ${shown.members.map((m) => m.path).join(', ')}`);
});

test('P2 toggles change the manifest and the command', async ({ page }) => {
	await openFinished(page);
	const before = presentCount(await openDialog(page));
	const manifest = JSON.parse(
		execFileSync(ORKCTL, ['--server', ENGINE, '--json', 'run', 'evidence', 'export', completedRun], {
			encoding: 'utf8'
		})
	) as { members: { kind: string }[] };
	const artifactCount = manifest.members.filter((m) => m.kind === 'artifact').length;
	expect(artifactCount).toBeGreaterThan(0);

	await page.getByTestId('run-evidence-artifacts').uncheck();
	await expect(page.getByTestId('run-evidence-summary')).toContainText(`${before - artifactCount} present`);
	await page.getByTestId('run-evidence-cli-trigger').click();
	await expect(page.getByTestId('run-evidence-cli-command')).toContainText("'--no-artifacts'");
	await expect(page.getByTestId('run-evidence-cli-command')).toHaveText(
		`orkctl --namespace 'default' 'run' 'evidence' 'export' '${completedRun}' '--output' '${completedRun}.evidence.tar.gz' '--no-artifacts'`
	);
	note(`P2 present ${before} -> ${before - artifactCount} with --no-artifacts`);
	await shot(page, 'screenshot-run-evidence-no-artifacts.png');
});

test('P3 the download completes only on last and the CLI verifies it', async ({ page }) => {
	await openFinished(page);
	await openDialog(page);
	await expect(page.getByTestId('run-evidence-artifacts')).toBeChecked();
	const download = page.waitForEvent('download');
	await page.getByTestId('run-evidence-download').click();
	const saved = await download;
	expect(saved.suggestedFilename()).toBe(`${completedRun}.evidence.tar.gz`);
	const file = outPath(`${completedRun}.evidence.tar.gz`);
	await saved.saveAs(file);
	await expect(page.getByTestId('run-evidence-download')).toHaveText('Download bundle');
	await expect(page.getByTestId('run-evidence-error')).toHaveCount(0);

	// The CLI's offline verifier over the file the browser saved: no server.
	const verified = execFileSync(ORKCTL, ['run', 'evidence', 'verify', file], { encoding: 'utf8' });
	const line = verified.split('\n')[0];
	expect(line).toMatch(/^verified \d+ present members against MANIFEST\.json$/);
	note(`P3 ${line}`);
	writeFileSync(outPath('run-evidence-verify.txt'), `$ orkctl run evidence verify ${path.basename(file)}\n${verified}`);
});

test('P4 a live run offers no Evidence until it finishes', async ({ page }) => {
	const live = await submit(SLEEPER);
	await page.goto(`${UI_ORIGIN}/runs/${live}?ns=default`);
	await expect(page.getByTestId('run-status')).toHaveText('RUNNING', { timeout: 60_000 });
	await expect(page.getByTestId('run-evidence')).toHaveCount(0);
	await shot(page, 'screenshot-run-evidence-live.png');
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED', { timeout: 120_000 });
	await expect(page.getByTestId('run-evidence')).toBeVisible();
	note(`P4 run ${live}: no Evidence while RUNNING, Evidence once SUCCEEDED`);
});

test('P5 no page-level control leaks when the dialog is closed', async ({ page }) => {
	await openFinished(page);
	await expect(page.getByTestId('run-evidence-dialog')).toHaveCount(0);
	await openDialog(page);
	await page.getByTestId('run-evidence-dialog-close').click();
	await expect(page.getByTestId('run-evidence-dialog')).toHaveCount(0);
	await expect(page.getByTestId('run-evidence-summary')).toHaveCount(0);
	await expect(page.getByTestId('run-evidence-manifest')).toHaveCount(0);
	// The overview still reads as before.
	await expect(page.getByTestId('run-status')).toHaveText('SUCCEEDED');
	await expect(page.getByTestId('task-completed')).toBeVisible();
	await expect(page.getByTestId('run-again')).toBeVisible();
	note('P5 the dialog leaves nothing on the page once closed');
});
