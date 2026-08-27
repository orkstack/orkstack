import { expect, test, type Page } from '@playwright/test';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killAll,
	noteTo,
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

// M11.5's before and after, on ONE lineage, against ONE engine.
//
// Two builds of the same application are asked the same question about the
// same three stored versions: the tree as it was before this milestone
// (`ORK_BEFORE_UI_DIR`, built by proof/M11.5/reproduce.sh from the parent
// commit) and the tree as it is now. Same data, same engine, same browser, one
// difference — which is what makes the pair a comparison rather than two
// pictures.
//
// It is also a test, and it fails for the right reason on either side. The
// BEFORE build must show HEAD's document under a URL that names the CURRENT
// version, and must offer no way to start an edit from the version list; the
// AFTER build must show the named version's document and offer the link. The
// first of those is the bug this milestone exists to kill, photographed rather
// than described.

const ENGINE = 'http://127.0.0.1:18088';
const ENGINE_WS = 'ws://127.0.0.1:18088';
const AFTER_ORIGIN = 'http://127.0.0.1:14181';
const BEFORE_ORIGIN = 'http://127.0.0.1:14182';

const TEMPORAL_NAMESPACE =
	process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-m11-5-version-scoped-editor';
const BEFORE_UI_DIR = process.env.ORK_BEFORE_UI_DIR ?? '';

const NS = 'm11-5-editing';
const STAMP = Date.now().toString(36);
const ORKFLOW = `before-after-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR ?? path.join(uiDir, 'test-results', 'proof-m11-5');
const out = proofOut(PROOF_DIR);
const note = noteTo(out('before-after.txt'));

const shot = (page: Page, name: string) => page.screenshot({ path: out(name), fullPage: true });
const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// `command` is a JSON ARRAY under the OCI Activity Contract — argv, never a
// shell string — so the caller's command goes to an explicit `sh -c`.
function orkflowYaml(version: string, description: string, command: string): string {
	return `name: ${ORKFLOW}
version: "${version}"
description: ${description}

tasks:
  - id: greet
    uses: shell
    with:
      op: exec
      command: ["sh", "-c", ${JSON.stringify(command)}]
`;
}

// The editor's URL for one version. Both builds are given exactly this — the
// coordinate is the page's, not the build's.
const editUrl = (origin: string, version: string) =>
	`${origin}/orkflows/${ORKFLOW}?ns=${NS}&version=${version}&tab=edit`;

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		out('before-after.txt'),
		[
			'# M11.5 — the same orkflow, the same engine, two builds of the UI.',
			'#',
			'# Written by ui/e2e/version-editor-before-after.spec.ts.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespace:      ${NS}`,
			`# orkflow:            ${ORKFLOW} (0.1.0 current, 0.2.0 head)`,
			`# before build:       ${BEFORE_UI_DIR}`,
			`# after build:        ${uiDir}/build`,
			''
		].join('\n')
	);

	if (!BEFORE_UI_DIR || !existsSync(path.join(BEFORE_UI_DIR, 'build', 'index.js'))) {
		// Not a skip: without the other half there is no comparison to make, and
		// a proof that quietly published only the "after" picture would claim
		// something it never measured. reproduce.sh builds this.
		throw new Error(
			'ORK_BEFORE_UI_DIR must name a built pre-M11.5 UI (see proof/M11.5/reproduce.sh)'
		);
	}
	if (!(await tcpOpen(7233))) throw new Error('Temporal is not reachable on 127.0.0.1:7233');
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18088, 14181, 14182]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	// The three first-party images, built and PUSHED: `uses: shell` is a
	// catalog name resolved by a real registry GET at apply time (§4.3), so an
	// unpushed tag has no manifest digest to pin (§4.6).
	ensureActivityImages();
	execFileSync('go', ['build', '-o', 'bin/ork', './cmd/ork'], { cwd: repoRoot });
	execFileSync('bun', ['run', 'build'], { cwd: uiDir, stdio: 'inherit' });

	spawnLogged(out('shots-engine.log'), './bin/ork', [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18088',
		ORK_WS_ALLOWED_ORIGINS: `${AFTER_ORIGIN},${BEFORE_ORIGIN}`
	});
	await waitForEngine(ENGINE);

	for (const [name, dir, port] of [
		['shots-ui-after.log', uiDir, '14181'],
		['shots-ui-before.log', BEFORE_UI_DIR, '14182']
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

	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: NS, description: 'M11.5 version-scoped editing' }
	});
	// The condition the bug hid behind: current and head are DIFFERENT
	// versions. Applied by the engine, so both builds are asked about a lineage
	// neither of them made.
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml('0.1.0', 'first revision', 'echo one'),
		namespace: NS
	});
	await rpc('OrkflowService/ApplyOrkflow', {
		yaml: orkflowYaml('0.2.0', 'second revision', 'echo two'),
		namespace: NS
	});
	await rpc('OrkflowService/SetCurrentOrkflowVersion', {
		name: ORKFLOW,
		version: '0.1.0',
		namespace: NS
	});
	note(`${ORKFLOW}: 0.1.0 promoted (current), 0.2.0 applied last (head)`);
});

test.afterAll(async () => {
	await killAll();
});

test('the page before this milestone: a URL naming 0.1.0 opens head, and no row offers Edit', async ({
	page
}) => {
	await page.goto(editUrl(BEFORE_ORIGIN, '0.1.0'));
	await expect(page.getByTestId('editor-form')).toBeVisible();

	// THE BUG. The URL names 0.1.0 — the version that RUNS — and the editor is
	// derived from 0.2.0, holding 0.2.0's text. An apply from here publishes a
	// document the operator never opened, under a base they never chose.
	await expect(page.getByTestId('editor-base-version')).toHaveText('0.2.0');
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/second revision/);
	await expect(page.getByTestId('editor-yaml')).not.toHaveValue(/first revision/);
	await shot(page, 'screenshot-before-editor-shows-head.png');

	// And there is no way in from the version list: the row for the version an
	// operator wants to change offers only Promote.
	await page.goto(`${BEFORE_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&tab=versions`);
	await expect(page.getByTestId('version-row-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-edit-0.1.0')).toHaveCount(0);
	await expect(page.getByTestId('version-edit-0.2.0')).toHaveCount(0);
	await shot(page, 'screenshot-before-versions-no-edit.png');
	note('before: ?version=0.1.0&tab=edit renders 0.2.0 (head); no row offers Edit');
});

test('the page after it: the same URL opens 0.1.0, over a head it names', async ({ page }) => {
	await page.goto(editUrl(AFTER_ORIGIN, '0.1.0'));
	await expect(page.getByTestId('editor-form')).toBeVisible();

	await expect(page.getByTestId('editor-base-version')).toHaveText('0.1.0');
	await expect(page.getByTestId('editor-head-version')).toHaveText('0.2.0');
	await expect(page.getByTestId('editor-yaml')).toHaveValue(/first revision/);
	await expect(page.getByTestId('editor-yaml')).not.toHaveValue(/second revision/);
	// The fork is stated before it happens: applying becomes the new head and
	// leaves the running version alone.
	await expect(page.getByTestId('editor-fork-notice')).toBeVisible();
	await shot(page, 'screenshot-after-editor-shows-selected.png');

	await page.goto(`${AFTER_ORIGIN}/orkflows/${ORKFLOW}?ns=${NS}&tab=versions`);
	await expect(page.getByTestId('version-edit-0.1.0')).toBeVisible();
	await expect(page.getByTestId('version-edit-0.2.0')).toBeVisible();
	await shot(page, 'screenshot-after-versions-edit-action.png');
	note('after: the same URL renders 0.1.0 over head 0.2.0; every row offers Edit');
});
