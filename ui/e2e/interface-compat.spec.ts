import { expect, test, type Page } from './test';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killStarted,
	noteTo,
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

// The interface compatibility gate in the browser
// (docs/UI_CLI_PARITY.md "Interface compatibility").
//
// Real engine (Connect API over real Temporal + real MinIO) and the real
// production UI build. Every verdict, finding, refusal and caller below is the
// engine's own answer; fixtures go straight to the engine, and everything a
// user does is done in the page.
//
// Isolation (AGENTS.md §11.1): this spec's own ports, its own Temporal
// namespace, a non-`default` OrkStack namespace (so every link has to carry
// the namespace it belongs to), and a per-run stamp on every orkflow name.
// Processes are stopped by the handles this spec started (killStarted), never
// by a name pattern.

const ENGINE = 'http://127.0.0.1:18307';
const ENGINE_WS = 'ws://127.0.0.1:18307';
const UI_ORIGIN = 'http://127.0.0.1:14307';

const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-interface-compat';
const NS = 'interface-compat';
const STAMP = Date.now().toString(36);
const BLOCK = `release-block-${STAMP}`;
const CHECKOUT = `checkout-${STAMP}`;
const PLAIN = `plain-${STAMP}`;
const TRIGGER = `ci-${STAMP}`;
const SCHEDULE = `nightly-${STAMP}`;

const PROOF_DIR = process.env.ORK_PROOF_DIR
	? path.join(process.env.ORK_PROOF_DIR, 'interface-compat')
	: path.join(uiDir, 'test-results', 'interface-compat');
const TRANSCRIPT = 'interface-compat-session.txt';

// Sequential and stateful on purpose: the promotion test moves the pointer
// the later tests read.
test.describe.configure({ mode: 'serial' });

const outPath = proofOut(PROOF_DIR);
const note = noteTo(path.join(PROOF_DIR, TRANSCRIPT));

function shot(page: Page, name: string) {
	return page.screenshot({ path: outPath(name), fullPage: true });
}

const rpc = <T>(method: string, body: unknown): Promise<T> => callEngine<T>(ENGINE, method, body);

// The shared compatibility fixture: 3.0.0 adds a required region and a defaulted
// timeout, drops output url, adds sbom, and rewrites every task.
function block(version: '2.1.0' | '3.0.0'): string {
	if (version === '2.1.0') {
		return `apiVersion: orkstack/v1
kind: Orkflow
name: ${BLOCK}
version: "2.1.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.publish.output.value }}'
tasks:
  - id: sign
    uses: shell
    with: { op: eval, command: ["sh", "-c", "echo sign"] }
  - id: publish
    uses: shell
    needs: [sign]
    with: { op: eval, command: ["sh", "-c", "echo publish"] }
`;
	}
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${BLOCK}
version: "3.0.0"
inputs:
  image: { type: string, required: true }
  notes: { type: string, default: "" }
  region: { type: string, required: true }
  timeout: { type: number, default: 300 }
outputs:
  digest: '{{ .tasks.attest.output.value }}'
  sbom: '{{ .tasks.sbom.output.value }}'
tasks:
  - id: attest
    uses: shell
    with: { op: eval, command: ["sh", "-c", "echo attest"] }
  - id: sbom
    uses: shell
    needs: [attest]
    with: { op: eval, command: ["sh", "-c", "echo sbom"] }
`;
}

function plain(version: string): string {
	return `apiVersion: orkstack/v1
kind: Orkflow
name: ${PLAIN}
version: "${version}"
inputs:
  who: { type: string, default: "world" }
tasks:
  - id: greet
    uses: shell
    with: { op: exec, command: ["sh", "-c", "echo hello ${version}"] }
`;
}

const checkoutYaml = `apiVersion: orkstack/v1
kind: Orkflow
name: ${CHECKOUT}
version: "4.2.0"
tasks:
  - id: release
    uses: orkflow:${BLOCK}@2.1.0
    with:
      image: ghcr.io/acme/checkout:4.2
      notes: from checkout
  - id: announce
    uses: shell
    needs: [release]
    condition: '{{ ne .tasks.release.output.url "" }}'
    with: { op: eval, command: ["sh", "-c", "echo {{ .tasks.release.output.digest }}"] }
`;

async function settle(page: Page, ready: string, failed: string, what: string): Promise<void> {
	const ok = page.getByTestId(ready);
	const panel = page.getByTestId(failed);
	const idleFailure = panel.locator('button:not([disabled])');
	await expect(ok.or(idleFailure).first()).toBeVisible();
	if (await idleFailure.isVisible()) {
		const said = (await panel.innerText()).replace(/\s+/g, ' ').trim();
		throw new Error(`${what} reported a failed read instead of loading: ${said}`);
	}
}

const versionsUrl = (name: string) => `${UI_ORIGIN}/orkflows/${name}?ns=${NS}&tab=versions`;
const openVersions = (page: Page) => settle(page, 'version-list', 'versions-error', 'the Versions tab');

type Catalog = { versions: { version: string; isCurrent?: boolean }[]; currentVersion?: string };
const catalog = (name: string) => rpc<Catalog>('OrkflowService/ListOrkflowVersions', { name, namespace: NS });

test.beforeAll(async () => {
	mkdirSync(PROOF_DIR, { recursive: true });
	writeFileSync(
		outPath(TRANSCRIPT),
		[
			'# The interface compatibility gate in the browser: verdict chips,',
			'# Compare interfaces, the promote acknowledgement, Callers, and the',
			'# apply warnings.',
			'#',
			'# Written by ui/e2e/interface-compat.spec.ts. Every line below was',
			'# produced by the run that wrote this file.',
			`# temporal namespace: ${TEMPORAL_NAMESPACE}`,
			`# ork namespace:      ${NS}`,
			`# orkflows:           ${BLOCK}, ${CHECKOUT}, ${PLAIN}`,
			`# engine:             ${ENGINE}`,
			''
		].join('\n')
	);

	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18307, 14307]) {
		if (await tcpOpen(port)) throw new Error(`port ${port} already in use (stray process?)`);
	}

	ensureActivityImages();

	// No Orker: nothing here submits a run, and the schedule is created paused.
	spawnLogged(outPath('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18307',
		ORK_WS_ALLOWED_ORIGINS: UI_ORIGIN
	});
	await waitForEngine(ENGINE);
	spawnLogged(
		outPath('ui.log'),
		'node',
		['build'],
		{
			HOST: '127.0.0.1',
			PORT: '14307',
			ENGINE_API_URL: ENGINE,
			PUBLIC_ENGINE_WS_URL: ENGINE_WS
		},
		uiDir
	);
	await waitFor('built ui', async () => (await fetch(`${UI_ORIGIN}/orkflows`)).ok);

	await rpc('NamespaceService/CreateNamespace', {
		namespace: { name: NS, description: 'interface compatibility gate' }
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: block('2.1.0'), namespace: NS });
	await rpc('OrkflowService/SetCurrentOrkflowVersion', { name: BLOCK, version: '2.1.0', namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: block('3.0.0'), namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: checkoutYaml, namespace: NS });
	await rpc('TriggerService/CreateTrigger', { namespace: NS, orkflowName: BLOCK, name: TRIGGER });
	await rpc('ScheduleService/CreateSchedule', {
		namespace: NS,
		name: SCHEDULE,
		orkflowName: BLOCK,
		orkflowVersion: '2.1.0',
		input: { data: { image: 'ghcr.io/acme/app:1' } },
		spec: { cron: ['0 3 * * *'] },
		paused: true
	});
	await rpc('OrkflowService/ApplyOrkflow', { yaml: plain('1.0.0'), namespace: NS });
	await rpc('OrkflowService/SetCurrentOrkflowVersion', { name: PLAIN, version: '1.0.0', namespace: NS });
	await rpc('OrkflowService/ApplyOrkflow', { yaml: plain('1.1.0'), namespace: NS });
	note(`applied ${BLOCK}@2.1.0 (current) and @3.0.0, ${CHECKOUT}@4.2.0 composing @2.1.0,`);
	note(`trigger ${TRIGGER} following current, paused schedule ${SCHEDULE} pinned to 2.1.0,`);
	note(`${PLAIN}@1.0.0 (current) and @1.1.0 with identical inputs and no outputs`);
});

test.afterAll(async () => {
	try {
		await rpc('ScheduleService/DeleteSchedule', { namespace: NS, name: SCHEDULE });
	} catch {
		// the schedule may never have been created; nothing to clean
	}
	await killStarted();
});

test('the versions tab marks a breaking version against the current one', async ({ page }) => {
	await page.goto(versionsUrl(BLOCK));
	await openVersions(page);
	await expect(page.getByTestId('version-interface-3.0.0')).toHaveText('breaking');
	await expect(page.getByTestId('version-interface-2.1.0')).toHaveCount(0);
	await expect(page.getByTestId('version-current-2.1.0')).toBeVisible();
	note('Versions tab: 3.0.0 is marked breaking; the current row 2.1.0 carries no chip');
	await shot(page, 'screenshot-versions-breaking-chip.png');
});

test('compare interfaces shows the findings the CLI prints', async ({ page }) => {
	await page.goto(versionsUrl(BLOCK));
	await openVersions(page);
	await page.getByTestId('diff-from-2.1.0').check();
	await page.getByTestId('diff-to-3.0.0').check();
	await page.getByTestId('compat-run').click();
	await expect(page.getByTestId('compat-result')).toBeVisible();
	await expect(page.getByTestId('compat-heading')).toContainText(`${BLOCK} 2.1.0 → 3.0.0`);
	await expect(page.getByTestId('compat-verdict')).toHaveText('breaking — 2 breaking changes');
	await expect(page.getByTestId('compat-finding-inputs.region-ADDED')).toBeVisible();
	await expect(page.getByTestId('compat-finding-outputs.url-REMOVED')).toBeVisible();
	await expect(page.getByTestId('compat-message-outputs.url-REMOVED')).toContainText(
		'output "url" is removed'
	);
	await page.getByTestId('compat-cli-trigger').click();
	await expect(page.getByTestId('compat-cli-command')).toHaveText(
		`orkctl --namespace '${NS}' 'orkflow' 'compat' '${BLOCK}' '2.1.0' '3.0.0'`
	);
	note('Compare interfaces 2.1.0 → 3.0.0: breaking — 2 breaking changes (inputs.region ADDED, outputs.url REMOVED)');
	await shot(page, 'screenshot-compare-interfaces.png');
});

test('callers lists the composing orkflow, the trigger and the schedule', async ({ page }) => {
	await page.goto(versionsUrl(BLOCK));
	await openVersions(page);
	await settle(page, 'callers-table', 'callers-error', 'the Callers section');
	await expect(page.getByTestId(`caller-orkflow-${CHECKOUT}-4.2.0-release`)).toBeVisible();
	await expect(page.getByTestId(`caller-orkflow-${CHECKOUT}-4.2.0-release`)).toContainText('digest, url');
	await expect(page.locator('[data-testid^="caller-trigger-"]')).toHaveCount(1);
	await expect(page.locator('[data-testid^="caller-trigger-"]')).toContainText(TRIGGER);
	await expect(page.locator('[data-testid^="caller-trigger-"]')).toContainText('current');
	await expect(page.getByTestId(`caller-schedule-${SCHEDULE}`)).toContainText('2.1.0');
	await expect(page.getByTestId('callers-scanned')).toContainText(
		'Moved by a promotion (they follow the current version): triggers 1, schedules 0.'
	);
	await page.getByTestId('callers-cli-trigger').click();
	await expect(page.getByTestId('callers-cli-command')).toHaveText(
		`orkctl --namespace '${NS}' 'orkflow' 'callers' '${BLOCK}'`
	);
	note(`Callers: ${CHECKOUT}@4.2.0 (reads digest, url), trigger ${TRIGGER} (current), schedule ${SCHEDULE} (2.1.0)`);
	await shot(page, 'screenshot-callers.png');

	// The promote dialog names the trigger that follows current.
	await page.getByTestId('promote-3.0.0').click();
	await expect(page.getByTestId('promote-followers')).toContainText(`trigger ${TRIGGER}`);
	await expect(page.getByTestId('promote-pinned')).toContainText('1 composing Orkflow version pins');
	await page.getByTestId('promote-cancel').click();
});

// Before the promotion below: 2.1.0 is current, and 3.0.0 is validated and
// applied from the Apply page. The apply is a repeat (3.0.0 is stored) and
// moves nothing; its warnings are the engine's comparison against 2.1.0.
test('apply shows breaking warnings against the current version', async ({ page }) => {
	await page.goto(`${UI_ORIGIN}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill(block('3.0.0'));
	await page.getByTestId('apply-validate').click();
	const issues = page.getByTestId('apply-validation-issues');
	await expect(issues).toBeVisible();
	for (const field of ['inputs.region', 'outputs.url', 'version']) {
		await expect(
			issues.locator(`[data-testid="apply-validation-issue"][data-field="${field}"][data-severity="warning"]`)
		).toHaveCount(1);
	}
	await expect(
		issues.locator('[data-testid="apply-validation-issue"][data-field="version"]')
	).toContainText(`breaks the declared interface of ${BLOCK}@2.1.0`);
	await shot(page, 'screenshot-apply-validate-warnings.png');

	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-success')).toContainText(`Applied ${BLOCK}@3.0.0`);
	// The warnings stay visible after the apply succeeded: the apply's own
	// warnings (one banner each) and the validation results above them.
	const applyWarnings = page.getByTestId('apply-warning');
	await expect(
		applyWarnings.filter({ hasText: `inputs.region: input "region" is added and required with no default` })
	).toHaveCount(1);
	await expect(applyWarnings.filter({ hasText: /^\s*outputs\.url: output "url" is removed/ })).toHaveCount(1);
	for (const field of ['inputs.region', 'outputs.url', 'version']) {
		await expect(
			issues.locator(`[data-testid="apply-validation-issue"][data-field="${field}"][data-severity="warning"]`)
		).toHaveCount(1);
	}
	expect((await catalog(BLOCK)).currentVersion).toBe('2.1.0');
	note('Apply page: validating 3.0.0 warns at inputs.region, outputs.url, version; apply succeeds, warnings stay, pointer stays 2.1.0');
	await shot(page, 'screenshot-apply-warnings-after.png');
});

test('a breaking promotion requires acknowledgement and then lands', async ({ page }) => {
	await page.goto(versionsUrl(BLOCK));
	await openVersions(page);
	await page.getByTestId('promote-3.0.0').click();
	await expect(page.getByTestId('promote-interface-verdict')).toContainText('breaking — 2 breaking changes');
	await expect(page.getByTestId('promote-finding-inputs.region-ADDED')).toBeVisible();
	await expect(page.getByTestId('promote-finding-outputs.url-REMOVED')).toBeVisible();
	await expect(page.getByTestId('promote-accept')).toBeDisabled();
	await shot(page, 'screenshot-promote-breaking-blocked.png');

	await page.getByTestId('promote-acknowledge').check();
	await expect(page.getByTestId('promote-accept')).toBeEnabled();
	await page.getByTestId('promote-cli-trigger').click();
	await expect(page.getByTestId('promote-cli-command')).toHaveText(
		`orkctl --namespace '${NS}' 'orkflow' 'promote' '${BLOCK}' '3.0.0' '--acknowledge-breaking'`
	);
	await page.getByTestId('promote-cli-trigger').click();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-3.0.0')).toBeVisible();
	const after = await catalog(BLOCK);
	expect(after.currentVersion).toBe('3.0.0');
	note('promote 3.0.0: accept disabled until acknowledged; landed; the engine lists current 3.0.0');
	await shot(page, 'screenshot-promote-breaking-landed.png');
});

test('an unknown promotion needs no acknowledgement', async ({ page }) => {
	await page.goto(versionsUrl(PLAIN));
	await openVersions(page);
	await expect(page.getByTestId('version-interface-1.1.0')).toHaveText('unknown');
	await page.getByTestId('promote-1.1.0').click();
	await expect(page.getByTestId('promote-interface-verdict')).toContainText('unknown — neither version declares outputs');
	await expect(page.getByTestId('promote-acknowledge')).toHaveCount(0);
	await expect(page.getByTestId('promote-accept')).toBeEnabled();
	await page.getByTestId('promote-accept').click();
	await expect(page.getByTestId('version-current-1.1.0')).toBeVisible();
	expect((await catalog(PLAIN)).currentVersion).toBe('1.1.0');
	note(`${PLAIN}: 1.1.0 is unknown against 1.0.0 and promotes with no acknowledgement`);
	await shot(page, 'screenshot-promote-unknown.png');
});
