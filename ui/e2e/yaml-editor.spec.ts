import { expect, test, type Page } from './test';
import AxeBuilder from '@axe-core/playwright';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
	noteTo,
	ownBucket,
	proofOut,
	removeBucket,
	rpc as callEngine,
	spawnLogged,
	storeEnv,
	tcpOpen,
	temporalAddress,
	temporalNamespace,
	temporalReachable,
	uiDir,
	uiEnv,
	waitFor,
	waitForEngine
} from './harness';
import { editorReady, fillYaml, yamlValue } from './yaml';

// The YAML editor (ui/src/lib/yaml/) against a real engine: a finding is
// placed on the line it is about, from each of its three sources — the
// generated schema as the document is typed, the strict YAML-to-JSON mapping
// and the DAG's own rules from the engine once typing pauses — and the editor
// says a document has no problems only when the engine said so. Then the
// keyboard: Tab indents, Escape then Tab leaves; completion offers the
// schema's keys; and the automation document is held to its own schema.

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('yaml-editor');
const ENGINE = 'http://127.0.0.1:18479';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('yaml-editor');
const NS = `yaml-editor-${Date.now().toString(36)}`;
const proof = proofOut(
	process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'yaml-editor') : path.join(uiDir, 'test-results', 'yaml-editor')
);
const note = noteTo(proof('transcript.txt'));
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

const doc = (name: string, body: string) => `apiVersion: orkstack/v1\nkind: Orkflow\nname: ${name}\nversion: "0.1.0"\ntasks:\n${body}`;

// Line 7 is `    need: [setup]`: a key the task schema does not have.
const UNKNOWN_KEY = doc('yaml-editor-unknown', '  - id: fetch\n    need: [setup]\n    uses: http\n    with:\n      url: https://example.com\n');
// Line 12 is `        enabled: yes`: data the strict mapping refuses (yes is a
// boolean to YAML 1.1 and a string to YAML 1.2), which only the engine sees.
const STRICT = doc(
	'yaml-editor-strict',
	'  - id: notify\n    uses: http\n    with:\n      url: https://example.com\n      op: post\n      body:\n        enabled: yes\n'
);
// Line 8 is `    needs: [setup]`: a well-shaped document whose need names no task.
const SEMANTIC = doc('yaml-editor-needs', '  - id: fetch\n    uses: http\n    needs: [setup]\n    with:\n      url: https://example.com\n');
const VALID = doc('yaml-editor-valid', '  - id: fetch\n    uses: http\n    with:\n      url: "https://example.com/{{ .inputs.day }}"\n').replace(
	'tasks:\n',
	'inputs:\n  day:\n    type: string\n    default: monday\ntasks:\n'
);

const status = (page: Page, testid = 'apply-yaml') => page.getByTestId(`${testid}-status`);

/** The text of each underlined range on a 1-based line, as drawn. */
function underlined(page: Page, line: number): Promise<string[]> {
	return page.locator('.cm-line').nth(line - 1).locator('.cm-lintRange-error, .cm-lintRange-warning').allInnerTexts();
}

/** The position button of the list entry holding `entry`. */
const positionOf = (page: Page, entry: ReturnType<Page['locator']>) => page.locator('li').filter({ has: entry }).getByRole('button');

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18479)) throw new Error('port 18479 already in use');
	ensureActivityImages();
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		ORK_HTTP_ADDR: '127.0.0.1:18479',
		...uiEnv()
	});
	await waitForEngine(ENGINE);
	await waitFor('built UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'yaml editor' } });
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('an unknown key is flagged on its line, by the schema as it is typed and by the engine', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', UNKNOWN_KEY);

	// The editor's own finding: line 7, column 5, the key itself underlined.
	const finding = page.locator('[data-testid="apply-yaml-finding"][data-line="7"]');
	await expect(finding).toBeVisible();
	await expect(finding).toContainText('unknown key need');
	await expect(finding).toContainText('did you mean needs?');
	await expect(finding).toHaveAttribute('data-source', 'schema');
	await expect(positionOf(page, finding)).toHaveText('7:5');
	expect(await underlined(page, 7)).toContain('need');
	await expect(page.locator('.cm-gutter-lint .cm-lint-marker-error').first()).toBeVisible();

	// The engine's own refusal, placed on the same line.
	const engine = page.getByTestId('apply-validation-issue').first();
	await expect(engine).toBeVisible();
	await expect(positionOf(page, engine)).toHaveText(/^7:\d+$/);
	await expect(status(page)).toHaveAttribute('data-tone', 'problems');

	// A finding's position moves the cursor there.
	await positionOf(page, finding).click();
	await expect(page.getByTestId('apply-yaml')).toBeFocused();
	note(`unknown key: ${await finding.innerText()} | engine: ${await engine.innerText()}`);

	// Hovering a key shows what the schema says about it.
	await page.locator('.cm-line').nth(7).getByText('uses').hover();
	await expect(page.locator('.cm-schema-doc')).toContainText('uses');
});

test('a strict-mapping refusal, which only the engine can see, lands on its line', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', STRICT);
	const issue = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].with.body.enabled"]');
	await expect(issue).toBeVisible();
	await expect(issue).toContainText('yes is a boolean to YAML 1.1 readers');
	await expect(positionOf(page, issue)).toHaveText('12:18');
	expect(await underlined(page, 12)).toEqual(['yes']);
	// The schema has nothing to say about it, and the editor does not pretend to.
	await expect(page.getByTestId('apply-yaml-finding')).toHaveCount(0);
	await expect(status(page)).toHaveAttribute('data-tone', 'problems');
	await expect(status(page)).toContainText('The engine refuses this document');
	note(`strict mapping: ${await issue.innerText()}`);
});

test('a semantic error only the engine finds is placed on the node its field names', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', SEMANTIC);
	const issue = page.locator('[data-testid="apply-validation-issue"][data-field="tasks[0].needs[0]"]');
	await expect(issue).toBeVisible();
	await expect(positionOf(page, issue)).toHaveText('8:13');
	expect(await underlined(page, 8)).toEqual(['setup']);
	await expect(page.getByTestId('apply-yaml-finding')).toHaveCount(0);
	note(`semantic: ${await issue.innerText()}`);
});

test('a valid document shows clean, and only once the engine has said so', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', VALID);
	const line = status(page);
	await expect(line).toHaveAttribute('data-tone', 'clean');
	await expect(line).toHaveText(
		'The engine finds no problems in this document. Apply also refuses a version already stored with other content.'
	);
	await expect(page.getByTestId('apply-yaml-finding')).toHaveCount(0);
	await expect(page.getByTestId('apply-validation-issues')).toHaveCount(0);
	await expect(page.locator('.cm-lintRange-error, .cm-lintRange-warning')).toHaveCount(0);
	// The template is coloured as a template, inside its string.
	await expect(page.locator('.cm-line .syntax-template')).toHaveText('{{ .inputs.day }}');
	// One edit, and the earlier answer no longer speaks for the document: the
	// status leaves "clean" at once and returns only with a new answer.
	await page.locator('.cm-line').nth(2).click();
	await page.keyboard.press('End');
	await page.keyboard.type('-2');
	await expect(line).toHaveAttribute('data-tone', 'checking');
	await expect(line).toHaveAttribute('data-tone', 'clean');
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/name: yaml-editor-valid-2/);
});

test('the editor in both themes: findings, colours and the list (screenshots)', async ({ page }) => {
	await page.setViewportSize({ width: 1280, height: 900 });
	const withTemplate = UNKNOWN_KEY.replace('url: https://example.com', 'url: "https://example.com/{{ .inputs.day }}"\n      op: fetch');
	for (const theme of ['light', 'dark'] as const) {
		await page.goto(`${UI}/apply?ns=${NS}`);
		await page.getByTestId(`theme-${theme}`).click();
		await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
		await fillYaml(page, 'apply-yaml', withTemplate);
		await expect(page.getByTestId('apply-validation-issue').first()).toBeVisible();
		await expect(page.getByTestId('apply-yaml-finding')).toHaveCount(2);
		// Every colour drawn is the token's, in this theme.
		const colours = await page.evaluate(() => {
			const css = getComputedStyle(document.documentElement);
			const token = (name: string) => {
				const probe = document.createElement('span');
				probe.style.color = css.getPropertyValue(name);
				document.body.appendChild(probe);
				const c = getComputedStyle(probe).color;
				probe.remove();
				return c;
			};
			const of = (selector: string) => getComputedStyle(document.querySelector(selector)!).color;
			return {
				key: [of('.cm-line .syntax-key'), token('--color-syntax-key')],
				string: [of('.cm-line .syntax-string'), token('--color-syntax-string')],
				template: [of('.cm-line .syntax-template'), token('--color-syntax-template')],
				ground: [getComputedStyle(document.querySelector('.cm-editor')!).backgroundColor, token('--color-paper')],
				gutter: [getComputedStyle(document.querySelector('.cm-gutters')!).backgroundColor, token('--color-paper-sunken')],
				mark: [getComputedStyle(document.querySelector('.cm-lint-marker-error')!).backgroundColor, token('--color-red')]
			};
		});
		for (const [role, [drawn, token]] of Object.entries(colours)) expect(`${theme} ${role}: ${drawn}`).toBe(`${theme} ${role}: ${token}`);
		await page.locator('.cm-line').nth(6).getByText('need').hover();
		await expect(page.locator('.cm-tooltip-lint')).toBeVisible();
		await shot(page, `yaml-editor-${theme}.png`);
		const axe = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
		const found = axe.violations.map((v) => `${v.id} (${v.nodes.length}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(', ')})`);
		note(`axe apply with findings (${theme}): ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
		expect(found).toEqual([]);
	}
});

test('Tab indents, Escape then Tab leaves the editor, and completion offers the schema’s keys', async ({ page }) => {
	await page.goto(`${UI}/apply?ns=${NS}`);
	await fillYaml(page, 'apply-yaml', doc('yaml-editor-keys', '  - id: fetch\n    uses: http\n    with:\n      url: https://example.com\n'));
	const editor = page.getByTestId('apply-yaml');
	// Tab is the editor's: it indents, and focus stays.
	await page.locator('.cm-line').nth(6).click();
	await page.keyboard.press('Home');
	await page.keyboard.press('Tab');
	await expect(editor).toBeFocused();
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/\n {6}uses: http/);
	await page.keyboard.press('Shift+Tab');
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/\n {4}uses: http/);
	// Ctrl-Shift-Space explains the key on the caret's line, in a popup and
	// to assistive technology.
	await page.keyboard.press('Control+Shift+Space');
	await expect(page.locator('.cm-schema-doc')).toContainText('uses');
	await expect(page.getByTestId('apply-yaml-announce')).toContainText('uses');
	await page.keyboard.press('Escape');
	await expect(page.locator('.cm-schema-doc')).toHaveCount(0);
	// Escape, then Tab: the keyboard leaves the editor for the next control,
	// Import YAML, whose label shows the focus.
	await page.keyboard.press('Escape');
	await page.keyboard.press('Tab');
	await expect(editor).not.toBeFocused();
	expect(await page.evaluate(() => document.activeElement?.closest('.cm-editor') === null)).toBe(true);
	await expect(page.getByTestId('apply-import')).toBeFocused();
	const outline = await page.getByTestId('apply-import').evaluate((input) => getComputedStyle(input.closest('label')!).outlineStyle);
	expect(outline).toBe('solid');

	// Completion: a new key in the task, from the task's schema; then its words.
	await page.locator('.cm-line').nth(6).click();
	await page.keyboard.press('End');
	await page.keyboard.press('Enter');
	// An option is chosen by clicking it: CodeMirror ignores Enter for a moment
	// after the list opens, so a fast typist never accepts by accident.
	await page.keyboard.type('jo');
	await expect(page.locator('.cm-tooltip-autocomplete li', { hasText: 'join' })).toBeVisible();
	// The field and its popups (CodeMirror mounts them inside the editor); the
	// page around it was scanned whole above, at its top, and is scrolled here.
	const axe = await new AxeBuilder({ page }).include('.yaml-field').withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
	const found = axe.violations.map((v) => `${v.id} (${v.nodes.length}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(', ')})`);
	note(`axe editor with the completion list open: ${found.length} violation(s)${found.length ? ' — ' + found.join('; ') : ''}`);
	expect(found).toEqual([]);
	await page.locator('.cm-tooltip-autocomplete li', { hasText: 'join' }).click();
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/\n {4}join: \n/);
	await page.keyboard.type('an');
	await page.locator('.cm-tooltip-autocomplete li', { hasText: 'any_failed' }).click();
	await expect(yamlValue(page, 'apply-yaml')).toHaveValue(/\n {4}join: any_failed\n/);
});

test('the Orkflow edit tab and the automation document use the same editor, each with its own schema', async ({ page }) => {
	// The edit tab: a stored version opens in the editor, coloured, and clean.
	await rpc('OrkflowService/ApplyOrkflow', { yaml: VALID, namespace: NS });
	await page.goto(`${UI}/orkflows/yaml-editor-valid?ns=${NS}&tab=edit`);
	await editorReady(page, 'editor-yaml');
	await expect(yamlValue(page, 'editor-yaml')).toHaveValue(VALID);
	await expect(page.locator('.cm-line .syntax-template')).toHaveText('{{ .inputs.day }}');
	await expect(status(page, 'editor-yaml')).toHaveAttribute('data-tone', 'clean');

	// Admin → Automation: an automation document, checked against the
	// automation schema, and its export coloured as YAML.
	await page.goto(`${UI}/admin/automation?ns=${NS}`);
	const automation = `apiVersion: orkstack/v1\nkind: Automation\nnamespace: ${NS}\nschedules:\n  nightly:\n    orkflow: yaml-editor-valid\n    cron: "@daily"\n    overlap: sometimes\n`;
	await fillYaml(page, 'automation-yaml', automation);
	await expect(page.locator('[data-testid="automation-yaml-finding"][data-line="8"]')).toContainText('must be one of skip');
	await page.getByTestId('automation-export').click();
	await expect(page.getByTestId('automation-export-yaml').locator('.syntax-key').first()).toHaveText('apiVersion');
	await shot(page, 'yaml-editor-automation.png');
});
