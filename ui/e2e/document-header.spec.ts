import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	killStarted,
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
import { ORKFLOW_PLACEHOLDER, automationPlaceholder } from '../src/lib/placeholders';

// The document header (docs/DSL.md, "The document header") through the UI:
// the Apply page's placeholder is a document that applies, header and all; a
// document without the header is refused by the engine and nothing is
// stored; an automation document is named for what it is; the version page's
// editor opens on the stored source with its header; and Admin → Automation's
// placeholder is the automation document's own header.
const ENGINE = 'http://127.0.0.1:18102';
const UI = 'http://127.0.0.1:14200';
const TEMPORAL_NAMESPACE = process.env.ORK_TEMPORAL_NAMESPACE ?? 'ork-ui-document-header';
const NS = `document-header-${Date.now().toString(36)}`;
const proof = proofOut(
	process.env.ORK_PROOF_DIR
		? path.join(process.env.ORK_PROOF_DIR, 'document-header')
		: path.join(uiDir, 'test-results', 'document-header')
);
const rpc = <T>(method: string, body: unknown) => callEngine<T>(ENGINE, method, body);
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

const HEADERLESS = `name: headerless
version: "0.1.0"
tasks:
  - id: fetch
    uses: http
    with:
      op: get
      url: "https://example.com"
`;

const AUTOMATION = `apiVersion: orkstack/v1
kind: Automation
namespace: ${NS}
`;

test.beforeAll(async () => {
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('MinIO is not reachable on 127.0.0.1:9000');
	for (const port of [18102, 14200]) if (await tcpOpen(port)) throw new Error(`port ${port} already in use`);
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...s3Env,
		ORK_HTTP_ADDR: '127.0.0.1:18102',
		ORK_WS_ALLOWED_ORIGINS: UI
	});
	await waitForEngine(ENGINE);
	spawnLogged(
		proof('ui.log'),
		'node',
		['build'],
		{ HOST: '127.0.0.1', PORT: '14200', ENGINE_API_URL: ENGINE, PUBLIC_ENGINE_WS_URL: ENGINE.replace('http', 'ws') },
		uiDir
	);
	await waitFor('built UI', async () => (await fetch(`${UI}/orkflows`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'document header' } });
});

test.afterAll(async () => {
	await killStarted();
});

test('the header is required, shown and offered by every authoring surface', async ({ page }) => {
	// 1. The Apply page's placeholder is a document that validates and applies.
	await page.goto(`${UI}/apply?ns=${NS}`);
	const editor = page.getByTestId('apply-yaml');
	await expect(editor).toHaveAttribute('placeholder', ORKFLOW_PLACEHOLDER);
	await editor.fill(ORKFLOW_PLACEHOLDER);
	await page.getByTestId('apply-validate').click();
	await expect(page.getByTestId('apply-validate')).toHaveText('Validate');
	await expect(page.getByTestId('apply-validation-issues')).toHaveCount(0);
	await expect(page.getByTestId('apply-error')).toHaveCount(0);
	await shot(page, 'document-header-apply.png');
	await page.getByTestId('apply-submit').click();
	await waitFor('the placeholder applied', async () => {
		const answer = await rpc<{ orkflows?: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
		return (answer.orkflows ?? []).some((wf) => wf.name === 'my-orkflow');
	});

	// 2. A document without the header is refused on apiVersion, at validate
	//    and at apply, and nothing is stored.
	await page.goto(`${UI}/apply?ns=${NS}`);
	await page.getByTestId('apply-yaml').fill(HEADERLESS);
	await page.getByTestId('apply-validate').click();
	const issue = page.getByTestId('apply-validation-issue');
	await expect(issue).toHaveCount(1);
	await expect(issue).toHaveAttribute('data-field', 'apiVersion');
	await expect(issue).toHaveText(
		'apiVersion: the document has no apiVersion or kind: an Orkflow begins with the two lines "apiVersion: orkstack/v1" and "kind: Orkflow" — add them at the top, after any leading comments (docs/DSL.md, "The document header")'
	);
	await page.getByTestId('apply-submit').click();
	await expect(page.getByTestId('apply-error')).toContainText('the document has no apiVersion or kind');
	const listed = await rpc<{ orkflows?: { name: string }[] }>('OrkflowService/ListOrkflows', { namespace: NS });
	expect((listed.orkflows ?? []).map((wf) => wf.name)).toEqual(['my-orkflow']);
	await shot(page, 'document-header-missing.png');

	// 3. An automation document is named for what it is, on kind.
	await page.getByTestId('apply-yaml').fill(AUTOMATION);
	await page.getByTestId('apply-validate').click();
	await expect(issue).toHaveAttribute('data-field', 'kind');
	await expect(issue).toContainText(
		'kind: Automation is an automation document, not an Orkflow: apply it with orkctl apply or Admin → Automation (docs/AUTOMATION.md); an Orkflow says kind: Orkflow'
	);

	// 4. The applied version's editor opens on its stored source, header first.
	await page.goto(`${UI}/orkflows/my-orkflow?ns=${NS}&tab=edit`);
	const source = page.getByTestId('editor-yaml');
	await expect(source).toBeVisible();
	await expect(source).toHaveValue(ORKFLOW_PLACEHOLDER);
	expect((await source.inputValue()).startsWith('apiVersion: orkstack/v1\nkind: Orkflow\n')).toBe(true);
	await shot(page, 'document-header-source.png');

	// 5. Admin → Automation offers the automation document's own header.
	await page.goto(`${UI}/admin/automation`);
	await expect(page.getByTestId('automation-yaml')).toHaveAttribute('placeholder', automationPlaceholder('default'));
});
