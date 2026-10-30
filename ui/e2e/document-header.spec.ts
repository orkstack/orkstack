import { expect, test, type Page } from './test';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import {
	bin,
	catalogEnv,
	clusterEnv,
	ensureActivityImages,
	ensureBucket,
	killStarted,
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
import { ORKFLOW_PLACEHOLDER, automationPlaceholder } from '../src/lib/placeholders';
import { editorReady, fillYaml, yamlValue } from './yaml';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('document-header');

// The document header (docs/DSL.md, "The document header") through the UI:
// the Apply page's placeholder is a document that applies, header and all; a
// document without the header is refused by the engine and nothing is
// stored; an automation document is named for what it is; the version page's
// editor opens on the stored source with its header; and Admin → Automation's
// placeholder is the automation document's own header.
const ENGINE = 'http://127.0.0.1:18102';
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('document-header');
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
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(18102)) throw new Error('port 18102 already in use');
	ensureActivityImages();
	const cluster = clusterEnv(TEMPORAL_NAMESPACE);
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...cluster,
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: '127.0.0.1:18102'
	});
	await waitForEngine(ENGINE);
	await waitFor('built UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
	await rpc('NamespaceService/CreateNamespace', { namespace: { name: NS, description: 'document header' } });
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('the header is required, shown and offered by every authoring surface', async ({ page }) => {
	// 1. The Apply page's placeholder is a document that validates and applies.
	await page.goto(`${UI}/apply?ns=${NS}`);
	await editorReady(page, 'apply-yaml');
	await expect(page.getByTestId('apply-yaml')).toHaveAttribute('aria-placeholder', ORKFLOW_PLACEHOLDER);
	await fillYaml(page, 'apply-yaml', ORKFLOW_PLACEHOLDER);
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
	await fillYaml(page, 'apply-yaml', HEADERLESS);
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
	await fillYaml(page, 'apply-yaml', AUTOMATION);
	await page.getByTestId('apply-validate').click();
	await expect(issue).toHaveAttribute('data-field', 'kind');
	await expect(issue).toContainText(
		'kind: Automation is an automation document, not an Orkflow: apply it with orkctl apply or Admin → Automation (docs/AUTOMATION.md); an Orkflow says kind: Orkflow'
	);

	// 4. The applied version's editor opens on its stored source, header first.
	await page.goto(`${UI}/orkflows/my-orkflow?ns=${NS}&tab=edit`);
	await editorReady(page, 'editor-yaml');
	const source = yamlValue(page, 'editor-yaml');
	await expect(source).toHaveValue(ORKFLOW_PLACEHOLDER);
	expect((await source.inputValue()).startsWith('apiVersion: orkstack/v1\nkind: Orkflow\n')).toBe(true);
	await shot(page, 'document-header-source.png');

	// 5. Admin → Automation offers the automation document's own header.
	// The layout restores the remembered namespace from localStorage and may
	// rewrite a bare URL as the page starts; name the namespace instead of
	// racing that restore (the behaviour is documented, the test was racy).
	await page.goto(`${UI}/admin/automation?ns=default`);
	await editorReady(page, 'automation-yaml');
	await expect(page.getByTestId('automation-yaml')).toHaveAttribute('aria-placeholder', automationPlaceholder('default'));
});
