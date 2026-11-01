import { expect, test, type Page } from './test';
import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
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
import { ORKFLOW_PLACEHOLDER } from '../src/lib/placeholders';
import { editorReady, fillYaml } from './yaml';

/** This spec's own bucket (harness.ts ownBucket), created in beforeAll and removed in afterAll. */
const BUCKET = ownBucket('security-headers');

// The UI's security headers (docs/SECURITY_MODEL.md, "Transport, listeners
// and configuration"): the page is sent under a strict content security
// policy — the one the build wrote into it, with the hash of each inline
// script it runs, and frame-ancestors — and the app still starts, draws,
// edits and calls the API under it, with nothing refused, while a script the
// build did not name is refused. And the listener answers its own address but
// refuses a request under a Host it does not know, which is what a page that
// reached it by DNS rebinding sends.
const PORT = 18871;
const ENGINE = `http://127.0.0.1:${PORT}`;
// The engine serves the UI (harness.ts uiEnv), so the page's origin is its own.
const UI = ENGINE;
const TEMPORAL_NAMESPACE = temporalNamespace('security-headers');
const proof = proofOut(
	process.env.ORK_PROOF_DIR
		? path.join(process.env.ORK_PROOF_DIR, 'security-headers')
		: path.join(uiDir, 'test-results', 'security-headers')
);
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

/** orkflowDoc is an Orkflow named name whose tasks: block is body. */
const orkflowDoc = (name: string, body: string) =>
	`apiVersion: orkstack/v1\nkind: Orkflow\nname: ${name}\nversion: "0.1.0"\ntasks:\n${body}`;

/** directive returns the sources a policy gives one directive. */
function directive(policy: string, name: string): string[] {
	for (const part of policy.split(';')) {
		const [key, ...sources] = part.trim().split(/\s+/);
		if (key === name) return sources;
	}
	return [];
}

/** get asks the engine for path with Host set to host, as a browser would. */
function get(host: string, urlPath: string): Promise<{ status: number; body: string }> {
	return new Promise((resolve, reject) => {
		const req = httpRequest({ host: '127.0.0.1', port: PORT, path: urlPath, headers: { host }, timeout: 30_000 }, (res) => {
			let body = '';
			res.setEncoding('utf8');
			res.on('data', (chunk) => (body += chunk));
			res.on('end', () => resolve({ status: res.statusCode ?? 0, body }));
		});
		req.on('timeout', () => req.destroy(new Error(`GET ${urlPath} with Host ${host}: no answer in 30s`)));
		req.on('error', reject);
		req.end();
	});
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	if (await tcpOpen(PORT)) throw new Error(`port ${PORT} already in use`);
	ensureActivityImages();
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...catalogEnv,
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: `127.0.0.1:${PORT}`
	});
	await waitForEngine(ENGINE);
	await waitFor('built UI', async () => (await fetch(`${UI}/_app/version.json`)).ok);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('the UI runs under a strict content security policy, and only the scripts its build names run', async ({
	page,
	cspViolations
}) => {
	// Every refusal the browser makes, on every page this test opens, is
	// recorded by the shared fixture (./test.ts), which fails any spec that
	// leaves one it did not declare; this test declares its positive control.

	// 1. The page's policy: the build's own, sent as a header, with
	//    frame-ancestors. Scripts come from the page's origin only, with no
	//    inline script but the hashed ones; eval is the editor's (ajv).
	const response = await page.goto(`${UI}/?ns=default`);
	const policy = response?.headers()['content-security-policy'] ?? '';
	writeFileSync(proof('policy.txt'), `${policy}\n`);
	expect(policy.startsWith("frame-ancestors 'none'; ")).toBe(true);
	expect(directive(policy, 'default-src')).toEqual(["'self'"]);
	const scripts = directive(policy, 'script-src');
	expect(scripts.filter((s) => !s.startsWith("'sha256-"))).toEqual(["'self'", "'unsafe-eval'"]);
	expect(directive(policy, 'object-src')).toEqual(["'none'"]);
	expect(directive(policy, 'base-uri')).toEqual(["'none'"]);
	expect(directive(policy, 'connect-src')).toEqual(["'self'"]);
	// Styles may be inline: app.html wraps the app in a styled element, Svelte
	// renders style attributes from its templates, and the YAML editor
	// (CodeMirror) inserts its own <style> element at run time. Images may be
	// data: URLs, which the editor draws its markers with. Neither can run a
	// script.
	expect(directive(policy, 'style-src')).toEqual(["'self'", "'unsafe-inline'"]);
	expect(directive(policy, 'img-src')).toEqual(["'self'", 'data:']);
	expect(directive(policy, 'font-src')).toEqual(["'self'"]);
	expect(directive(policy, 'form-action')).toEqual(["'self'"]);
	const meta = await page.locator('meta[http-equiv="content-security-policy"]').getAttribute('content');
	expect(policy).toBe(`frame-ancestors 'none'; ${meta}`);
	expect(response?.headers()['x-frame-options']).toBe('DENY');

	// 2. Every inline script the page holds — app.html's theme script and
	//    SvelteKit's boot script — is named by its hash, and nothing more is.
	const inline = await page.evaluate(() =>
		Array.from(document.querySelectorAll('script:not([src])')).map((s) => s.textContent ?? '')
	);
	// The page has inline scripts at all, so the two checks below see some.
	expect(inline.length).toBeGreaterThan(0);
	const hashes = new Set(inline.map((s) => `'sha256-${createHash('sha256').update(s).digest('base64')}'`));
	const named = new Set(scripts.filter((s) => s.startsWith("'sha256-")));
	// No inline script runs unnamed, and the policy names no script the page
	// does not hold.
	expect([...hashes].filter((hash) => !named.has(hash))).toEqual([]);
	expect([...named].filter((hash) => !hashes.has(hash))).toEqual([]);
	await expect(page.locator('html')).toHaveAttribute('data-theme', /^(light|dark)$/);
	await shot(page, 'security-headers-home.png');

	// 3. The app works under it. It draws a list from the API. The editor,
	//    which styles itself inline and compiles its schema checks at run
	//    time (the reason for 'unsafe-eval'), flags an unknown key as it is
	//    typed (src/lib/yaml/lint.ts) and completes an http task's `with:`
	//    keys, which needs the task's `if uses: http` condition compiled
	//    (src/lib/yaml/complete.ts). And a validation round-trips.
	await page.goto(`${UI}/orkflows?ns=default`);
	await expect(page.getByTestId('orkflows-empty').first()).toBeVisible();
	await page.goto(`${UI}/apply?ns=default`);
	await editorReady(page, 'apply-yaml');
	await fillYaml(page, 'apply-yaml', orkflowDoc('csp-lint', '  - id: fetch\n    need: [setup]\n    uses: http\n    with:\n      url: https://example.com\n'));
	const finding = page.locator('[data-testid="apply-yaml-finding"][data-line="7"]');
	await expect(finding).toHaveAttribute('data-source', 'schema');
	await expect(finding).toContainText('unknown key need');
	await fillYaml(page, 'apply-yaml', orkflowDoc('csp-complete', '  - id: fetch\n    uses: http\n    with:\n      url: https://example.com\n'));
	await page.locator('.cm-line').nth(8).click();
	await page.keyboard.press('End');
	await page.keyboard.press('Enter');
	await page.keyboard.type('captu');
	await expect(page.locator('.cm-tooltip-autocomplete li', { hasText: 'captureBodyAs' })).toBeVisible();
	await shot(page, 'security-headers-completion.png');
	await page.keyboard.press('Escape');
	await fillYaml(page, 'apply-yaml', ORKFLOW_PLACEHOLDER);
	const validated = page.waitForResponse((r) => r.url().endsWith('/orkstack.v1.OrkflowService/ValidateOrkflow'));
	await page.getByTestId('apply-validate').click();
	expect((await validated).ok()).toBe(true);
	await expect(page.getByTestId('apply-validate')).toHaveText('Validate');
	await expect(page.getByTestId('apply-validation-issues')).toHaveCount(0);
	await expect(page.getByTestId('apply-error')).toHaveCount(0);
	await shot(page, 'security-headers-apply.png');
	// The run and task-log sockets ride connect-src 'self'; the run page
	// specs open them under this same policy.
	expect(cspViolations.unexpected).toEqual([]);

	// 4. The positive control: an inline script the build did not name is
	//    refused, and does not run. Declared first, since both policies on
	//    the page (the header and the meta tag) report it.
	cspViolations.expect(/^\/apply: script-src-elem inline$/);
	await page.evaluate(() => {
		const script = document.createElement('script');
		script.textContent = 'window.injected = true';
		document.head.appendChild(script);
	});
	await expect.poll(() => cspViolations.expected.length).toBeGreaterThan(0);
	expect(await page.evaluate(() => (window as unknown as { injected?: boolean }).injected)).toBeUndefined();
});

// The shared fixture fails a spec that leaves a refusal it did not declare:
// this test leaves one and is expected to fail. It proves the fixture is
// wired into every spec; what it decides, and the message it fails with,
// are the UI unit suite's (test/csp.test.ts).
test('a refusal no spec accounts for fails the spec', async ({ page, cspViolations }) => {
	test.fail();
	await page.goto(`${UI}/?ns=default`);
	await page.evaluate(() => {
		const script = document.createElement('script');
		script.textContent = 'window.injected = true';
		document.head.appendChild(script);
	});
	await expect.poll(() => cspViolations.unexpected.length).toBeGreaterThan(0);
});

test('the listener answers its own address and refuses a Host it does not know', async () => {
	for (const host of [`127.0.0.1:${PORT}`, `localhost:${PORT}`]) {
		expect((await get(host, '/')).status, `Host ${host}`).toBe(200);
	}
	const transcript: string[] = [];
	for (const [host, urlPath] of [
		[`evil.example:${PORT}`, '/'],
		[`evil.example:${PORT}`, '/_app/version.json'],
		[`rebound.example.com:${PORT}`, '/runs']
	]) {
		const refused = await get(host, urlPath);
		transcript.push(`GET ${urlPath} Host: ${host} -> ${refused.status} ${refused.body.trim()}`);
		expect(refused.status, `Host ${host}`).toBe(403);
		expect(refused.body).toContain('ORK_ALLOWED_HOSTS');
	}
	writeFileSync(proof('refused-hosts.txt'), `${transcript.join('\n')}\n`);
});
