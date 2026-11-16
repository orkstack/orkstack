import { expect, test, type Page } from './test';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdirSync, mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import https from 'node:https';
import os from 'node:os';
import path from 'node:path';
import {
	bin,
	clusterEnv,
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

// Sign-in to the web UI (UX35; docs/AUTHENTICATION.md, "Sign-in for the web
// UI") against a real enforcing engine and oidctest, the development identity
// provider: a visitor with no sign-in goes to /signin and lands where they
// were going; the account menu names them; a viewer sees no write control and
// is refused when it calls the RPC anyway; an admin's binding reaches a second
// browser within the refresh interval; disabling a principal ends its other
// browser's sign-in; Explain agrees with the server; a sign-in past its
// revalidation is refreshed under a form being written, which keeps what it
// holds; and signing out ends it.
const BUCKET = ownBucket('signin');
const PORT = 18761;
const IDP_PORT = 18762;
const ENGINE = `http://127.0.0.1:${PORT}`;
const IDP = `https://127.0.0.1:${IDP_PORT}`;
const TEMPORAL_NAMESPACE = temporalNamespace('signin');
const proof = proofOut(process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'signin') : path.join(uiDir, 'test-results', 'signin'));
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

test.use({ ignoreHTTPSErrors: true });

// This process asks oidctest, which serves its own CA, trusting that CA alone.
let idpCA = '';
function idpRequest(method: string, path0: string, body = ''): Promise<number> {
	return new Promise((resolve, reject) => {
		const req = https.request(`${IDP}${path0}`, { method, ca: idpCA }, (res) => {
			res.resume();
			resolve(res.statusCode ?? 0);
		});
		req.on('error', reject);
		req.end(body);
	});
}

async function asUser(email: string, groups: string[]): Promise<void> {
	const status = await idpRequest('POST', '/_control/user', JSON.stringify({ email, groups }));
	if (status !== 204) throw new Error(`oidctest did not take the user: ${status}`);
}

async function signIn(page: Page, path0: string): Promise<void> {
	await page.goto(`${ENGINE}${path0}`);
	await expect(page).toHaveURL(/\/signin\?return=/);
	await page.getByTestId('signin-issuer-people').click();
	await expect(page).toHaveURL(new RegExp(`${path0.replace(/[?]/g, '\\?')}$`));
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	for (const p of [PORT, IDP_PORT]) if (await tcpOpen(p)) throw new Error(`port ${p} already in use`);
	const dir = mkdtempSync(path.join(os.tmpdir(), 'signin-spec-'));
	const oidcDir = path.join(dir, 'oidc');
	spawnLogged(proof('oidctest.log'), bin('oidctest'), [
		'-dir', oidcDir, '-addr', `127.0.0.1:${IDP_PORT}`, '-redirect', `${ENGINE}/v1/auth/callback/people`,
		'-email', 'admin@example.test', '-groups', 'admins', '-control'
	], {});
	await waitFor('oidctest', async () => {
		try {
			idpCA = readFileSync(path.join(oidcDir, 'ca.pem'), 'utf8');
			return (await idpRequest('GET', '/.well-known/openid-configuration')) === 200;
		} catch {
			return false;
		}
	});
	const signinKey = path.join(dir, 'signin.key');
	writeFileSync(signinKey, randomBytes(32), { mode: 0o600 });
	const engineKey = path.join(dir, 'engine-signing.pem');
	writeFileSync(engineKey, generateKeyPairSync('ec', { namedCurve: 'P-256' }).privateKey.export({ type: 'pkcs8', format: 'pem' }), { mode: 0o600 });
	const secret = readFileSync(path.join(oidcDir, 'web-client.secret'), 'utf8').trim();
	spawnLogged(proof('engine.log'), bin('ork'), [], {
		...clusterEnv(TEMPORAL_NAMESPACE),
		...storeEnv(BUCKET),
		...uiEnv(),
		ORK_HTTP_ADDR: `127.0.0.1:${PORT}`,
		ORK_AUTH_MODE: 'enforce',
		ORK_AUTH_PUBLIC_URL: ENGINE,
		ORK_AUTH_ADMINS: 'group:people:admins',
		ORK_AUTH_POLICY_REFRESH: '1s',
		ORK_AUTH_SIGNIN_KEY_FILES: signinKey,
		ORK_AUTH_SIGNIN_REVALIDATE_INTERVAL: '4s',
		ORK_IDENTITY_SIGNING_KEY_FILES: engineKey,
		ORK_IDENTITY_SIGNING_ALGORITHM: 'ES256',
		SIGNIN_SPEC_WEB_SECRET: secret,
		ORK_AUTH_ISSUERS: [
			'issuers:',
			'  people:',
			`    issuer: ${IDP}`,
			'    kind: user',
			'    audiences: [orkstack-web, orkctl]',
			'    algorithms: [ES256]',
			'    username_claim: email',
			'    require_verified_email: true',
			'    groups_claim: groups',
			'    max_lifetime: 2h',
			`    ca_file: ${path.join(oidcDir, 'ca.pem')}`,
			'    login:',
			'      client_id: orkstack-web',
			'      client_secret:',
			'        source: env:SIGNIN_SPEC_WEB_SECRET',
			'      cli_client_id: orkctl',
			'      scopes: [openid, email, groups]'
		].join('\n')
	});
	await waitFor('the engine', async () => (await fetch(`${ENGINE}/signin`).catch(() => null))?.ok === true);
});

test.afterAll(async () => {
	await killStarted();
	removeBucket(BUCKET);
});

test('a visitor signs in, lands where they were going, and the account menu names them', async ({ page }) => {
	await asUser('admin@example.test', ['admins']);
	await page.goto(`${ENGINE}/signin`);
	await expect(page.getByTestId('signin-issuer-people')).toBeVisible();
	await shot(page, 'signin.png');
	await signIn(page, '/admin/access');
	await expect(page.getByTestId('access-title')).toBeVisible();
	await page.getByTestId('account-trigger').click();
	await expect(page.getByTestId('account-principal')).toHaveText('user:people:admin@example.test');
	await expect(page.getByTestId('account-roles')).toContainText('admin');
	// The panel is drawn in full, inside the viewport, beside its trigger.
	const box = await page.getByTestId('account-menu').boundingBox();
	const view = page.viewportSize();
	expect(box && view && box.width >= 240 && box.height >= 120 && box.y >= 0 && box.y + box.height <= view.height).toBe(true);
	await page.screenshot({ path: proof('signin-account-menu.png') });
});

test('a viewer sees no write control and is refused when it calls the RPC anyway; an admin’s binding reaches it', async ({ browser }) => {
	const admin = await browser.newContext({ ignoreHTTPSErrors: true });
	const viewer = await browser.newContext({ ignoreHTTPSErrors: true });
	const a = await admin.newPage();
	const v = await viewer.newPage();
	await asUser('admin@example.test', ['admins']);
	await signIn(a, '/admin/access');
	// The viewer signs in bound to nothing, and is told so.
	await asUser('vera@example.test', ['viewers']);
	await v.goto(`${ENGINE}/admin/access`);
	await v.getByTestId('signin-issuer-people').click();
	await expect(v.getByTestId('no-access-who')).toContainText('user:people:vera@example.test');
	await expect(v.getByTestId('no-access-who')).toContainText('viewers');
	await shot(v, 'signin-no-access.png');
	// The admin binds the viewers group to viewer at the deployment.
	await a.getByTestId('binding-new').click();
	await a.getByTestId('binding-name').fill('viewers');
	await a.getByTestId('binding-role').selectOption('viewer');
	await a.getByTestId('binding-scope').fill('deployment');
	await a.getByTestId('binding-subjects').fill('group:people:viewers');
	await a.getByTestId('binding-save').click();
	await expect(a.getByTestId('binding-viewers')).toBeVisible();
	await shot(a, 'signin-binding-created.png');
	// Within the engine's refresh interval the viewer's browser reaches Access.
	await waitFor('the binding reaches the viewer', async () => {
		await v.goto(`${ENGINE}/admin/access`);
		return v
			.getByTestId('access-title')
			.waitFor({ timeout: 2000 })
			.then(() => true, () => false);
	});
	await expect(v.getByTestId('binding-new')).toHaveAttribute('aria-disabled', 'true');
	await expect(v.getByText('You need access.write on this deployment, or access.bind on a namespace.')).toBeVisible();
	// A viewer holds no access.read: the page says what to ask for, not that something failed.
	await expect(v.getByTestId('access-error')).toHaveAttribute('data-refusal', 'permission');
	await expect(v.getByTestId('access-error')).toContainText('You need access.read on this deployment.');
	const refused = await v.evaluate(async () => {
		const res = await fetch('/orkstack.v1.AccessService/DeleteBinding', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"name":"viewers"}' });
		return res.status;
	});
	expect(refused).toBe(403);
	await shot(v, 'signin-viewer-access.png');

	// Disabling the viewer ends its sign-in in its own browser.
	await a.goto(`${ENGINE}/admin/access/principals`);
	await a.getByTestId('person-disable-user:people:vera@example.test').click();
	await a.getByTestId('person-confirm').click();
	await expect(a.getByTestId('person-disabled-user:people:vera@example.test')).toBeVisible();
	// Within the engine's refresh interval every call of the disabled viewer
	// is refused unauthenticated, and its page says its sign-in has ended.
	await waitFor('the disabled viewer is refused', async () => {
		const listed = await v.evaluate(async () => (await fetch('/orkstack.v1.RunService/ListRuns', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).status);
		return listed === 401;
	});
	await v.getByTestId('access-tab-Roles').click();
	await expect(v.getByTestId('signin-ended').or(v.getByTestId('signin-page'))).toBeVisible();
	await shot(v, 'signin-disabled-viewer.png');

	// Explain agrees with the server.
	await a.goto(`${ENGINE}/admin/access/explain`);
	await a.getByTestId('explain-principal').fill('user:people:vera@example.test');
	await a.getByTestId('explain-permission').selectOption('run.read');
	await a.getByTestId('explain-run').click();
	await expect(a.getByTestId('explain-denied')).toBeVisible();
	await shot(a, 'signin-explain.png');
	await admin.close();
	await viewer.close();
});

test('a sign-in past its revalidation is refreshed under a form being written, and signing out ends it', async ({ page }) => {
	await asUser('admin@example.test', ['admins']);
	await signIn(page, '/admin/access/explain');
	await page.getByTestId('explain-principal').fill('user:people:someone');
	// Past the 4 s revalidation interval the next call is refused stale, and
	// the page refreshes the sign-in and sends it again: the form keeps what
	// it holds and the call is answered. The deadline is the sign-in's own.
	const until = await page.evaluate(async () => {
		const res = await fetch('/orkstack.v1.AuthService/GetSignin', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
		return Date.parse(((await res.json()) as { signin: { validatedUntil: string } }).signin.validatedUntil);
	});
	await expect.poll(() => Date.now() > until + 500, { timeout: 15_000 }).toBe(true);
	await page.getByTestId('explain-run').click();
	await expect(page.getByTestId('explain-answer')).toBeVisible();
	await expect(page.getByTestId('explain-principal')).toHaveValue('user:people:someone');
	await expect(page.getByTestId('signin-ended')).toHaveCount(0);

	await page.getByTestId('account-trigger').click();
	await page.getByTestId('account-signout').click();
	await expect(page).toHaveURL(new RegExp(`^${IDP}/end-session`));
	await page.goto(`${ENGINE}/runs`);
	await expect(page).toHaveURL(/\/signin\?return=%2Fruns/);
});
