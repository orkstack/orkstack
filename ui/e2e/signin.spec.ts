import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdirSync, mkdtempSync, writeFileSync, readFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { browserServerOwner } from './browser-server';
import {
	bin,
	clusterEnv,
	ensureBucket,
	killStarted,
	ownBucket,
	proofOut,
	removeBucket,
	repoRoot,
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
// UI") against a real enforcing engine and Dex, the development identity
// provider (deploy/dex/config.yaml's users, signed in through Dex's own login
// form; test/tools/dexrun starts it): a visitor with no sign-in goes to /signin and lands where they
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
const IDP = `https://127.0.0.1:${IDP_PORT}/dex`;
const TEMPORAL_NAMESPACE = temporalNamespace('signin');
const proof = proofOut(process.env.ORK_PROOF_DIR ? path.join(process.env.ORK_PROOF_DIR, 'signin') : path.join(uiDir, 'test-results', 'signin'));
const shot = (page: Page, name: string) => page.screenshot({ path: proof(name), fullPage: true });

test.use({ ignoreHTTPSErrors: true });

// Every user deploy/dex/config.yaml configures signs in with this password.
const PASSWORD = 'password';
let dexID = '';

/** signIn opens path0, is sent to /signin, and signs in at Dex as email. */
async function signIn(page: Page, path0: string, email: string): Promise<void> {
	await page.goto(`${ENGINE}${path0}`);
	await expect(page).toHaveURL(/\/signin\?return=/);
	await atDex(page, email);
	await expect(page).toHaveURL(new RegExp(`${path0.replace(/[?]/g, '\\?')}$`));
}

/** atDex follows the issuer's button and fills Dex's login form as email. */
async function atDex(page: Page, email: string): Promise<void> {
	await page.getByTestId('signin-issuer-people').click();
	await expect(page).toHaveURL(new RegExp(`^${IDP}/auth`));
	await page.locator('input[name="login"]').fill(email);
	await page.locator('input[name="password"]').fill(PASSWORD);
	await page.locator('button[type="submit"]').click();
}

test.beforeAll(async () => {
	ensureBucket(BUCKET);
	mkdirSync(proof('..'), { recursive: true });
	if (!(await temporalReachable())) throw new Error(`Temporal is not reachable on ${temporalAddress()}`);
	if (!(await tcpOpen(9000))) throw new Error('the object store is not reachable on 127.0.0.1:9000');
	for (const p of [PORT, IDP_PORT]) if (await tcpOpen(p)) throw new Error(`port ${p} already in use`);
	const dir = mkdtempSync(path.join(os.tmpdir(), 'signin-spec-'));
	// Dex, labelled with the proof (or this worker) it lives for, so a sweep
	// stops it should this worker die before afterAll.
	const started = JSON.parse(
		execFileSync(bin('dexrun'), [
			'start', '-dir', path.join(dir, 'dex'), '-addr', `127.0.0.1:${IDP_PORT}`, '-redirect', `${ENGINE}/v1/auth/callback/people`,
			'-proof', process.env.ORK_PROOF_ID ?? `e2e-${process.pid}`, '-owner', browserServerOwner()
		], { cwd: repoRoot, encoding: 'utf8' })
	) as { id: string; issuer: string; ca_file: string; secret_file: string };
	dexID = started.id;
	if (started.issuer !== IDP) throw new Error(`Dex's issuer is ${started.issuer}, not ${IDP}`);
	const signinKey = path.join(dir, 'signin.key');
	writeFileSync(signinKey, randomBytes(32), { mode: 0o600 });
	const engineKey = path.join(dir, 'engine-signing.pem');
	writeFileSync(engineKey, generateKeyPairSync('ec', { namedCurve: 'P-256' }).privateKey.export({ type: 'pkcs8', format: 'pem' }), { mode: 0o600 });
	const secret = readFileSync(started.secret_file, 'utf8').trim();
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
			'    algorithms: [RS256]',
			'    username_claim: email',
			'    require_verified_email: true',
			'    groups_claim: groups',
			'    max_lifetime: 2h',
			`    ca_file: ${started.ca_file}`,
			'    login:',
			'      client_id: orkstack-web',
			'      client_secret:',
			'        source: env:SIGNIN_SPEC_WEB_SECRET',
			'      cli_client_id: orkctl',
			'      scopes: [openid, email, groups, offline_access]'
		].join('\n')
	});
	await waitFor('the engine', async () => (await fetch(`${ENGINE}/signin`).catch(() => null))?.ok === true);
});

test.afterAll(async () => {
	await killStarted();
	try {
		if (dexID) execFileSync(bin('dexrun'), ['stop', dexID], { cwd: repoRoot });
	} finally {
		removeBucket(BUCKET);
	}
});

test('a visitor signs in, lands where they were going, and the account menu names them', async ({ page }) => {
	await page.goto(`${ENGINE}/signin`);
	await expect(page.getByTestId('signin-issuer-people')).toBeVisible();
	await shot(page, 'signin.png');
	await signIn(page, '/admin/access', 'admin@example.test');
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
	await signIn(a, '/admin/access', 'admin@example.test');
	// The viewer signs in bound to nothing, and is told so.
	await v.goto(`${ENGINE}/admin/access`);
	await atDex(v, 'vera@example.test');
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
	await signIn(page, '/admin/access/explain', 'admin@example.test');
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
	// Dex offers no end-session endpoint: signing out ends the sign-in here
	// and lands on /signin.
	await expect(page).toHaveURL(/\/signin$/);
	await page.goto(`${ENGINE}/runs`);
	await expect(page).toHaveURL(/\/signin\?return=%2Fruns/);
});
