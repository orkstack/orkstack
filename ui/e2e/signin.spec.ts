import { expect, test, type Page } from './test';
import { execFileSync } from 'node:child_process';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdirSync, mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
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

	// The admin finds that refusal in the audit trail in one click: Denied
	// narrows it to what access control refused, and pressed again lets go.
	await a.goto(`${ENGINE}/admin/audit`);
	const denied = a.getByTestId('audit-shortcut-denied');
	await expect(denied).not.toHaveAttribute('aria-current');
	await denied.click();
	await expect(a).toHaveURL(/filter=decision%3Ddenied/);
	await expect(a.getByTestId('audit-shortcut-denied')).toHaveAttribute('aria-current', 'true');
	const refusal = a.getByTestId('audit-row').filter({ hasText: 'user:people:vera@example.test' });
	await expect(refusal.first()).toBeVisible();
	await expect(refusal.first().getByTestId('audit-decision')).toContainText('Denied');
	await expect(a.getByTestId('audit-row').getByTestId('audit-decision').filter({ hasNotText: 'Denied' })).toHaveCount(0);
	// A long refusal reads as lines, never a word a line beside the fixed columns.
	const target = await refusal.first().getByTestId('audit-target').boundingBox();
	expect(target && target.width >= 280).toBe(true);
	await shot(a, 'signin-audit-denied.png');
	await a.getByTestId('audit-shortcut-denied').click();
	await expect(a.getByTestId('audit-shortcut-denied')).not.toHaveAttribute('aria-current');
	await expect(a).not.toHaveURL(/decision/);
	// Every principal is verified under enforce: nothing was asserted.
	await a.getByTestId('audit-shortcut-unverified').click();
	await expect(a).toHaveURL(/filter=principal_verified%3Dfalse/);
	await expect(a.getByTestId('audit-empty')).toBeVisible();

	// Disabling the viewer ends its sign-in in its own browser.
	await a.goto(`${ENGINE}/admin/access/principals`);
	// People lists who signed in over the window, newest first, with when.
	const vera = 'user:people:vera@example.test';
	await expect(a.getByTestId('people-window-30d')).toHaveAttribute('aria-current', 'page');
	await expect(a.getByTestId(`person-seen-at-${vera}`)).toBeVisible();
	await expect(a.getByTestId('people-seen-error')).toHaveCount(0);
	await expect(a.getByTestId('people-seen-stepped')).toHaveCount(0);
	await shot(a, 'signin-people.png');
	// An audit trail too large to read for 30 days in one request: the page
	// steps down to 7 days by itself and says so. 5001 unrelated records, ten
	// days back, are one more than the engine examines in one step
	// (artifacts.AuditScanMaxObjects), written through the store's own code.
	execFileSync(bin('audit-seed'), ['5001', 'namespace.update', '10'], { cwd: repoRoot, env: { ...process.env, ...storeEnv(BUCKET) } });
	await a.reload();
	await expect(a.getByTestId('people-seen-stepped')).toHaveText('Sign-ins of the last 7 days; 30 days examines more audit records than one request may.');
	await expect(a.getByTestId('people-window-7d')).toHaveAttribute('aria-current', 'page');
	await expect(a.getByTestId(`person-seen-at-${vera}`)).toBeVisible();
	await expect(a.getByTestId('people-seen-error')).toHaveCount(0);
	await shot(a, 'signin-people-stepped-down.png');
	// Asked for 24 hours, it reads 24 hours and says nothing more.
	await a.getByTestId('people-window-1d').click();
	await expect(a).toHaveURL(/window=1d/);
	await expect(a.getByTestId('people-window-1d')).toHaveAttribute('aria-current', 'page');
	await expect(a.getByTestId('people-seen-stepped')).toHaveCount(0);
	await expect(a.getByTestId(`person-seen-at-${vera}`)).toBeVisible();
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
	// and lands on /signin. The URL is read once the page is drawn, after
	// anything the shell's effects would write into it: the sign-in pages
	// name no namespace.
	await expect(page.getByTestId('signin-issuer-people')).toBeVisible();
	await expect(page).toHaveURL(/\/signin$/);
	await page.goto(`${ENGINE}/runs`);
	await expect(page.getByTestId('signin-issuer-people')).toBeVisible();
	await expect(page).toHaveURL(/\/signin\?return=%2Fruns$/);
});

// The identity screens as they are drawn, in light and dark and at a
// phone's width, each once its own content is in; and what the design review
// changed, proven where it is drawn: a person with no access copies what an
// administrator needs, a new key's private half is saved before its public
// half is stored, under the id the engine then names it by, and disabling
// yourself says it is you.
const ACCESS_SCREENS = [
	{ path: '/admin/access', name: 'bindings', ready: 'binding-new' },
	{ path: '/admin/access/roles', name: 'roles', ready: 'role-new' },
	{ path: '/admin/access/service-accounts', name: 'service-accounts', ready: 'sa-new' },
	{ path: '/admin/access/principals', name: 'people', ready: 'people-table' },
	{ path: '/admin/access/explain', name: 'explain', ready: 'explain-run' },
	{ path: '/admin/access/history', name: 'history', ready: 'history-table' }
] as const;

test('every identity screen is drawn in light, dark and at a phone’s width, and says what a change does to you', async ({ browser }) => {
	// Four browsers, each signing in twice through Dex, and one key made and stored.
	test.setTimeout(240_000);
	for (const scheme of ['light', 'dark'] as const) {
		for (const [width, height, tag] of [[1440, 900, 'wide'], [390, 844, 'phone']] as const) {
			const first = scheme === 'light' && tag === 'wide';
			const ctx = await browser.newContext({ ignoreHTTPSErrors: true, colorScheme: scheme, viewport: { width, height }, acceptDownloads: true, permissions: first ? ['clipboard-read', 'clipboard-write'] : [] });
			const page = await ctx.newPage();
			const at = `${scheme}-${tag}`;
			await page.goto(`${ENGINE}/signin`);
			await expect(page.getByTestId('signin-issuer-people')).toBeVisible();
			await shot(page, `screens/${at}-signin.png`);
			// A person bound to nothing is told so.
			await page.goto(`${ENGINE}/runs`);
			await atDex(page, 'dev@example.test');
			await expect(page.getByTestId('no-access-who')).toContainText('user:people:dev@example.test');
			// A bare page: drawn, its URL still names no namespace.
			await expect(page).toHaveURL(/\/signin\/no-access$/);
			await shot(page, `screens/${at}-no-access.png`);
			if (first) {
				await page.getByTestId('no-access-copy').click();
				await expect(page.getByTestId('no-access-copied')).toHaveText('Copied.');
				expect(await page.evaluate(() => navigator.clipboard.readText())).toBe('Principal: user:people:dev@example.test\nSigned in with: people\nGroups: dev-admins');
			}
			await ctx.clearCookies();
			await signIn(page, '/admin/access', 'admin@example.test');
			for (const s of ACCESS_SCREENS) {
				await page.goto(`${ENGINE}${s.path}`);
				await expect(page.getByTestId(s.ready)).toBeVisible();
				await shot(page, `screens/${at}-access-${s.name}.png`);
			}
			if (first) {
				// A key is offered, again on asking, and stored only once saved.
				await page.goto(`${ENGINE}/admin/access/service-accounts`);
				await page.getByTestId('sa-new').click();
				await page.getByTestId('sa-name').fill('deployer');
				await page.getByTestId('sa-save').click();
				await expect(page.getByTestId('sa-key-store')).toBeDisabled();
				await expect(page.getByTestId('sa-key-confirm')).toBeDisabled();
				const kid = (await page.getByTestId('sa-key-kid').innerText()).replace(/^Key\s*/, '').trim();
				expect(kid).toMatch(/^[A-Za-z0-9_-]{43}$/);
				const offered = page.waitForEvent('download');
				await page.getByTestId('sa-key-download').click();
				const file = await offered;
				expect(file.suggestedFilename()).toBe(`deployer-${kid.slice(0, 12)}.pem`);
				// saveAs, not path(): the browser runs behind a browser server.
				const saved = proof(`screens/${file.suggestedFilename()}`);
				await file.saveAs(saved);
				try {
					expect(readFileSync(saved, 'utf8')).toMatch(/^-----BEGIN PRIVATE KEY-----\n/);
				} finally {
					rmSync(saved);
				}
				await expect(page.getByTestId('sa-key-download')).toHaveText('Download again');
				await page.getByTestId('sa-key-confirm').check();
				await shot(page, `screens/${at}-access-save-key.png`);
				await page.getByTestId('sa-key-store').click();
				await expect(page.getByTestId('sa-saved')).toContainText(kid);
				// The engine names the stored key by the same thumbprint.
				await expect(page.getByTestId('sa-deployer')).toContainText(kid);
				await shot(page, `screens/${at}-access-service-accounts-saved.png`);
				// Disabling yourself says it is you, and nothing changes until confirmed.
				await page.goto(`${ENGINE}/admin/access/principals`);
				await page.getByTestId('person-disable-user:people:admin@example.test').click();
				await expect(page.getByTestId('person-self')).toContainText('This is you.');
				await shot(page, `screens/${at}-access-disable-self.png`);
				await page.getByTestId('person-cancel').click();
				await expect(page.getByTestId('person-disabled-user:people:admin@example.test')).toHaveCount(0);
			}
			if (tag === 'wide') {
				await page.getByTestId('account-trigger').click();
				await expect(page.getByTestId('account-panel')).toBeVisible();
				await page.screenshot({ path: proof(`screens/${at}-account-menu.png`) });
			}
			await ctx.close();
		}
	}
});
