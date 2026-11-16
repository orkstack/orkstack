// Sign-in, as the page keeps it (docs/AUTHENTICATION.md, "Sign-in for the
// web UI"). The cookie is HttpOnly: the page never sees it. What the page
// holds is the sign-in's CSRF token, handed out by AuthService.GetSignin and
// kept in this module's memory only, never in storage.
//
// signinFetch wraps the fetch every Connect client is given:
//   * a request that is not GET or HEAD carries Ork-CSRF, which every page
//     that calls GetSignin (the root layout, No access) sets here;
//   * a request refused SIGNIN_STALE (the sign-in is past its revalidation)
//     makes the page refresh it, one refresh at a time across every tab (the
//     Web Locks API, bounded in time), and is sent again once;
//   * a sign-in that has ended (SIGNIN_ENDED), on the request, its refresh or
//     its retry, is reported to the shell, which offers to sign in again
//     without leaving the page, so unsaved edits stay. A refresh the engine
//     or the issuer could not answer ends nothing: the refusal is returned.
//
// Bodies are replayed as given: Connect's unary calls send a string or a
// Uint8Array, never a stream, so a retry sends the same bytes.

export const CSRF_HEADER = 'Ork-CSRF';
export const SIGNIN_STALE = 'SIGNIN_STALE';
export const SIGNIN_ENDED = 'SIGNIN_ENDED';
const REFRESH_PATH = '/orkstack.v1.AuthService/RefreshSignin';
const LOCK_NAME = 'orkstack-signin-refresh';
/** How long a tab waits for another's refresh, or its own, before giving up. */
export const REFRESH_TIMEOUT_MS = 30_000;

let csrfToken = '';
let ended: (() => void) | null = null;
let inTab: Promise<RefreshOutcome> | null = null;
let held: Promise<void> | null = null;
const GETSIGNIN_PATH = '/orkstack.v1.AuthService/GetSignin';

/** The CSRF token GetSignin answered; empty when not signed in by cookie. */
export function setCsrfToken(token: string): void {
	csrfToken = token;
}

/** forgetCsrfToken makes the next write read the token again: after a new
 * sign-in in another tab, the old token is refused. */
export function forgetCsrfToken(): void {
	csrfToken = '';
}

/** holdCsrfToken says the sign-in is being read: until the returned
 * function is called, a request that would carry the token waits for it, so
 * a page's loads, which run beside the layout's, never go out without it and
 * are refused SIGNIN_CSRF. GetSignin, which hands the token out, never waits.
 * The caller releases it in a finally, token or none. */
export function holdCsrfToken(): () => void {
	let release!: () => void;
	const hold = new Promise<void>((r) => (release = r));
	held = hold;
	return () => {
		if (held === hold) held = null;
		release();
	};
}

function pathOf(input: RequestInfo | URL): string {
	const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
	try {
		return new URL(raw, 'http://page.invalid').pathname;
	} catch {
		return raw;
	}
}

export function csrfTokenForTest(): string {
	return csrfToken;
}

/** onSigninEnded registers what the shell does when the sign-in has ended. */
export function onSigninEnded(fn: (() => void) | null): void {
	ended = fn;
}

type Locks = { request<T>(name: string, options: { signal?: AbortSignal }, fn: () => Promise<T>): Promise<T> };

function locks(): Locks | null {
	const nav = (globalThis as { navigator?: { locks?: Locks } }).navigator;
	return nav?.locks ?? null;
}

/** The refusal a response carries, read from its Connect error body. */
export async function refusalOf(res: Response): Promise<string> {
	if (res.status !== 401) return '';
	try {
		const body = (await res.clone().json()) as { message?: unknown };
		const msg = typeof body.message === 'string' ? body.message : '';
		if (msg.startsWith(SIGNIN_STALE)) return SIGNIN_STALE;
		if (msg.startsWith(SIGNIN_ENDED)) return SIGNIN_ENDED;
	} catch {
		// Not a Connect error body: not a sign-in's refusal.
	}
	return '';
}

export type RefreshOutcome = 'ok' | 'ended' | 'error';

/** refreshSignin asks the engine to revalidate the sign-in, once across
 * tabs: a tab that waited for another's refresh finds its sign-in fresh, and
 * the engine answers it without redeeming the refresh token again. */
export function refreshSignin(base: typeof fetch): Promise<RefreshOutcome> {
	if (inTab) return inTab;
	const signal = AbortSignal.timeout(REFRESH_TIMEOUT_MS);
	const run = async (): Promise<RefreshOutcome> => {
		const headers: Record<string, string> = { 'Content-Type': 'application/json' };
		if (csrfToken) headers[CSRF_HEADER] = csrfToken;
		const res = await base(REFRESH_PATH, { method: 'POST', headers, body: '{}', credentials: 'same-origin', signal });
		if (res.ok) return 'ok';
		return res.status === 401 ? 'ended' : 'error';
	};
	const l = locks();
	inTab = (l ? l.request(LOCK_NAME, { signal }, run) : run())
		.catch((): RefreshOutcome => 'error')
		.finally(() => {
			inTab = null;
		});
	return inTab;
}

function methodOf(input: RequestInfo | URL, init?: RequestInit): string {
	if (init?.method) return init.method.toUpperCase();
	if (typeof Request !== 'undefined' && input instanceof Request) return input.method.toUpperCase();
	return 'GET';
}

function withCsrf(init: RequestInit | undefined, method: string): RequestInit | undefined {
	if (!csrfToken || method === 'GET' || method === 'HEAD') return init;
	const headers = new Headers(init?.headers);
	headers.set(CSRF_HEADER, csrfToken);
	return { ...init, headers };
}

/** signinFetch wraps base with the sign-in's rules (above). */
export function signinFetch(base: typeof fetch): typeof fetch {
	return async (input, init) => {
		const method = methodOf(input, init);
		if (held && method !== 'GET' && method !== 'HEAD' && pathOf(input) !== GETSIGNIN_PATH) await held;
		const res = await base(input, withCsrf(init, method));
		const refusal = await refusalOf(res);
		if (refusal === SIGNIN_STALE) {
			const outcome = await refreshSignin(base);
			if (outcome === 'ended') ended?.();
			if (outcome !== 'ok') return res;
			const again = await base(input, withCsrf(init, method));
			if ((await refusalOf(again)) !== '') ended?.();
			return again;
		}
		if (refusal === SIGNIN_ENDED) ended?.();
		return res;
	};
}

/** The path a sign-in returns to: this page, as the engine will accept it. */
export function returnPath(loc: { pathname: string; search: string }): string {
	return loc.pathname + loc.search;
}

/** The path that starts a sign-in with an issuer, returning to ret; empty
 * when the engine's login path is not a path of this origin. */
export function loginHref(loginPath: string, ret: string): string {
	if (!/^\/v1\/auth\/login\/[a-z][a-z0-9-]*$/.test(loginPath)) return '';
	return `${loginPath}?return=${encodeURIComponent(ret)}`;
}

/** Where sign-out sends the browser: the issuer's end-session page when it
 * is an https URL, else this deployment's sign-in page. */
export function endSessionTarget(url: string): string {
	try {
		return url && new URL(url).protocol === 'https:' ? url : '/signin';
	} catch {
		return '/signin';
	}
}
