import { expect, test } from "bun:test";
import {
  CSRF_HEADER,
  endSessionTarget,
  holdCsrfToken,
  loginHref,
  onSigninEnded,
  refusalOf,
  setCsrfToken,
  signinFetch,
} from "../src/lib/signin";

function refusal(message: string): Response {
  return new Response(JSON.stringify({ code: "unauthenticated", message }), {
    status: 401,
  });
}

function fresh() {
  setCsrfToken("");
  onSigninEnded(null);
}

test("a request that is not GET carries the CSRF token; a GET does not", async () => {
  fresh();
  const seen: Headers[] = [];
  const base = (async (_i: RequestInfo | URL, init?: RequestInit) => {
    seen.push(new Headers(init?.headers));
    return new Response("{}");
  }) as typeof fetch;
  setCsrfToken("tok");
  const f = signinFetch(base);
  await f("/orkstack.v1.RunService/CancelRun", { method: "POST", body: "{}" });
  await f("/runs", { method: "GET" });
  expect(seen[0].get(CSRF_HEADER)).toBe("tok");
  expect(seen[1].get(CSRF_HEADER)).toBeNull();
});

test("a stale sign-in is refreshed once and the request sent again", async () => {
  fresh();
  const calls: string[] = [];
  let stale = true;
  const base = (async (i: RequestInfo | URL) => {
    const path = String(i);
    calls.push(path);
    if (path.endsWith("RefreshSignin")) {
      stale = false;
      return new Response("{}");
    }
    return stale
      ? refusal("SIGNIN_STALE: the sign-in must be refreshed")
      : new Response('{"ok":true}');
  }) as typeof fetch;
  const f = signinFetch(base);
  const [a, b] = await Promise.all([
    f("/orkstack.v1.RunService/ListRuns", { method: "POST", body: "{}" }),
    f("/orkstack.v1.RunService/GetRun", { method: "POST", body: "{}" }),
  ]);
  expect(a.ok && b.ok).toBe(true);
  expect(calls.filter((c) => c.endsWith("RefreshSignin")).length).toBe(1);
});

test("an ended sign-in, or a refresh the engine refuses, is reported to the shell", async () => {
  fresh();
  let told = 0;
  onSigninEnded(() => told++);
  const base = (async (i: RequestInfo | URL) => {
    if (String(i).endsWith("RefreshSignin"))
      return refusal("SIGNIN_ENDED: the sign-in has ended; sign in again");
    return refusal("SIGNIN_STALE: x");
  }) as typeof fetch;
  const res = await signinFetch(base)("/x", { method: "POST" });
  expect(res.status).toBe(401);
  expect(told).toBe(1);
  const ended = (async () =>
    refusal("SIGNIN_ENDED: gone")) as unknown as typeof fetch;
  await signinFetch(ended)("/x", { method: "POST" });
  expect(told).toBe(2);
});

test("refusalOf reads only a 401 Connect body naming a sign-in refusal", async () => {
  expect(await refusalOf(refusal("SIGNIN_STALE: x"))).toBe("SIGNIN_STALE");
  expect(await refusalOf(refusal("SIGNIN_ENDED: x"))).toBe("SIGNIN_ENDED");
  expect(await refusalOf(refusal("the credential was refused (key)"))).toBe("");
  expect(await refusalOf(new Response("nope", { status: 401 }))).toBe("");
  expect(
    await refusalOf(
      new Response('{"message":"SIGNIN_STALE"}', { status: 403 }),
    ),
  ).toBe("");
});

test("loginHref carries the return path, encoded", () => {
  expect(loginHref("/v1/auth/login/corp", "/runs/ork-1?tab=log")).toBe(
    "/v1/auth/login/corp?return=%2Fruns%2Fork-1%3Ftab%3Dlog",
  );
});

test('a refresh the engine could not answer ends nothing, and the refusal is returned', async () => {
	fresh();
	setCsrfToken('tok');
	let told = 0;
	onSigninEnded(() => told++);
	const base = (async (i: RequestInfo | URL) => {
		if (String(i).endsWith('RefreshSignin')) return new Response('{"message":"down"}', { status: 503 });
		return refusal('SIGNIN_STALE: x');
	}) as typeof fetch;
	const res = await signinFetch(base)('/x', { method: 'POST' });
	expect(res.status).toBe(401);
	expect(told).toBe(0);
});

test('a retry refused as ended is reported', async () => {
	fresh();
	setCsrfToken('tok');
	let told = 0;
	onSigninEnded(() => told++);
	let first = true;
	const base = (async (i: RequestInfo | URL) => {
		if (String(i).endsWith('RefreshSignin')) return new Response('{}');
		if (first) {
			first = false;
			return refusal('SIGNIN_STALE: x');
		}
		return refusal('SIGNIN_ENDED: disabled');
	}) as typeof fetch;
	await signinFetch(base)('/x', { method: 'POST' });
	expect(told).toBe(1);
});


test('loginHref takes only the engine’s own login path; endSessionTarget only https', () => {
	expect(loginHref('//evil.example/x', '/')).toBe('');
	expect(loginHref('javascript:alert(1)', '/')).toBe('');
	expect(loginHref('https://evil.example/v1/auth/login/x', '/')).toBe('');
	expect(endSessionTarget('https://login.example/end')).toBe('https://login.example/end');
	expect(endSessionTarget('javascript:alert(1)')).toBe('/signin');
	expect(endSessionTarget('')).toBe('/signin');
});

test("a request sent while the sign-in is being read waits for its CSRF token; GetSignin itself does not wait", async () => {
  fresh();
  const seen: { path: string; csrf: string | null }[] = [];
  const base = (async (i: RequestInfo | URL, init?: RequestInit) => {
    seen.push({ path: String(i), csrf: new Headers(init?.headers).get(CSRF_HEADER) });
    return new Response("{}");
  }) as typeof fetch;
  const f = signinFetch(base);
  const release = holdCsrfToken();
  const write = f("/orkstack.v1.AccessService/GetAccess", { method: "POST", body: "{}" });
  await f("/orkstack.v1.AuthService/GetSignin", { method: "POST", body: "{}" });
  expect(seen.map((s) => s.path)).toEqual(["/orkstack.v1.AuthService/GetSignin"]);
  setCsrfToken("tok");
  release();
  await write;
  expect(seen[1]).toEqual({ path: "/orkstack.v1.AccessService/GetAccess", csrf: "tok" });
});

test("a hold released with no token lets the waiting request go without one", async () => {
  fresh();
  const seen: (string | null)[] = [];
  const base = (async (_i: RequestInfo | URL, init?: RequestInit) => {
    seen.push(new Headers(init?.headers).get(CSRF_HEADER));
    return new Response("{}");
  }) as typeof fetch;
  const release = holdCsrfToken();
  const write = signinFetch(base)("/orkstack.v1.SystemService/GetSystemInfo", { method: "POST", body: "{}" });
  release();
  await write;
  expect(seen).toEqual([null]);
});
