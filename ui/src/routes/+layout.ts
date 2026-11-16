import { redirect } from "@sveltejs/kit";
import { Code, ConnectError } from "@connectrpc/connect";
import { errorMessage } from "#lib/client.ts";
import { authClient } from "#lib/clients/auth.ts";
import { cachedWho, readAuthMode, rememberWho } from "#lib/authMode.ts";
import type { GetSigninResponse } from "#lib/gen/orkstack/v1/auth_service_pb.ts";
import { holdCsrfToken, setCsrfToken } from "#lib/signin.ts";
import type { Grants } from "#lib/permissions.ts";
import { namespaceClient } from "#lib/clients/namespaces.ts";
import type { Budget, NamespaceHold } from "#lib/gen/orkstack/v1/namespace_pb.ts";
import { namespaceFrom, namespaceLabel } from "#lib/namespace.ts";
import type { LayoutLoad } from "./$types";

// The UI is a static single-page app the engine serves (internal/webui):
// every route renders in the browser, from index.html, and no load ever runs
// on a server.
export const ssr = false;

// The namespace picker is in the layout because the namespace scopes every
// page under it. Loading the list here also means an engine that has only the
// default namespace still renders the control, showing the one name there is
// rather than nothing.
// Who the caller is, once per navigation (UX35): in asserted mode nobody is
// verified and nothing changes; otherwise GetSignin names the caller, hands
// the page its CSRF token, and says what it may do. A caller with no
// credential goes to /signin, returning here; one bound to nothing goes to
// /signin/no-access.
async function whoAmI(fetchFn: typeof fetch, url: URL): Promise<Who> {
  const cached = cachedWho<Who>();
  if (cached) return cached;
  const who = await askWho(fetchFn, url);
  rememberWho(who);
  return who;
}

type Who = { authMode: string; me: GetSigninResponse | null; grants: Grants };

async function askWho(fetchFn: typeof fetch, url: URL): Promise<Who> {
  // GetSignin first, and alone: it hands out the CSRF token every other call
  // of a cookie sign-in carries (GetSystemInfo, and the page's own loads,
  // which run beside this one and wait for it under the hold).
  let me: GetSigninResponse | null = null;
  let signinErr: unknown = null;
  const release = holdCsrfToken();
  try {
    me = await authClient(fetchFn).getSignin({});
    setCsrfToken(me.csrfToken);
  } catch (err) {
    setCsrfToken("");
    signinErr = err;
  } finally {
    release();
  }
  const authMode: string = await readAuthMode(fetchFn);
  if (authMode === "asserted") {
    setCsrfToken("");
    return { authMode, me: null, grants: { enforced: false, held: [] } };
  }
  if (me) {
    if (authMode === "enforce" && me.bindings.length === 0 && me.signin) redirect(307, "/signin/no-access");
    return { authMode, me, grants: { enforced: authMode === "enforce", held: me.permissions } };
  }
  if (signinErr instanceof ConnectError && signinErr.code === Code.Unauthenticated && authMode === "enforce") {
    redirect(307, `/signin?return=${encodeURIComponent(url.pathname + url.search)}`);
  }
  if (!(signinErr instanceof ConnectError)) throw signinErr; // a bug
  return { authMode, me: null, grants: { enforced: authMode === "enforce", held: [] } };
}

export const load: LayoutLoad = async ({ fetch, url }) => {
  // The sign-in pages draw no shell and read nothing of it.
  if (url.pathname === "/signin" || url.pathname.startsWith("/signin/")) {
    return { bare: true, authMode: "", me: null as GetSigninResponse | null, grants: { enforced: false, held: [] } as Grants,
      namespace: namespaceFrom(url), namespaces: [] as string[], namespaceHold: null as NamespaceHold | null,
      namespaceBudget: null as Budget | null, namespaceError: null as string | null };
  }
  const who = await whoAmI(fetch, url);
  const namespace = namespaceFrom(url);
  try {
    const res = await namespaceClient(fetch).listNamespaces({});
    // The hold of the namespace this page is in, from the same read — no
    // extra RPC. It is a snapshot as of this navigation, the picker's own
    // contract; the server is the gate, and the submit panel reads fresh.
    const shown = namespaceLabel(namespace);
    const namespaceHold: NamespaceHold | null =
      res.namespaces.find((ns) => ns.name === shown)?.hold ?? null;
    // The budget of the same document, on the same terms: a snapshot as of
    // this navigation, read by the dashboard's Usage card to ask its own
    // period's question. The server is the gate.
    const namespaceBudget: Budget | null =
      res.namespaces.find((ns) => ns.name === shown)?.budget ?? null;
    return {
      bare: false,
      ...who,
      namespace,
      namespaces: res.namespaces.map((ns) => ns.name),
      namespaceHold,
      namespaceBudget,
      namespaceError: null as string | null,
    };
  } catch (err) {
    // A namespace list the engine will not answer must not blank the page:
    // the picker falls back to the namespace this page is already in, and
    // says why it cannot offer the others.
    return {
      bare: false,
      ...who,
      namespace,
      namespaces: [],
      namespaceHold: null as NamespaceHold | null,
      namespaceBudget: null as Budget | null,
      namespaceError: errorMessage(err),
    };
  }
};
