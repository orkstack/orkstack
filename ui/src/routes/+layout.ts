import { errorMessage } from "$lib/client";
import { namespaceClient } from "$lib/clients/namespaces";
import type { Budget, NamespaceHold } from "$lib/gen/orkstack/v1/namespace_pb";
import { namespaceFrom, namespaceLabel } from "$lib/namespace";
import type { LayoutLoad } from "./$types";

// The UI is a static single-page app the engine serves (internal/webui):
// every route renders in the browser, from index.html, and no load ever runs
// on a server.
export const ssr = false;

// The namespace picker is in the layout because the namespace scopes every
// page under it. Loading the list here also means an engine that has only the
// default namespace still renders the control, showing the one name there is
// rather than nothing.
export const load: LayoutLoad = async ({ fetch, url }) => {
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
      namespace,
      namespaces: [],
      namespaceHold: null as NamespaceHold | null,
      namespaceBudget: null as Budget | null,
      namespaceError: errorMessage(err),
    };
  }
};
