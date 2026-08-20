import { errorMessage, orkstack } from "$lib/client";
import { namespaceFrom } from "$lib/namespace";
import type { LayoutLoad } from "./$types";

// The namespace picker is in the layout because the namespace scopes every
// page under it. Loading the list here also means an engine that has only the
// default namespace still renders the control, showing the one name there is
// rather than nothing.
export const load: LayoutLoad = async ({ fetch, url }) => {
  const namespace = namespaceFrom(url);
  try {
    const res = await orkstack(fetch).namespaces.listNamespaces({});
    return {
      namespace,
      namespaces: res.namespaces.map((ns) => ns.name),
      namespaceError: null as string | null,
    };
  } catch (err) {
    // A namespace list the engine will not answer must not blank the page:
    // the picker falls back to the namespace this page is already in, and
    // says why it cannot offer the others.
    return { namespace, namespaces: [], namespaceError: errorMessage(err) };
  }
};
