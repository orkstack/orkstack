// The namespace every engine read on this page is scoped to.
//
// The engine scopes ListOrkflows, GetOrkflow, ListRuns, GetRun, SubmitRun,
// ApplyOrkflow and the event socket by namespace, and a request that names
// none resolves to `default` — never "all of them". So a UI that sends nothing
// is not unscoped: it is pinned to `default`, and somebody working in
// `payments` gets an empty page with no hint that another namespace exists and
// no control to reach it.
//
// It lives in the query string because it is a coordinate of what the page is
// showing, on the same terms as `?orkflow=`: a reload, a back button, a
// bookmark and a pasted link all keep it, and the server render and the
// browser agree without a store to synchronize.

export const NAMESPACE_PARAM = "namespace";

// DEFAULT_NAMESPACE is what the engine resolves an unnamed namespace to. The
// UI keeps the wire value empty in that case rather than spelling it out, so
// one name is never sent as two different things.
export const DEFAULT_NAMESPACE = "default";

// namespaceFrom reads the namespace a page was asked for. Empty means the
// default namespace, which is exactly what the request carries.
export function namespaceFrom(url: URL): string {
  return url.searchParams.get(NAMESPACE_PARAM) ?? "";
}

// namespaceLabel is the name to show a person. The wire value for the default
// namespace is empty; its name is not.
export function namespaceLabel(namespace: string): string {
  return namespace || DEFAULT_NAMESPACE;
}

// withNamespace carries the namespace onto an internal link, so following one
// does not silently drop back to `default`. The default namespace adds no
// parameter: it is the answer to an absent one.
export function withNamespace(path: string, namespace: string): string {
  if (!namespace || namespace === DEFAULT_NAMESPACE) return path;
  const [base, query] = path.split("?");
  const params = new URLSearchParams(query ?? "");
  params.set(NAMESPACE_PARAM, namespace);
  return `${base}?${params.toString()}`;
}
