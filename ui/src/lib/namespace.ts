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

// NAMESPACE_PARAM is `ns` — the spelling the design fixed (v003-design.md
// §1.9) and the one a person types by hand. It is the PAGE's parameter and has
// nothing to do with the request field, which stays `namespace`: the field is
// the contract with the engine, this is the address of a page.
export const NAMESPACE_PARAM = "ns";

// DEFAULT_NAMESPACE is what the engine resolves an unnamed namespace to. The
// UI keeps the wire value empty in that case rather than spelling it out, so
// one name is never sent as two different things.
export const DEFAULT_NAMESPACE = "default";

// NAMESPACE_STORAGE_KEY remembers the last namespace CHOSEN, so that opening a
// bare `/orkflows` in the browser that made the choice reopens the work the
// user was doing instead of resetting them to `default` in silence.
//
// It remembers a choice, never a page: what is stored is written by the
// switcher, so a pasted link cannot overwrite somebody else's preference by
// being followed once, and `default` is stored explicitly when it is picked —
// otherwise choosing `default` would be undone by the restore on the next
// bare URL.
export const NAMESPACE_STORAGE_KEY = "orkstack.namespace";

// namespaceFrom reads the namespace a page was asked for. Empty means the
// default namespace, which is exactly what the request carries.
export function namespaceFrom(url: URL): string {
  return url.searchParams.get(NAMESPACE_PARAM) ?? "";
}

// hasNamespaceParam distinguishes "this URL says default" from "this URL says
// nothing". Only the second is a candidate for the remembered choice.
export function hasNamespaceParam(url: URL): boolean {
  return url.searchParams.has(NAMESPACE_PARAM);
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

// rememberNamespace stores a namespace the user CHOSE. A label, not a wire
// value: `default` is remembered under its name so that picking it is a
// decision the restore below honours rather than an absence it overrides.
export function rememberNamespace(namespace: string): void {
  try {
    globalThis.localStorage?.setItem(NAMESPACE_STORAGE_KEY, namespaceLabel(namespace));
  } catch {
    // Private-mode browsers throw on write. A namespace that cannot be
    // remembered still works; it just does not survive a bare URL.
  }
}

// rememberedNamespace reads the last chosen namespace, as a wire value: empty
// for `default`, for nothing stored, and on a server render where there is no
// storage to read.
export function rememberedNamespace(): string {
  try {
    const stored = globalThis.localStorage?.getItem(NAMESPACE_STORAGE_KEY) ?? "";
    return stored === DEFAULT_NAMESPACE ? "" : stored;
  } catch {
    return "";
  }
}
