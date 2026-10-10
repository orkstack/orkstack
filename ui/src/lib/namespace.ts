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
// showing, on the same terms as a list's `?filter=`: a reload, a back button, a
// bookmark and a pasted link all keep it, and the server render and the
// browser agree without a store to synchronize.

// NAMESPACE_PARAM is `ns` — the fixed spelling, and the one a person types by
// hand. It is the PAGE's parameter and has
// nothing to do with the request field, which stays `namespace`: the field is
// the contract with the engine, this is the address of a page.
export const NAMESPACE_PARAM = "ns";

// DEFAULT_NAMESPACE is what the engine resolves an unnamed namespace to. It is
// a name like any other on the PAGE — `?ns=default` is spelled out, see
// withNamespace — and never on the wire, where the default namespace is the
// empty string. One namespace, one wire value.
export const DEFAULT_NAMESPACE = "default";

// NAMESPACE_STORAGE_KEY remembers the last namespace this browser LOOKED AT —
// written by the switcher, and by the layout on any URL that names one — so
// that opening a bare `/orkflows` reopens the work the user was doing instead
// of resetting them to `default` in silence.
//
// It stores the LABEL, so `default` is remembered as a name when it is chosen.
// Storing the wire value would make "chose `default`" and "chose nothing" the
// same empty string, and the restore would then drag somebody who deliberately
// picked `default` back into the namespace they had just left.
export const NAMESPACE_STORAGE_KEY = "orkstack.namespace";

// namespaceFrom reads the namespace a page was asked for, as a WIRE value:
// empty for the default namespace, which is exactly what a request carries for
// it. The address bar spells `default` out; the request never does, so one
// namespace is never sent to the engine as two different strings.
export function namespaceFrom(url: URL): string {
  const asked = url.searchParams.get(NAMESPACE_PARAM) ?? "";
  return asked === DEFAULT_NAMESPACE ? "" : asked;
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

// withNamespace carries the namespace onto an internal link — including the
// default one, spelled out as `?ns=default`.
//
// Naming it by SILENCE is the bug this replaces. A URL with no `?ns=` does not
// say "the default namespace"; it says "no choice recorded", and the restore
// in +layout.svelte then answers it with whatever the READER last chose. One
// address opened `payments` for one colleague and `default` for another —
// precisely what a pasted link must never do. So every internal link states
// the namespace it belongs to, and "selection is canonical in the URL on every
// route" holds for `default` too.
export function withNamespace(path: string, namespace: string): string {
  const [base, query] = path.split("?");
  const params = new URLSearchParams(query ?? "");
  params.set(NAMESPACE_PARAM, namespaceLabel(namespace));
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

// rememberedNamespace reads the last namespace this browser was in, as its
// LABEL, or null when it has never been in one — including on a server render,
// where there is no storage to read. Null and `default` are different answers:
// null means a bare URL has nothing to restore and is simply made to say
// `default`, while `default` is a namespace to restore like any other.
export function rememberedNamespace(): string | null {
  try {
    return globalThis.localStorage?.getItem(NAMESPACE_STORAGE_KEY) ?? null;
  } catch {
    // Private-mode browsers throw on read.
    return null;
  }
}
