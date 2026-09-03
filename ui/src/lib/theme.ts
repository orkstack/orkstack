// The reader's chosen appearance: light, dark, or "whatever my OS says".
//
// "system" is never written to storage. The moment something writes an
// explicit choice here, that choice stops following a later change to the
// OS setting — which is exactly the one behavior a reader who picked
// "System" must keep getting: not "whatever my OS said when this tab
// loaded", but "whatever my OS says, always, until I pick something else".
export type Theme = "system" | "light" | "dark";

// ResolvedTheme is the only vocabulary app.css's dark rules know. Every
// `Theme` — "system" included — is resolved to one of these two before it
// ever reaches the DOM; see `applyTheme`.
export type ResolvedTheme = "light" | "dark";

const THEME_STORAGE_KEY = "ork-theme";
const DARK_QUERY = "(prefers-color-scheme: dark)";

// rememberTheme stores a choice the reader made. "system" is stored as
// nothing AT ALL — removing the key rather than writing it — so a later
// visit with no explicit choice reads as "system" the same way a visit that
// never called this at all does.
export function rememberTheme(theme: Theme): void {
  try {
    if (theme === "system") {
      globalThis.localStorage?.removeItem(THEME_STORAGE_KEY);
    } else {
      globalThis.localStorage?.setItem(THEME_STORAGE_KEY, theme);
    }
  } catch {
    // Private-mode browsers throw on write. A theme that cannot be
    // remembered still applies for this load; it just reverts to the
    // system default on the next one.
  }
}

// rememberedTheme reads the reader's last CHOICE — never a resolved
// appearance — including on a server render, where there is no storage to
// read and "system" is the only honest answer.
export function rememberedTheme(): Theme {
  try {
    const stored = globalThis.localStorage?.getItem(THEME_STORAGE_KEY);
    return stored === "light" || stored === "dark" ? stored : "system";
  } catch {
    return "system";
  }
}

function systemPrefersDark(): boolean {
  try {
    return globalThis.matchMedia?.(DARK_QUERY).matches ?? false;
  } catch {
    return false;
  }
}

// resolveTheme turns a choice into the one of two appearances the page
// actually paints. "system" is not a third look; it is "read the OS now".
export function resolveTheme(theme: Theme): ResolvedTheme {
  return theme === "system" ? (systemPrefersDark() ? "dark" : "light") : theme;
}

// applyTheme paints the resolved appearance. The attribute always names one
// of the two real themes, never "system" — app.css's dark rules key off
// `data-theme="dark"` alone (see app.css's own comment on why there is no
// media query there too); the CHOICE that produced this value lives only in
// storage and in the layout's own state.
export function applyTheme(theme: Theme): void {
  const root = globalThis.document?.documentElement;
  if (!root) return;
  root.setAttribute("data-theme", resolveTheme(theme));
}

// watchSystemTheme re-applies whenever the OS preference changes AND the
// reader's current choice is "system" — see the type's own doc comment for
// why a stale snapshot taken at load time is not good enough. `getTheme` is
// called fresh on every OS change rather than captured once, so callers can
// hand it a live reference (e.g. `() => theme` closing over a $state) and
// this keeps working across the reader changing their mind later.
//
// Returns the unsubscribe function. Where matchMedia does not exist (SSR,
// old browsers) it returns a no-op rather than throwing.
export function watchSystemTheme(getTheme: () => Theme): () => void {
  let mql: MediaQueryList | undefined;
  try {
    mql = globalThis.matchMedia?.(DARK_QUERY);
  } catch {
    return () => {};
  }
  if (!mql) return () => {};
  const onChange = () => {
    if (getTheme() === "system") applyTheme("system");
  };
  mql.addEventListener("change", onChange);
  return () => mql?.removeEventListener("change", onChange);
}
