import { expect, type Page } from '@playwright/test';

// The page interactions more than one spec performs, in one place.
//
// harness.ts is this directory's PROCESS module — spawn a binary, probe a
// port, call an engine, kill everything — and says why it is a module rather
// than a copy per spec: "a helper that exists twice drifts twice". This is the
// same argument applied to the other half, the browser: a control whose shape
// several specs drive should be driven from one place, so that rebuilding the
// control is one edit here instead of sixteen scattered ones.
//
// Playwright collects `*.spec.ts`, so this file is imported and never run as a
// test.

// The namespace switcher is the custom Select (ui/src/lib/components/Select.svelte),
// not a native <select>: a button carrying combobox semantics, and a listbox
// of option BUTTONS that only exists while it is open. So there is no
// `selectOption` to call and no `value` to assert — choosing is a click on the
// trigger and then a click on the row, which is also the gesture a person
// makes.
//
// `page` is a parameter rather than a closed-over fixture because these specs
// drive more than one session: a second browser context standing in for a
// colleague opening a pasted link is the whole point of some of them, and a
// helper pinned to one Page would silently assert about the wrong browser.

/** Switch the namespace picker (the sidebar Select) to `name`, on whichever session is passed. */
export async function selectNamespace(page: Page, name: string): Promise<void> {
	await page.getByTestId('namespace-picker').click();
	// The option's own testid is the namespace name: the picker's options are
	// built from the registry's names, so value and label are one string.
	await page.getByTestId(`namespace-picker-option-${name}`).click();
	// Deliberately no assertion that the value took: the switch navigates with
	// `invalidateAll`, and callers gate a stalled navigation on purpose. What
	// the picker shows afterwards is expectNamespace's business, at the moment
	// the caller says so.
}

/**
 * Assert the namespace picker currently displays `name`.
 *
 * The trigger is a button, so `toHaveValue` does not apply to it at all. Its
 * only text is the selected option's label — the chevron beside it is an
 * `aria-hidden` <svg> that contributes none — and toHaveText normalizes
 * whitespace, so the surrounding markup's indentation is not part of the
 * comparison. Exact, not substring: `alpha` must not pass for `alpha-2`.
 */
export async function expectNamespace(page: Page, name: string): Promise<void> {
	await expect(page.getByTestId('namespace-picker')).toHaveText(name);
}
