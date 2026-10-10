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
 * Choose `value` from any Select (ui/src/lib/components/Select.svelte) by its
 * testid — the page-size picker, a status filter, anything built out of it.
 *
 * The gesture is the trigger, then the row, exactly as selectNamespace spells
 * it out above: the options are option BUTTONS that only exist while the
 * listbox is open, so Playwright's `selectOption` has nothing to act on. The
 * value, not the label, names the row — the component builds each option's
 * testid from its value.
 */
export async function selectOption(page: Page, testid: string, value: string): Promise<void> {
	await page.getByTestId(testid).click();
	await page.getByTestId(`${testid}-option-${value}`).click();
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

/**
 * The Admin → Namespaces address that shows `name`'s row: the list filtered
 * to it (`filter=name=<name>`, the structured list filter) on a first page.
 *
 * The list is paged (ui/src/routes/admin/namespaces/+page.ts), so the page a
 * namespace sorts onto depends on every other namespace in the bucket. A spec
 * that opened the bare list and looked for its row would pass or fail on what
 * other runs left behind; through the filter it never depends on that. `ns`
 * is the switcher's selection, carried as every other link carries it.
 */
export function adminNamespaceUrl(origin: string, name: string, ns = 'default'): string {
	const params = new URLSearchParams({ ns, filter: `name=${name}` });
	return `${origin}/admin/namespaces?${params.toString()}`;
}

/**
 * The operator Select's option values in ListFilter
 * (ui/src/lib/components/ListFilter.svelte): FilterOperator's numbers.
 */
export const OP = {
	equals: '1',
	notEquals: '2',
	in: '3',
	contains: '4',
	prefix: '5',
	atOrAfter: '6',
	before: '7'
} as const;

/**
 * Add one filter row to a ListFilter form and fill it, as a person does:
 * Add filter, the field, a label key, the operator, then the value. Nothing
 * is applied — the caller submits with `${testid}-filters-apply`, which is
 * the point of a draft.
 */
export async function addListFilter(
	page: Page,
	testid: string,
	row: { field: string; key?: string; op: (typeof OP)[keyof typeof OP]; value?: string; select?: string }
): Promise<number> {
	const index = await page.locator(`[data-testid^="${testid}-filter-row-"]`).count();
	await page.getByTestId(`${testid}-filter-add`).click();
	await selectOption(page, `${testid}-filter-field-${index}`, row.field);
	if (row.key !== undefined) await page.getByTestId(`${testid}-filter-key-${index}`).fill(row.key);
	await selectOption(page, `${testid}-filter-op-${index}`, row.op);
	if (row.select !== undefined) await selectOption(page, `${testid}-filter-value-${index}`, row.select);
	if (row.value !== undefined) await page.getByTestId(`${testid}-filter-value-${index}`).fill(row.value);
	return index;
}
