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
 * The trigger is a button whose text is the sidebar switch's: an avatar
 * letter, a "Namespace" caption, then the selected option's label (the
 * chevron is an `aria-hidden` <svg> and contributes none). So the match is
 * the caption followed by exactly `name` to the end of the text: `alpha` must
 * not pass for `alpha-2`, and a longer name that merely ends in `alpha` does
 * not either, since only whitespace may sit between the caption and the name.
 */
export async function expectNamespace(page: Page, name: string): Promise<void> {
	await expect(page.getByTestId('namespace-picker')).toHaveText(new RegExp(`Namespace\\s*${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*$`));
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

/**
 * taskMapCellCenter returns a point to click for the task map's cell `index`
 * (a task's cell: the map's first section). It reads the map's own geometry
 * (`data-pitch`, `data-columns`), scrolls the map's area — never the page —
 * so the cell's row is in view, and returns the cell's centre in page
 * coordinates. The area's padding is the map's `p-2`.
 */
export async function taskMapCellCenter(page: Page, index: number): Promise<{ x: number; y: number }> {
	const map = page.getByTestId('task-map');
	await expect(map).toHaveAttribute('data-painted', 'true');
	const pitch = Number(await map.getAttribute('data-pitch'));
	const columns = Number(await map.getAttribute('data-columns'));
	const x0 = Number((await map.getAttribute('data-x0')) ?? 0);
	// A small run's map is drawn stage by stage: where each stage's cells start and stand.
	const stages = ((await map.getAttribute('data-rows')) ?? '0:0').split(',').map((s) => s.split(':').map(Number));
	const [start, top] = stages.filter(([first]) => first <= index).at(-1)!;
	const local = index - start;
	const area = page.getByTestId('task-map-area');
	await area.scrollIntoViewIfNeeded();
	const row = Math.floor(local / columns);
	const col = local % columns;
	await area.evaluate((el, y) => (el.scrollTop = Math.max(0, y)), top + row * pitch - pitch);
	const box = await area.boundingBox();
	if (!box) throw new Error('the task map area is not on screen');
	const scrollTop = await area.evaluate((el) => el.scrollTop);
	const PAD = 8;
	const half = (pitch - 2) / 2;
	return { x: box.x + PAD + x0 + col * pitch + half, y: box.y + PAD + top + row * pitch - scrollTop + half };
}

/**
 * The whole page as one image, as a reader scrolling it would see it. One
 * thing a full-page capture otherwise gets wrong is undone for the capture
 * only, and restored after it: task cards are laid out with
 * `content-visibility: auto`, which leaves every card past the first
 * viewport unpainted in a capture that does not scroll. During the capture
 * every element is painted.
 */
export async function fullPageShot(page: Page, path: string): Promise<void> {
	const style = await page.addStyleTag({
		content: '* { content-visibility: visible !important; }'
	});
	try {
		await page.screenshot({ path, fullPage: true });
	} finally {
		await style.evaluate((el) => el.remove());
	}
}

/**
 * Select task `id` on the run page, as a person does: press its name in the
 * task list. The name is a toggle (aria-pressed), so it is pressed only when
 * the task is not already selected — pressing a selected, explicitly chosen
 * task would clear the selection. A task the page auto-selected (the failed
 * one, else the first running or awaiting) reads aria-pressed true already.
 */
export async function selectTask(page: Page, id: string): Promise<void> {
	const link = page.getByTestId(`task-deep-link-${id}`);
	await expect(link).toBeVisible();
	// The page may have chosen this task by itself (pressed, and not in the
	// address): pressing it makes it the reader's own choice, which stays.
	if (new URL(page.url()).searchParams.get('task') !== id) await link.click();
	await expect(link).toHaveAttribute('aria-pressed', 'true');
	await expect(page).toHaveURL(new RegExp(`[?&]task=${id}(&|$)`));
}

/**
 * Select task `id` and open its Inspect disclosure in the Item card. The
 * disclosure is one boolean that survives a change of selection, so it is
 * pressed only when it is not already expanded.
 */
/** Chooses one of the Tasks card's secondary actions, from its More. */
export async function tasksMore(page: Page, id: string): Promise<void> {
	await page.getByTestId('run-tasks-more').click();
	await page.getByTestId(id).click();
}

export async function inspectTask(page: Page, id: string): Promise<void> {
	await selectTask(page, id);
	const toggle = page.getByTestId(`task-inspect-toggle-${id}`);
	await expect(toggle).toBeVisible();
	if ((await toggle.getAttribute('aria-expanded')) !== 'true') await toggle.click();
	await expect(toggle).toHaveAttribute('aria-expanded', 'true');
}

/**
 * One command in the page's one terminal icon (SheetHeader's `commands`), by
 * its label, once a click on page-cli-trigger has opened it.
 */
export function pageCli(page: Page, label: string) {
	return page.getByTestId('page-cli-popover').locator(`[data-cli-label="${label}"]`);
}

/**
 * Open a namespace's own page from its row: the fields the list leaves out
 * (Signed by, Budget, Allowed secrets, Updated) are there, and its edit form.
 */
export async function openNamespacePage(page: Page, name: string): Promise<void> {
	await page.getByTestId(`namespace-edit-${name}`).click();
	await expect(page.getByTestId('namespace-facts')).toBeVisible();
}
