// Admin → Namespaces is a cursor-paged list like Runs and Orkflows
// (browsing.ts), filtered by the one structured list filter
// (lib/listFilter.ts; docs/UI_CLI_PARITY.md "Filtering lists"). These are its
// pure pieces.
import { listHref } from './browsing';
import { FilterOperator, filterOf } from './listFilter';

/**
 * Where the page goes after a create: the filter `name=<created>` on a first
 * page, so its row is on screen whichever page it would sort onto. The
 * namespace selection and page size are kept.
 */
export function createdNamespaceHref(coordinates: Record<string, string>, name: string): string {
	return listHref('/admin/namespaces', { ...coordinates, cursor: '', page: '' }, {
		filter: [filterOf('name', FilterOperator.EQUALS, [name])]
	});
}

/**
 * Which empty-state sentence, if any, belongs above the list.
 *
 * - `no-match`: filters are applied and nothing matched them.
 * - `past-end`: an unfiltered page with nothing on it — its rows were deleted
 *   after the cursor that reached it was issued.
 * - `only-default`: the whole registry is `default`, the first-run sentence.
 */
export function namespacesEmptyState(view: { filtered: boolean; page: number; count: number; hasNext: boolean }): 'no-match' | 'past-end' | 'only-default' | null {
	if (view.count === 0) return view.filtered ? 'no-match' : 'past-end';
	if (!view.filtered && view.page === 1 && !view.hasNext && view.count <= 1) return 'only-default';
	return null;
}

/**
 * A namespace's own page, carrying the list's address (paging, filters) in
 * `from`, so leaving it lands where the reader was.
 */
export function namespacePageHref(name: string, listSearch: string, namespace: string): string {
	const params = new URLSearchParams({ ns: namespace || 'default' });
	if (listSearch) params.set('from', listSearch);
	return `/admin/namespaces/${encodeURIComponent(name)}?${params.toString()}`;
}

/** Where a namespace's page returns to: the list it came from, else this namespace's row. */
export function namespaceReturnHref(url: { searchParams: { get(name: string): string | null } }, name: string, namespace: string): string {
	const from = url.searchParams.get('from') ?? '';
	// Only a query of the list itself: `from` is in an address anyone can write.
	if (from.startsWith('?')) return `/admin/namespaces${from}`;
	return createdNamespaceHref({ ns: namespace }, name);
}
