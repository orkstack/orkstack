// Admin → Namespaces is a cursor-paged list like Runs and Orkflows
// (browsing.ts), with one filter of its own: `q`, a substring of the name,
// sent as ListNamespacesRequest.name_contains. These are its pure pieces.
import { listHref } from './browsing';

/** The longest name_contains the engine accepts: the longest a name can be. */
export const NAMESPACE_FILTER_MAX = 200;

/** The filter as the URL carries it: trimmed, and otherwise exactly as typed. */
export function namespaceFilterParam(value: string): string {
	return value.trim();
}

/**
 * Where the page goes after a create: the created name as the filter, on a
 * first page, so its row is on screen whichever page it would sort onto. The
 * namespace selection and page size are kept.
 */
export function createdNamespaceHref(coordinates: Record<string, string>, name: string): string {
	return listHref('/admin/namespaces', { ...coordinates, q: name, cursor: '', page: '' });
}

/**
 * Which empty-state sentence, if any, belongs above the list.
 *
 * - `no-match`: the filter matched no name, so the sentence names the filter.
 * - `past-end`: an unfiltered page with nothing on it — its rows were deleted
 *   after the cursor that reached it was issued.
 * - `only-default`: the whole registry is `default`, the first-run sentence.
 */
export function namespacesEmptyState(view: { filter: string; page: number; count: number; hasNext: boolean }): 'no-match' | 'past-end' | 'only-default' | null {
	if (view.count === 0) return view.filter ? 'no-match' : 'past-end';
	if (!view.filter && view.page === 1 && !view.hasNext && view.count <= 1) return 'only-default';
	return null;
}
