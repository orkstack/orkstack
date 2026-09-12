import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { Status } from './gen/orkstack/v1/common_pb';

const statuses: Record<string, Status> = {
	pending: Status.PENDING,
	running: Status.RUNNING,
	succeeded: Status.SUCCEEDED,
	failed: Status.FAILED,
	canceled: Status.CANCELED,
	skipped: Status.SKIPPED
};

export function statusFromParam(value: string): Status { return statuses[value] ?? Status.UNSPECIFIED; }
export function statusParam(value: Status): string { return Object.entries(statuses).find(([, status]) => status === value)?.[0] ?? ''; }
export function pageSize(value: string): number { const parsed = Number(value); return [10, 25, 50, 100].includes(parsed) ? parsed : 25; }
export function instant(value: string) {
	if (!value) return undefined;
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? undefined : timestampFromDate(date);
}
export function runFilterError(status: string, after: string, before: string): string | null {
	if (status && statusFromParam(status) === Status.UNSPECIFIED) return `Unknown run status ${status}.`;
	if (after && !instant(after)) return 'Created-since must be a valid timestamp.';
	if (before && !instant(before)) return 'Created-until must be a valid timestamp.';
	if (after && before && new Date(before).getTime() <= new Date(after).getTime()) return 'Created-until must be after created-since.';
	return null;
}
export function pageNumber(value: string): number { const parsed = Number(value); return Number.isInteger(parsed) && parsed > 0 ? parsed : 1; }

// LIST_PATHS are the two cursor-paged lists. Naming them keeps a stack key and
// an href from being built for a path that has neither.
export type ListPath = '/runs' | '/orkflows';

// listHref turns a list's coordinates into its address, and it is the ONLY
// place either list writes a query string.
//
// It only ever `set`s, never `append`s, which is the whole point: the Orkflows
// page-size links used to concatenate `&size=N` onto a query that already
// carried `size=25`, so the URL read `size=25&size=50` and `searchParams.get`
// answered with the old value. A builder that starts from an empty
// URLSearchParams and sets each key once cannot express that URL.
//
// Empty values are omitted, so a cleared filter leaves the bar instead of
// lingering as `since=`; `page` is omitted at 1 because page 1 is what an
// address with no page means; `ns` is written first (spelled out for
// `default`, as withNamespace does) so the coordinate a reader looks for is
// the first thing they read.
export function listHref(path: ListPath, coordinates: Record<string, string>): string {
	const params = new URLSearchParams();
	if (coordinates.ns) params.set('ns', coordinates.ns);
	for (const [key, value] of Object.entries(coordinates)) if (value && key !== 'ns' && !(key === 'page' && value === '1')) params.set(key, value);
	return `${path}?${params.toString()}`;
}

// PAGE_STACK_PREFIX namespaces the per-tab cursor stacks in sessionStorage.
export const PAGE_STACK_PREFIX = 'orkstack.pages:';

// A stack is the list of cursors BEHIND the page being shown — the path this
// reader took to get here, not a coordinate of what the page shows. It used to
// ride the URL as a `history=` JSON array, which grew the address bar by one
// escaped engine cursor per Next click.
//
// It is keyed by the list's canonical filter query (the builder's output with
// `cursor` and `page` removed), so a stack belongs to exactly one filter set
// and can never be applied to another.
export function pageStackKey(path: ListPath, coordinates: Record<string, string>): string {
	return `${PAGE_STACK_PREFIX}${listHref(path, { ...coordinates, cursor: '', page: '' })}`;
}

/** The slice of Storage the page stack needs, so a test can hand in a fake. */
export interface PageStackStorage {
	getItem(key: string): string | null;
	setItem(key: string, value: string): void;
}

// sessionStorage, not localStorage: the engine's cursor is a position rather
// than a snapshot (docs/UI_CLI_PARITY.md), so a stack only means anything for
// as long as the reader who built it is still paging. And not page.state,
// which SvelteKit resets on a full reload — F5 on page 4 is an ordinary
// gesture and must not lose Previous.
//
// Both helpers are guarded: private-mode browsers throw on access, and a stack
// that cannot be read or written simply means Previous is disabled and First
// page is offered instead.
export function readPageStack(key: string, storage: PageStackStorage | undefined = globalThis.sessionStorage): string[] {
	try {
		const raw = storage?.getItem(key);
		if (!raw) return [];
		const parsed: unknown = JSON.parse(raw);
		return Array.isArray(parsed) && parsed.every((entry) => typeof entry === 'string') ? parsed : [];
	} catch { return []; }
}
export function writePageStack(key: string, stack: string[], storage: PageStackStorage | undefined = globalThis.sessionStorage): void {
	try { storage?.setItem(key, JSON.stringify(stack)); } catch { /* see readPageStack */ }
}

// sinceInputValue is the inverse of `new Date(input).toISOString()`: it turns
// an applied ISO instant back into the local `YYYY-MM-DDTHH:mm` a
// datetime-local input holds, so the dashboard's custom window shows the
// window that is in force instead of an empty field beside it.
export function sinceInputValue(iso: string): string {
	if (!iso) return '';
	const date = new Date(iso);
	if (Number.isNaN(date.getTime())) return '';
	return new Date(date.getTime() - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
}

export function statsWindowError(window: string, since: string): string | null {
	if (!['15m', '6h', '24h', 'custom'].includes(window)) return `Unknown dashboard window ${window}.`;
	if (window === 'custom' && !since) return 'Custom dashboard windows require a valid since timestamp.';
	if (since && !instant(since)) return 'Dashboard since must be a valid timestamp.';
	return null;
}
