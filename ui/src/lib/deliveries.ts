import { timestampDate, type Duration, type Timestamp } from '@bufbuild/protobuf/wkt';
import type { Delivery } from './gen/orkstack/v1/webhook_service_pb';
import { listHref, pageStackKey } from './browsing';
import { FilterOperator, equalityOf, filterFlags, filterOf, type FieldFilter } from './listFilter';

// The Deliveries panel's pure half (Admin → Webhooks): the panel's address,
// how a record reads, and the orkctl command for the same listing. Kept out
// of the component so each rule is unit-tested once (test/deliveries.test.ts)
// and the panel and `orkctl webhook deliveries` can never disagree.
//
// What the panel may be filtered on is not written here: it is the
// ListDeliveriesRequest allowlist, offered by the one filter component
// (lib/listFilter.ts, components/ListFilter.svelte) and decided by the engine
// (docs/UI_CLI_PARITY.md "Filtering lists").

/** The caption's fixed half: what a record is, and that the body is not in it. */
export const DELIVERIES_CAPTION =
	"One record per settled delivery — every attempt, the signature sent, and the body's SHA-256; the body itself is not stored.";

export const DEFAULT_DELIVERY_LIMIT = 50;
export const MAX_DELIVERY_LIMIT = 200;

/**
 * The panel's filter parameter. Admin → Webhooks holds two lists, so the
 * panel's filters ride `deliveryFilter` and the subscriptions table's ride
 * `filter` (lib/browsing.ts listHref).
 */
export const DELIVERY_FILTER_PARAM = 'deliveryFilter';

export interface DeliveryCoordinates {
	/** The page's namespace selector, preserved in every address; it does NOT filter the panel. */
	ns: string;
	/** The panel's filters (its `deliveryFilter` parameters). */
	filters: FieldFilter[];
	limit: number;
	cursor: string;
	page: number;
	/**
	 * The subscriptions table's own filters (its `filter` parameters), carried
	 * through so paging the panel never drops them. They do not filter the panel.
	 */
	subscriptionFilters: FieldFilter[];
}

/** limit from the URL: 1..200, anything else is the default. */
export function deliveryLimit(value: string | null): number {
	const parsed = Number(value ?? '');
	return Number.isInteger(parsed) && parsed >= 1 && parsed <= MAX_DELIVERY_LIMIT ? parsed : DEFAULT_DELIVERY_LIMIT;
}

/** The panel's filters for one subscription: what a row's Deliveries link opens. */
export function webhookDeliveriesFilters(webhookId: string): FieldFilter[] {
	return [filterOf('webhook_id', FilterOperator.EQUALS, [webhookId])];
}

/** The panel's filters for one subscription's deliveries about one run (a redelivery's original). */
export function runDeliveriesFilters(runId: string, webhookId: string): FieldFilter[] {
	return [filterOf('run_id', FilterOperator.EQUALS, [runId]), filterOf('webhook_id', FilterOperator.EQUALS, [webhookId])];
}

function coordinatesOf(c: DeliveryCoordinates): Record<string, string> {
	return {
		ns: c.ns || 'default',
		limit: c.limit === DEFAULT_DELIVERY_LIMIT ? '' : String(c.limit),
		cursor: c.cursor,
		// A page number without a cursor names no page the engine could return.
		page: c.cursor ? String(c.page) : ''
	};
}

function filtersOf(c: DeliveryCoordinates): Record<string, FieldFilter[]> {
	return { filter: c.subscriptionFilters, [DELIVERY_FILTER_PARAM]: c.filters };
}

/** The page's address with the panel's coordinates — listHref's rules (lib/browsing.ts). */
export function deliveriesHref(c: DeliveryCoordinates, changes: Partial<DeliveryCoordinates> = {}, hash = ''): string {
	const next = { ...c, ...changes };
	return listHref('/admin/webhooks', coordinatesOf(next), filtersOf(next)) + (hash ? `#${hash}` : '');
}

/** The sessionStorage key for this filter set's cursor stack. */
export function deliveriesStackKey(c: DeliveryCoordinates): string {
	return pageStackKey('/admin/webhooks', coordinatesOf(c), filtersOf(c));
}

/** `orkctl webhook deliveries …` for the same listing (first page), every value shell-quoted — never -n. */
export function deliveriesCliCommand(c: Pick<DeliveryCoordinates, 'filters' | 'limit'>): string {
	const parts = ['orkctl', 'webhook', 'deliveries', ...filterFlags(c.filters)];
	if (c.limit !== DEFAULT_DELIVERY_LIMIT) parts.push('--limit', String(c.limit));
	return parts.join(' ');
}

/** What the panel's scope sentence says about namespaces: every one, one, or a narrowing. */
export function deliveriesNamespaceScope(filters: FieldFilter[]): string {
	if (!filters.some((f) => f.field === 'namespace')) return 'Showing every namespace';
	const one = equalityOf(filters, 'namespace');
	return one ? `Showing namespace ${one}` : 'Showing the namespaces the namespace filter selects';
}

/** An instant to the millisecond, UTC: what an attempt timeline reads in. */
export function instantMs(ts: Timestamp | undefined): string {
	return ts ? timestampDate(ts).toISOString() : '-';
}

/** An instant to the second, UTC, as the window caption and inputs show it. */
export function instantSeconds(ts: Timestamp | undefined): string {
	return ts ? timestampDate(ts).toISOString().replace(/\.\d{3}Z$/, 'Z') : '';
}

export function millis(d: Duration | undefined): string {
	if (!d) return '0 ms';
	return `${Math.round(Number(d.seconds) * 1000 + d.nanos / 1e6)} ms`;
}

/** The last attempt: its status code, or its error when no response arrived, and when. */
export function lastAttemptLabel(d: Pick<Delivery, 'attempts'>): string {
	const last = d.attempts.at(-1);
	if (!last) return '-';
	const outcome = last.statusCode ? String(last.statusCode) : (last.error || '-').slice(0, 40);
	return last.at ? `${outcome} at ${instantMs(last.at).slice(11)}` : outcome;
}

/** "delivered, 200 in 91 ms", "not delivered, 502 in 77 ms", "not delivered, dial tcp …". */
export function redeliveryOutcome(d: Pick<Delivery, 'delivered' | 'attempts'>): string {
	const verdict = d.delivered ? 'delivered' : 'not delivered';
	const last = d.attempts.at(-1);
	if (!last) return verdict;
	if (!last.statusCode) return `${verdict}, ${last.error || '-'}`;
	return `${verdict}, ${last.statusCode} in ${millis(last.duration)}`;
}

/** The inline result a redelivery leaves on the row it repeated. */
export function redeliveryResult(d: Pick<Delivery, 'id' | 'delivered' | 'attempts'>): string {
	return `Redelivered as ${d.id}: ${redeliveryOutcome(d)}`;
}

/** The confirmation's body: what will happen, before it happens. */
export function redeliverySentence(url: string): string {
	return (
		`OrkStack will POST this event to ${url} once, now, signed with the subscription's current secret ` +
		'and embedding the run as it reads today. Nothing is queued and nothing retries; you are the retry policy.'
	);
}
