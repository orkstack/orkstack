import type { MessageInitShape } from '@bufbuild/protobuf';
import { timestampDate, timestampFromDate, type Duration, type Timestamp } from '@bufbuild/protobuf/wkt';
import { DeliveryOutcome, ListDeliveriesRequestSchema, type Delivery } from './gen/orkstack/v1/service_pb';
import { EventType } from './gen/orkstack/v1/run_pb';
import { shellQuote } from './runDetail';
import { RUN_EVENTS, TASK_EVENTS } from './webhooks';

// The Deliveries panel's pure half (Admin → Webhooks): what the URL says, the
// request it becomes, how a record reads, and the orkctl command for the same
// listing. Kept out of the component so each rule is unit-tested once
// (test/deliveries.test.ts) and the panel and `orkctl webhook deliveries` can
// never disagree about a filter.

/** The caption's fixed half: what a record is, and that the body is not in it. */
export const DELIVERIES_CAPTION =
	"One record per settled delivery — every attempt, the signature sent, and the body's SHA-256; the body itself is not stored.";

export const DEFAULT_DELIVERY_LIMIT = 50;
export const MAX_DELIVERY_LIMIT = 200;

/** Every event a subscription can receive, which is every event a record can carry. */
export const DELIVERY_EVENTS: EventType[] = [...RUN_EVENTS, ...TASK_EVENTS];

export const eventName = (event: EventType) => EventType[event] ?? '';

export type DeliveredFilter = '' | 'true' | 'false';

export interface DeliveryCoordinates {
	/** The page's namespace selector, preserved in every address; it does NOT filter the panel. */
	ns: string;
	webhook: string;
	run: string;
	/** The event's short name (RUN_FAILED), or empty for every event. */
	event: string;
	delivered: DeliveredFilter;
	/** The panel's own namespace filter; empty means every namespace. */
	namespace: string;
	since: string;
	until: string;
	limit: number;
	cursor: string;
	page: number;
	/**
	 * The subscriptions table's own filters (its `filter` parameters, in
	 * canonical order), carried through so paging the panel never drops them.
	 * They do not filter the panel.
	 */
	subscriptionFilter?: string[];
}

export function deliveredFilter(value: string | null): DeliveredFilter {
	return value === 'true' || value === 'false' ? value : '';
}

/** limit from the URL: 1..200, anything else is the default. */
export function deliveryLimit(value: string | null): number {
	const parsed = Number(value ?? '');
	return Number.isInteger(parsed) && parsed >= 1 && parsed <= MAX_DELIVERY_LIMIT ? parsed : DEFAULT_DELIVERY_LIMIT;
}

// RFC 3339 with seconds and a zone, or a bare date meaning midnight UTC — both
// of which `orkctl webhook deliveries --since/--until` accepts as an instant.
const BOUND = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2}))?$/;
const SLUG = /^[a-z][a-z0-9-]*$/;

export function deliveryBound(value: string): Timestamp | undefined {
	if (!value) return undefined;
	const iso = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00Z` : value;
	const date = new Date(iso);
	return Number.isNaN(date.getTime()) ? undefined : timestampFromDate(date);
}

/** A filter the engine would refuse is refused here first, never sent as a wider read. */
export function deliveriesFilterError(c: DeliveryCoordinates): string | null {
	if (c.event && !DELIVERY_EVENTS.some((event) => eventName(event) === c.event))
		return `Unknown event ${c.event}: only a subscribable event is ever delivered.`;
	if (c.namespace && (!SLUG.test(c.namespace) || c.namespace.length > 200))
		return 'Namespace must be lowercase letters, digits and hyphens, starting with a letter.';
	if (c.since && (!BOUND.test(c.since) || !deliveryBound(c.since))) return 'Since must be an RFC 3339 instant (or YYYY-MM-DD).';
	if (c.until && (!BOUND.test(c.until) || !deliveryBound(c.until))) return 'Until must be an RFC 3339 instant (or YYYY-MM-DD).';
	const s = deliveryBound(c.since);
	const u = deliveryBound(c.until);
	if (s && u && timestampDate(u).getTime() <= timestampDate(s).getTime()) return 'Until must be after since.';
	return null;
}

/** The ListDeliveries request for these coordinates. */
export function deliveriesRequest(c: DeliveryCoordinates): MessageInitShape<typeof ListDeliveriesRequestSchema> {
	const event = DELIVERY_EVENTS.find((candidate) => eventName(candidate) === c.event) ?? EventType.UNSPECIFIED;
	const outcome =
		c.delivered === 'true'
			? DeliveryOutcome.DELIVERED
			: c.delivered === 'false'
				? DeliveryOutcome.FAILED
				: DeliveryOutcome.UNSPECIFIED;
	return {
		webhookId: c.webhook,
		runId: c.run,
		event,
		outcome,
		namespace: c.namespace,
		since: deliveryBound(c.since),
		until: deliveryBound(c.until),
		pageSize: c.limit === DEFAULT_DELIVERY_LIMIT ? 0 : c.limit,
		pageToken: c.cursor
	};
}

/**
 * The page's address with the panel's coordinates. Only ever `set`s, omits
 * empties and leaves `page` out at 1 — listHref's rules (lib/browsing.ts).
 */
export function deliveriesHref(c: DeliveryCoordinates, changes: Partial<DeliveryCoordinates> = {}, hash = ''): string {
	const next = { ...c, ...changes };
	const params = new URLSearchParams();
	params.set('ns', next.ns || 'default');
	for (const text of next.subscriptionFilter ?? []) params.append('filter', text);
	if (next.webhook) params.set('webhook', next.webhook);
	if (next.run) params.set('run', next.run);
	if (next.event) params.set('event', next.event);
	if (next.delivered) params.set('delivered', next.delivered);
	if (next.namespace) params.set('namespace', next.namespace);
	if (next.since) params.set('since', next.since);
	if (next.until) params.set('until', next.until);
	if (next.limit !== DEFAULT_DELIVERY_LIMIT) params.set('limit', String(next.limit));
	if (next.cursor) params.set('cursor', next.cursor);
	if (next.cursor && next.page > 1) params.set('page', String(next.page));
	return `/admin/webhooks?${params.toString()}${hash ? `#${hash}` : ''}`;
}

/** The sessionStorage key for this filter set's cursor stack. */
export function deliveriesStackKey(c: DeliveryCoordinates): string {
	return `orkstack.pages:${deliveriesHref(c, { cursor: '', page: 1 })}`;
}

/** `orkctl webhook deliveries …` for the same listing (first page), every value shell-quoted. */
export function deliveriesCliCommand(c: DeliveryCoordinates): string {
	const parts = ['orkctl'];
	if (c.namespace) parts.push('-n', shellQuote(c.namespace));
	parts.push('webhook', 'deliveries');
	if (c.webhook) parts.push('--webhook', shellQuote(c.webhook));
	if (c.run) parts.push('--run', shellQuote(c.run));
	if (c.event) parts.push('--event', shellQuote(c.event));
	if (c.delivered) parts.push(`--delivered=${c.delivered}`);
	if (c.since) parts.push('--since', shellQuote(c.since));
	if (c.until) parts.push('--until', shellQuote(c.until));
	if (c.limit !== DEFAULT_DELIVERY_LIMIT) parts.push('--limit', String(c.limit));
	return parts.join(' ');
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
