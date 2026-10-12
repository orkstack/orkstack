import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { durationFromMs, timestampFromDate } from '@bufbuild/protobuf/wkt';
import {
	DEFAULT_DELIVERY_LIMIT,
	deliveriesCliCommand,
	deliveriesHref,
	deliveriesNamespaceScope,
	deliveriesStackKey,
	deliveryLimit,
	lastAttemptLabel,
	redeliveryOutcome,
	redeliveryResult,
	redeliverySentence,
	runDeliveriesFilters,
	webhookDeliveriesFilters,
	type DeliveryCoordinates
} from '../src/lib/deliveries';
import { DeliveryAttemptSchema, ListDeliveriesRequestSchema } from '../src/lib/gen/orkstack/v1/webhook_service_pb';
import { parseFilter, specFor, type FieldFilter } from '../src/lib/listFilter';

const base: DeliveryCoordinates = {
	ns: 'default',
	filters: [],
	limit: DEFAULT_DELIVERY_LIMIT,
	cursor: '',
	page: 1,
	subscriptionFilters: []
};

function parsed(...texts: string[]): FieldFilter[] {
	return texts.map((text) => {
		const f = parseFilter(text);
		if ('error' in f) throw new Error(f.error);
		return f;
	});
}

test('the panel offers the ListDeliveries allowlist: the key coordinates, events only a subscription receives', () => {
	const spec = specFor(ListDeliveriesRequestSchema);
	expect(spec.resource).toBe('Delivery');
	expect(spec.fields.map((f) => f.path)).toEqual(['created_at', 'namespace', 'run_id', 'webhook_id', 'event', 'delivered']);
	const event = spec.fields.find((f) => f.path === 'event');
	expect(event?.kind).toBe('enum');
	expect(event?.values).toHaveLength(10);
	expect(event?.values).not.toContain('TASK_LOG');
	expect(event?.values).not.toContain('RUN_CREATED');
	expect(spec.fields.find((f) => f.path === 'delivered')?.kind).toBe('bool');
	expect(spec.fields.find((f) => f.path === 'created_at')?.kind).toBe('time');
});

test('a limit outside 1..200 falls back to the default instead of reaching the engine', () => {
	expect(deliveryLimit('2')).toBe(2);
	expect(deliveryLimit('201')).toBe(DEFAULT_DELIVERY_LIMIT);
	expect(deliveryLimit(null)).toBe(DEFAULT_DELIVERY_LIMIT);
});

test('the address writes ns first, the panel filters as repeated deliveryFilter keys, and drops the page with the cursor', () => {
	const c = { ...base, ns: 'payments', filters: parsed('webhook_id=wh-1', 'delivered=false'), cursor: 'abc', page: 3 };
	expect(deliveriesHref(c)).toBe(
		'/admin/webhooks?ns=payments&deliveryFilter=delivered%3Dfalse&deliveryFilter=webhook_id%3Dwh-1&cursor=abc&page=3'
	);
	expect(deliveriesHref(c, { cursor: '', page: 1 })).toBe(
		'/admin/webhooks?ns=payments&deliveryFilter=delivered%3Dfalse&deliveryFilter=webhook_id%3Dwh-1'
	);
	// The subscriptions table's own filters ride `filter` and survive a panel change.
	const both = { ...c, subscriptionFilters: parsed('url~example') };
	expect(deliveriesHref(both, { filters: [], cursor: '', page: 1 })).toBe('/admin/webhooks?ns=payments&filter=url%7Eexample');
	expect(deliveriesHref(base, { limit: 2 }, 'deliveries')).toBe('/admin/webhooks?ns=default&limit=2#deliveries');
	// A stack key belongs to one filter set, whatever the cursor.
	expect(deliveriesStackKey(c)).toBe(
		'orkstack.pages:/admin/webhooks?ns=payments&deliveryFilter=delivered%3Dfalse&deliveryFilter=webhook_id%3Dwh-1'
	);
	expect(deliveriesStackKey(c)).toBe(deliveriesStackKey({ ...c, cursor: 'xyz', page: 9 }));
	expect(deliveriesStackKey(c)).not.toBe(deliveriesStackKey({ ...c, filters: parsed('webhook_id=wh-1') }));
});

test("a subscription's Deliveries link opens deliveryFilter=webhook_id=<id>", () => {
	const href = deliveriesHref(base, { filters: webhookDeliveriesFilters('wh-4a1c0e2b7d3f8159') }, 'deliveries');
	expect(new URL(href, 'http://x').searchParams.getAll('deliveryFilter')).toEqual(['webhook_id=wh-4a1c0e2b7d3f8159']);
	const original = deliveriesHref(base, { filters: runDeliveriesFilters('ork-1', 'wh-1') });
	expect(new URL(original, 'http://x').searchParams.getAll('deliveryFilter')).toEqual(['run_id=ork-1', 'webhook_id=wh-1']);
});

test('the CLI equivalent is webhook deliveries with every filter shell-quoted and --limit, never -n', () => {
	expect(deliveriesCliCommand(base)).toBe('orkctl webhook deliveries');
	expect(deliveriesCliCommand({ filters: parsed("run_id=ork-'x", 'delivered=false', 'namespace=payments'), limit: 2 })).toBe(
		`orkctl webhook deliveries --filter 'delivered=false' --filter 'namespace=payments' --filter 'run_id=ork-'"'"'x' --limit 2`
	);
});

test('the scope sentence says every namespace unless a namespace filter narrows the panel', () => {
	expect(deliveriesNamespaceScope([])).toBe('Showing every namespace');
	expect(deliveriesNamespaceScope(parsed('namespace=payments'))).toBe('Showing namespace payments');
	expect(deliveriesNamespaceScope(parsed('namespace^pay'))).toBe('Showing the namespaces the namespace filter selects');
});

test('an attempt reads as its status, or its error when no response arrived', () => {
	const at = timestampFromDate(new Date('2026-09-21T02:14:07.412Z'));
	const answered = create(DeliveryAttemptSchema, { number: 4, statusCode: 502, at, duration: durationFromMs(77) });
	const unreachable = create(DeliveryAttemptSchema, { number: 1, error: 'dial tcp 10.0.0.1:443: connection refused' });
	expect(lastAttemptLabel({ attempts: [answered] })).toBe('502 at 02:14:07.412Z');
	expect(lastAttemptLabel({ attempts: [unreachable] })).toBe('dial tcp 10.0.0.1:443: connection refuse');
	expect(lastAttemptLabel({ attempts: [] })).toBe('-');
	expect(redeliveryOutcome({ delivered: false, attempts: [answered] })).toBe('not delivered, 502 in 77 ms');
	expect(redeliveryOutcome({ delivered: false, attempts: [unreachable] })).toBe(
		'not delivered, dial tcp 10.0.0.1:443: connection refused'
	);
	const ok = create(DeliveryAttemptSchema, { number: 1, statusCode: 200, duration: durationFromMs(91) });
	expect(redeliveryResult({ id: 'dlv-new', delivered: true, attempts: [ok] })).toBe(
		'Redelivered as dlv-new: delivered, 200 in 91 ms'
	);
});

test('the confirmation states one POST, now, signed, today’s run, no queue', () => {
	const sentence = redeliverySentence('https://payments.example/hooks');
	for (const fragment of ['https://payments.example/hooks once, now', "current secret", 'as it reads today', 'Nothing is queued', 'you are the retry policy'])
		expect(sentence).toContain(fragment);
});
