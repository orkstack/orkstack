import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { durationFromMs, timestampFromDate } from '@bufbuild/protobuf/wkt';
import {
	DEFAULT_DELIVERY_LIMIT,
	DELIVERY_EVENTS,
	deliveredFilter,
	deliveriesCliCommand,
	deliveriesFilterError,
	deliveriesHref,
	deliveriesRequest,
	deliveriesStackKey,
	deliveryLimit,
	lastAttemptLabel,
	redeliveryOutcome,
	redeliveryResult,
	redeliverySentence,
	type DeliveryCoordinates
} from '../src/lib/deliveries';
import { DeliveryAttemptSchema, DeliveryOutcome } from '../src/lib/gen/orkstack/v1/service_pb';
import { EventType } from '../src/lib/gen/orkstack/v1/run_pb';

const base: DeliveryCoordinates = {
	ns: 'default',
	webhook: '',
	run: '',
	event: '',
	delivered: '',
	namespace: '',
	since: '',
	until: '',
	limit: DEFAULT_DELIVERY_LIMIT,
	cursor: '',
	page: 1
};

test('the panel offers exactly the eight subscribable events, never TASK_LOG or RUN_CREATED', () => {
	expect(DELIVERY_EVENTS.length).toBe(8);
	expect(DELIVERY_EVENTS).not.toContain(EventType.TASK_LOG);
	expect(DELIVERY_EVENTS).not.toContain(EventType.RUN_CREATED);
});

test('URL values outside their domain fall back instead of reaching the engine', () => {
	expect(deliveredFilter('true')).toBe('true');
	expect(deliveredFilter('false')).toBe('false');
	expect(deliveredFilter('maybe')).toBe('');
	expect(deliveryLimit('2')).toBe(2);
	expect(deliveryLimit('201')).toBe(DEFAULT_DELIVERY_LIMIT);
	expect(deliveryLimit(null)).toBe(DEFAULT_DELIVERY_LIMIT);
});

test('a filter the engine would refuse is refused locally', () => {
	expect(deliveriesFilterError(base)).toBeNull();
	expect(deliveriesFilterError({ ...base, event: 'TASK_LOG' })).toContain('Unknown event');
	expect(deliveriesFilterError({ ...base, namespace: 'Bad Name' })).toContain('Namespace');
	expect(deliveriesFilterError({ ...base, since: 'yesterday' })).toContain('Since');
	expect(deliveriesFilterError({ ...base, since: '2026-09-21', until: '2026-09-20' })).toBe('Until must be after since.');
});

test('the request carries every filter; an empty namespace means every namespace', () => {
	const request = deliveriesRequest({
		...base,
		webhook: 'wh-4a1c0e2b7d3f8159',
		run: 'ork-1',
		event: 'RUN_FAILED',
		delivered: 'false',
		since: '2026-09-21',
		limit: 2,
		cursor: 'tok'
	});
	expect(request.webhookId).toBe('wh-4a1c0e2b7d3f8159');
	expect(request.runId).toBe('ork-1');
	expect(request.event).toBe(EventType.RUN_FAILED);
	expect(request.outcome).toBe(DeliveryOutcome.FAILED);
	expect(request.namespace).toBe('');
	expect(request.pageSize).toBe(2);
	expect(request.pageToken).toBe('tok');
	expect(Number(request.since?.seconds)).toBe(Date.UTC(2026, 8, 21) / 1000);
	expect(request.until).toBeUndefined();
	expect(deliveriesRequest({ ...base, delivered: 'true' }).outcome).toBe(DeliveryOutcome.DELIVERED);
	expect(deliveriesRequest(base).pageSize).toBe(0);
});

test('the address sets each key once, keeps ns first, and drops the page with the cursor', () => {
	const c = { ...base, ns: 'payments', webhook: 'wh-1', delivered: 'false' as const, cursor: 'abc', page: 3 };
	expect(deliveriesHref(c)).toBe('/admin/webhooks?ns=payments&webhook=wh-1&delivered=false&cursor=abc&page=3');
	expect(deliveriesHref(c, { event: 'RUN_FAILED', cursor: '', page: 1 })).toBe(
		'/admin/webhooks?ns=payments&webhook=wh-1&event=RUN_FAILED&delivered=false'
	);
	expect(deliveriesHref(base, { webhook: 'wh-1' }, 'deliveries')).toBe('/admin/webhooks?ns=default&webhook=wh-1#deliveries');
	expect(deliveriesStackKey(c)).toBe('orkstack.pages:/admin/webhooks?ns=payments&webhook=wh-1&delivered=false');
});

test('the CLI equivalent repeats every filter, shell-quoted, with -n only for a namespace filter', () => {
	expect(deliveriesCliCommand(base)).toBe('orkctl webhook deliveries');
	expect(
		deliveriesCliCommand({ ...base, ns: 'ignored', namespace: 'payments', run: "ork-'x", delivered: 'false', limit: 2 })
	).toBe(`orkctl -n 'payments' webhook deliveries --run 'ork-'"'"'x' --delivered=false --limit 2`);
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
