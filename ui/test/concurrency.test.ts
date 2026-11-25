import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { ConcurrencyQueueCause, ConcurrencyQueuedSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { ConcurrencySpecSchema, ConcurrencyStrategy } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { concurrencyLabel, ordinal, queuedBehind, queuedHeadline, queuedPlace, shortDuration, waitsOnArrival } from '../src/lib/concurrency';

const at = (iso: string) => timestampFromMs(Date.parse(iso));
const base = (cause: ConcurrencyQueueCause) =>
	create(ConcurrencyQueuedSchema, {
		key: 'db-migrate:prod',
		limit: 1,
		position: 2,
		positionKnown: true,
		holdersAhead: ['ork-01'],
		holdersCount: 1,
		queuedSince: at('2026-11-19T10:02:00Z'),
		timesOutAt: at('2026-11-19T12:02:00Z'),
		cause
	});

// The headline is `orkctl run get`'s Queued line up to its parenthesis
// (cmd/orkctl/run_queued.go, queuedLine), for each reason a run waits.
test('the headline says why the run waits, in orkctl run get\'s words', () => {
	const cases: Array<[ConcurrencyQueueCause, string]> = [
		[ConcurrencyQueueCause.ARRIVAL, 'Queued for key db-migrate:prod'],
		[ConcurrencyQueueCause.UNSPECIFIED, 'Queued for key db-migrate:prod'],
		[ConcurrencyQueueCause.RESUME, 'Queued for key db-migrate:prod after a resume'],
		[ConcurrencyQueueCause.UNPAUSE, 'Waiting again for key db-migrate:prod after an unpause'],
		[ConcurrencyQueueCause.FAILING, 'Waiting for its concurrency slot to run its failure handlers (key db-migrate:prod)']
	];
	for (const [cause, want] of cases) expect(queuedHeadline(base(cause))).toBe(want);
});

test('its place is an ordinal when the run could read it, and unknown otherwise', () => {
	expect(queuedPlace(base(ConcurrencyQueueCause.ARRIVAL))).toBe('2nd in line');
	const unknown = base(ConcurrencyQueueCause.ARRIVAL);
	unknown.positionKnown = false;
	unknown.position = 0;
	expect(queuedPlace(unknown)).toBe('place unknown');
});

test('ordinals are English ones, the teens included', () => {
	expect([1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 101, 111, 112].map(ordinal)).toEqual([
		'1st', '2nd', '3rd', '4th', '11th', '12th', '13th', '21st', '22nd', '23rd', '101st', '111th', '112th'
	]);
});

test('behind names the holders shown, how many more there are, and whether they are as of arrival', () => {
	expect(queuedBehind(base(ConcurrencyQueueCause.ARRIVAL))).toEqual({ runs: ['ork-01'], more: 0, asOfArrival: false });
	const many = base(ConcurrencyQueueCause.ARRIVAL);
	many.limit = 3;
	many.holdersAhead = ['ork-01', 'ork-02'];
	many.holdersCount = 3;
	many.holdersAsOfArrival = true;
	expect(queuedBehind(many)).toEqual({ runs: ['ork-01', 'ork-02'], more: 1, asOfArrival: true });
	const none = base(ConcurrencyQueueCause.ARRIVAL);
	none.holdersAhead = [];
	none.holdersCount = 0;
	expect(queuedBehind(none)).toBeNull();
});

// The Orkflow page's Concurrency fact and `orkctl orkflow get`'s Concurrency
// line are one sentence (cmd/orkctl/orkflow.go, concurrencyLabel).
test('a declared limit reads as orkctl orkflow get prints it', () => {
	const spec = create(ConcurrencySpecSchema, {
		limit: 1,
		key: 'db-migrate:{{ .inputs.env }}',
		onLimit: ConcurrencyStrategy.QUEUE,
		queueTimeoutSeconds: 7200n,
		maxQueued: 50
	});
	expect(concurrencyLabel(spec)).toBe('limit 1 on key db-migrate:{{ .inputs.env }}; the rest wait in line up to 2h, at most 50');
	expect(concurrencyLabel(create(ConcurrencySpecSchema, { limit: 2 }))).toBe(
		"limit 2 on this Orkflow's own key; the rest wait in line up to the deployment's default"
	);
	expect(concurrencyLabel(create(ConcurrencySpecSchema, { limit: 3, queueTimeoutSeconds: 1800n }))).toBe(
		"limit 3 on this Orkflow's own key; the rest wait in line up to 30m"
	);
});

// Go's Duration form with the CLI's two trims (cmd/orkctl/schedule.go, shortDuration).
test('a queue timeout reads as the CLI prints it', () => {
	expect([7200, 1800, 3605, 3665, 90, 45, 0, 86400].map(shortDuration)).toEqual([
		'2h', '30m', '1h0m5s', '1h1m5s', '1m30s', '45s', '0s', '24h'
	]);
});

test('another strategy is named as the DSL writes it, and an unknown one by its number', () => {
	expect(concurrencyLabel(create(ConcurrencySpecSchema, { limit: 2, onLimit: ConcurrencyStrategy.CANCEL_OLDEST }))).toBe(
		"limit 2 on this Orkflow's own key; on_limit cancel_oldest"
	);
	expect(concurrencyLabel(create(ConcurrencySpecSchema, { limit: 2, onLimit: 99 as ConcurrencyStrategy }))).toBe(
		"limit 2 on this Orkflow's own key; on_limit 99"
	);
});

test('only a run that arrived to a full key waits on arrival', () => {
	const arrived = [ConcurrencyQueueCause.UNSPECIFIED, ConcurrencyQueueCause.ARRIVAL, ConcurrencyQueueCause.RESUME, ConcurrencyQueueCause.UNPAUSE, ConcurrencyQueueCause.FAILING];
	expect(arrived.map((cause) => waitsOnArrival(base(cause)))).toEqual([true, true, false, false, false]);
});
