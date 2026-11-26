import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { ConcurrencyQueueCause, ConcurrencyQueuedSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { ConcurrencySpecSchema, ConcurrencyStrategy } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { ConcurrencyLimitSchema, ConcurrencyRebuild, ConcurrencySlotHolderSchema } from '../src/lib/gen/orkstack/v1/concurrency_service_pb';
import { concurrencyLabel, limitHref, ordinal, queuedBehind, queuedHeadline, queuedPlace, shortDuration, waitsOnArrival } from '../src/lib/concurrency';
import { concurrencyGetCli, concurrencyHref, concurrencyListCli, entryLevel, holderNote, limitState, shownOf, strategyName } from '../src/lib/concurrencyLimits';
import { FilterOperator, filterOf, filtersFromUrl } from '../src/lib/listFilter';

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

// The Concurrency page's State is `orkctl concurrency`'s STATE cell
// (cmd/orkctl/concurrency.go, concurrencyState and renderConcurrencyList).
test('a limit\'s state is the CLI\'s word for it, with why beside it', () => {
	const limit = (fields: Parameters<typeof create<typeof ConcurrencyLimitSchema>>[1]) => limitState(create(ConcurrencyLimitSchema, fields));
	expect(limit({ answering: true, rebuild: ConcurrencyRebuild.NONE })).toEqual({ word: 'OK', tone: 'ok', detail: '' });
	expect(limit({ answering: true })).toEqual({ word: 'OK', tone: 'ok', detail: '' });
	expect(limit({ answering: true, rebuild: ConcurrencyRebuild.READING })).toEqual({ word: 'Reading its holders', tone: 'wait', detail: '' });
	expect(limit({ answering: true, rebuild: ConcurrencyRebuild.SETTLING })).toEqual({ word: 'Reading its holders', tone: 'wait', detail: '' });
	expect(limit({ answering: true, rebuild: ConcurrencyRebuild.FAILING, rebuildFailingSince: at('2026-11-25T01:02:03Z') })).toEqual({
		word: 'Reading its holders',
		tone: 'fail',
		detail: 'failing since 2026-11-25T01:02:03Z'
	});
	// Not answering wins over whatever else the message holds.
	expect(limit({ answering: false, notAnsweringReason: 'the limit did not answer in time', rebuild: ConcurrencyRebuild.READING })).toEqual({
		word: 'Not answering',
		tone: 'fail',
		detail: 'the limit did not answer in time'
	});
});

test('a rebuild this build does not know is not read as fine', () => {
	const state = limitState(create(ConcurrencyLimitSchema, { answering: true, rebuild: 99 as ConcurrencyRebuild }));
	expect(state.tone).not.toBe('ok');
	expect(state.word).toBe('Unknown (99)');
});

test('an entry names its level and strategy as the CLI does, and a stalled holder what must be done', () => {
	expect([entryLevel(false), entryLevel(true)]).toEqual(['run', 'task']);
	expect([ConcurrencyStrategy.QUEUE, ConcurrencyStrategy.REFUSE, ConcurrencyStrategy.SKIP, ConcurrencyStrategy.UNSPECIFIED].map(strategyName)).toEqual([
		'queue',
		'refuse',
		'skip',
		'-'
	]);
	expect(holderNote(create(ConcurrencySlotHolderSchema, { stalled: true }))).toBe('not making progress; an operator must end it');
	expect(holderNote(create(ConcurrencySlotHolderSchema, {}))).toBe('');
});

test('a list shorter than its total says so, and a whole one says nothing', () => {
	expect(shownOf(10, 35, 'holding')).toBe('The first 10 of 35 holding');
	expect(shownOf(10, 11, 'waiting')).toBe('The first 10 of 11 waiting');
	expect(shownOf(3, 3, 'holding')).toBe('');
	// A total that lags its own list never claims fewer than are shown.
	expect(shownOf(3, 2, 'holding')).toBe('');
});

test('a key survives its own address, whatever it holds', () => {
	for (const key of ['ork:orkflow:default/db-migrate', 'a+b=c@d', './/x', 'a b&c=d#e?f%2F', '..', 'é"\'$(x)']) {
		const url = new URL(limitHref('payments', key), 'http://x');
		expect(url.pathname).toBe('/admin/concurrency');
		expect(url.searchParams.get('key')).toBe(key);
		expect(url.searchParams.get('ns')).toBe('payments');
	}
});

test('the list\'s address carries its namespace, filters and cursor, and a page only with a cursor', () => {
	const filters = [filterOf('key', FilterOperator.PREFIX, ['db-'])];
	const url = new URL(concurrencyHref({ ns: 'payments', filters, cursor: 'tok', page: 2 }), 'http://x');
	expect(filtersFromUrl(url)).toEqual({ filters, error: null });
	expect([url.searchParams.get('ns'), url.searchParams.get('cursor'), url.searchParams.get('page')]).toEqual(['payments', 'tok', '2']);
	expect(concurrencyHref({ ns: '', filters: [], cursor: '', page: 3 })).toBe('/admin/concurrency?ns=default');
});

test('the commands are the same reads, every value quoted', () => {
	expect(concurrencyListCli('payments', [filterOf('key', FilterOperator.PREFIX, ['db-'])])).toBe("orkctl -n payments concurrency list -F 'key^db-'");
	expect(concurrencyListCli('', [])).toBe('orkctl -n default concurrency list');
	expect(concurrencyGetCli('payments', "it's $(x)")).toBe(`orkctl -n payments concurrency get 'it'"'"'s $(x)'`);
	// A key that begins with a dash is a key, never a flag.
	expect(concurrencyGetCli('payments', '-x')).toBe('orkctl -n payments concurrency get -- -x');
});
