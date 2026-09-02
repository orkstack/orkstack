import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { EventSchema, EventType, type Event } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	EVENT_BUFFER_CAP,
	appendEvent,
	emptyEventBuffer,
	isFailureEvent,
	matchesFilter
} from '../src/lib/eventBuffer';

// The run page's event buffer, on its own.
//
// A page that held `let events = $state<Event[]>([])` and pushed into it for
// the life of the tab would be bounded only by the run: a long run with many
// tasks grows one deeply reactive proxy and one DOM row per event until the
// tab cannot paint. The cap is the bound, and DROPPING IN SILENCE is the thing a log view
// must never do — so what was dropped is counted, and the count is shown.
//
// This lives beside the page rather than inside it because it is a pure
// function of (buffer, event): a `bun test` can hold it to every property the
// inspector needs without a browser.

function ev(sequence: number, type: EventType, taskId = ''): Event {
	return create(EventSchema, {
		runId: 'ork-1',
		sequence: BigInt(sequence),
		type,
		taskId,
		message: `event ${sequence}`
	});
}

test('the buffer keeps the newest events and counts every one it dropped', () => {
	let buffer = emptyEventBuffer();
	for (let i = 1; i <= 2500; i += 1) buffer = appendEvent(buffer, ev(i, EventType.TASK_LOG), 2000);

	// Bounded: the array a browser renders can never exceed the cap.
	expect(buffer.events).toHaveLength(2000);
	// It is the TAIL that is kept. A cap holding the first 2000 would leave a
	// reader watching a run's opening titles for ever.
	expect(buffer.events[0].sequence).toBe(501n);
	expect(buffer.events[buffer.events.length - 1].sequence).toBe(2500n);
	// And the loss is a number on the screen, not a silence.
	expect(buffer.dropped).toBe(500);
	expect(buffer.total).toBe(2500);
});

test('a buffer under the cap drops nothing and keeps the engine order', () => {
	let buffer = emptyEventBuffer();
	for (const [seq, type] of [
		[1, EventType.RUN_CREATED],
		[2, EventType.RUN_STARTED],
		[3, EventType.TASK_SCHEDULED],
		[4, EventType.TASK_SUCCEEDED],
		[5, EventType.RUN_SUCCEEDED]
	] as const) {
		buffer = appendEvent(buffer, ev(seq, type));
	}
	expect(buffer.dropped).toBe(0);
	expect(buffer.total).toBe(5);
	expect(buffer.events.map((e) => Number(e.sequence))).toEqual([1, 2, 3, 4, 5]);
});

test('the default cap is the one the page renders with', () => {
	expect(EVENT_BUFFER_CAP).toBe(2000);
});

test('the appended buffer is a new value, so a raw state can carry it', () => {
	// $state.raw only propagates on REPLACEMENT: a page that mutated this
	// array in place would render the first event and then nothing, live and
	// silently wrong. Appending returns a new buffer and a new array.
	const before = emptyEventBuffer();
	const after = appendEvent(before, ev(1, EventType.RUN_CREATED));
	expect(after).not.toBe(before);
	expect(after.events).not.toBe(before.events);
	expect(before.events).toHaveLength(0);
});

test('the filters select by kind and preserve chronology', () => {
	const stream = [
		ev(1, EventType.RUN_CREATED),
		ev(2, EventType.TASK_SCHEDULED, 'a'),
		ev(3, EventType.TASK_FAILED, 'a'),
		ev(4, EventType.TASK_SUCCEEDED, 'b'),
		ev(5, EventType.RUN_FAILED)
	];

	const seqs = (filter: 'all' | 'run' | 'tasks' | 'failures') =>
		stream.filter((e) => matchesFilter(e, filter)).map((e) => Number(e.sequence));

	expect(seqs('all')).toEqual([1, 2, 3, 4, 5]);
	// Run lifecycle keeps its own failure: a filter is a view of the stream,
	// not a re-classification of it.
	expect(seqs('run')).toEqual([1, 5]);
	expect(seqs('tasks')).toEqual([2, 3, 4]);
	// Failures cut across both, which is why they are a filter of their own.
	expect(seqs('failures')).toEqual([3, 5]);
});

test('a failure is a failure whether the run or a task failed', () => {
	expect(isFailureEvent(ev(1, EventType.RUN_FAILED))).toBe(true);
	expect(isFailureEvent(ev(2, EventType.TASK_FAILED, 'a'))).toBe(true);
	expect(isFailureEvent(ev(3, EventType.RUN_CANCELED))).toBe(false);
	expect(isFailureEvent(ev(4, EventType.TASK_SUCCEEDED, 'a'))).toBe(false);
});
