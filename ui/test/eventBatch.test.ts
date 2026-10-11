import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { EventSchema, EventType, type Event } from '../src/lib/gen/orkstack/v1/run_pb';
import { applyItemEvent, applyItemEvents, type ItemView } from '../src/lib/each';
import { eventBatcher } from '../src/lib/eventBatch';
import { appendEvent, appendEvents, emptyEventBuffer, eventWindow } from '../src/lib/eventBuffer';
import { watchRunEvents } from '../src/lib/runEvents';

// F-ui-perf-2: a burst of socket frames is applied once per frame — one
// buffer write, one item-array copy per task — with the stream's order kept
// and its duplicates still dropped.

function itemEvent(sequence: number, item: number, status = Status.SUCCEEDED): Event {
	return create(EventSchema, {
		runId: 'ork-1',
		sequence: BigInt(sequence),
		type: EventType.TASK_LOG,
		taskId: 'convert',
		status,
		item,
		message: `item ${item}`
	});
}

test('1000 events in one tick are applied once, in order', () => {
	let flushFrame: (() => void) | undefined;
	const batches: Event[][] = [];
	const batcher = eventBatcher(
		(events) => batches.push(events),
		(flush) => {
			flushFrame = flush;
			return () => (flushFrame = undefined);
		}
	);
	for (let i = 1; i <= 1000; i++) batcher.push(itemEvent(i, i - 1));
	expect(batches).toHaveLength(0);
	flushFrame?.();
	expect(batches).toHaveLength(1);
	expect(batches[0].map((e) => Number(e.sequence))).toEqual(Array.from({ length: 1000 }, (_, i) => i + 1));
	// Nothing queued: a second frame applies nothing.
	flushFrame?.();
	expect(batches).toHaveLength(1);
});

test('flush applies at once (a terminal event), and cancel drops the queue', () => {
	const batches: Event[][] = [];
	let scheduled = 0;
	const batcher = eventBatcher(
		(events) => batches.push(events),
		() => {
			scheduled++;
			return () => {};
		}
	);
	batcher.push(itemEvent(1, 0));
	batcher.push(itemEvent(2, 1));
	expect(scheduled).toBe(1);
	batcher.flush();
	expect(batches.map((b) => b.length)).toEqual([2]);
	batcher.push(itemEvent(3, 2));
	batcher.cancel();
	batcher.flush();
	expect(batches.map((b) => b.length)).toEqual([2]);
});

test('one buffer write for the batch, equal to appending one by one', () => {
	const events = Array.from({ length: 2500 }, (_, i) => itemEvent(i + 1, i));
	let oneByOne = emptyEventBuffer();
	for (const event of events) oneByOne = appendEvent(oneByOne, event, 2000);
	const batched = appendEvents(emptyEventBuffer(), events, 2000);
	expect(batched.total).toBe(oneByOne.total);
	expect(batched.dropped).toBe(oneByOne.dropped);
	expect(batched.events.map((e) => e.sequence)).toEqual(oneByOne.events.map((e) => e.sequence));
	const empty = emptyEventBuffer();
	expect(appendEvents(empty, [])).toBe(empty);
});

test('one item-array copy per batch, equal to applying one by one', () => {
	const events = [itemEvent(1, 2, Status.RUNNING), itemEvent(2, 0), itemEvent(3, 2), itemEvent(4, 5, Status.FAILED)];
	const items: ItemView[] = [{ index: 0, status: Status.PENDING, attempt: 0, message: '' }];
	let oneByOne = items;
	for (const event of events) oneByOne = applyItemEvent(oneByOne, event);
	const original = Array.prototype.slice;
	let copies = 0;
	Array.prototype.slice = function (this: unknown[], ...args: [number?, number?]) {
		copies++;
		return original.apply(this, args);
	} as typeof original;
	let batched: ItemView[];
	try {
		batched = applyItemEvents(items, events);
	} finally {
		Array.prototype.slice = original;
	}
	expect(copies).toBe(1);
	expect(batched).toEqual(oneByOne);
	expect(items).toHaveLength(1);
	const unrelated = [create(EventSchema, { runId: 'ork-1', sequence: 9n, type: EventType.TASK_STARTED, taskId: 'convert' })];
	expect(applyItemEvents(items, unrelated)).toBe(items);
});

test('the inspector draws the newest window and counts the rest', () => {
	const rows = Array.from({ length: 1000 }, (_, i) => i);
	const drawn = eventWindow(rows, 200);
	expect(drawn.rows).toHaveLength(200);
	expect(drawn.rows[0]).toBe(800);
	expect(drawn.earlier).toBe(800);
	expect(eventWindow(rows, 700).earlier).toBe(300);
	expect(eventWindow([1, 2], 200)).toEqual({ rows: [1, 2], earlier: 0 });
});

// The socket's dedupe still holds with a batcher behind it: a resumed stream
// that repeats sequences delivers each event once, in order.
class fakeSocket {
	static last: fakeSocket | undefined;
	onopen: (() => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onerror: (() => void) | null = null;
	onclose: (() => void) | null = null;
	constructor(readonly url: string) {
		fakeSocket.last = this;
	}
	close() {}
	frame(sequence: number) {
		this.onmessage?.({
			data: JSON.stringify({ runId: 'ork-1', sequence: String(sequence), type: 'TASK_LOG', taskId: 'convert', item: sequence, status: 'SUCCEEDED' })
		});
	}
}

test('order and dedupe survive the batch', () => {
	(globalThis as { WebSocket?: unknown }).WebSocket = fakeSocket;
	let flushFrame: (() => void) | undefined;
	const batches: Event[][] = [];
	const batcher = eventBatcher(
		(events) => batches.push(events),
		(flush) => {
			flushFrame = flush;
			return () => {};
		}
	);
	const stop = watchRunEvents('ork-1', '', (event) => batcher.push(event));
	for (const sequence of [1, 2, 3, 2, 3, 4, 1, 5]) fakeSocket.last?.frame(sequence);
	flushFrame?.();
	stop();
	expect(batches).toHaveLength(1);
	expect(batches[0].map((e) => Number(e.sequence))).toEqual([1, 2, 3, 4, 5]);
});
