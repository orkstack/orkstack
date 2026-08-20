import { expect, test } from 'bun:test';
import { create, toJson } from '@bufbuild/protobuf';
import { TaskLogChunkSchema } from '../src/lib/gen/orkstack/v1/service_pb';
import { followTaskLogs, type LogDelivery } from '../src/lib/taskLogs';

// The client half of "a task that retries is still followed".
//
// The engine moves a follow onto a task's next attempt on its own
// (internal/api/logs.go): a Temporal activity retry is invisible in the task's
// status, so a stream that stayed on the attempt it resolved at connect time
// would deliver nothing, and say it was live, for the rest of the task's life.
// What reaches the browser is a chunk of attempt N+1 whose seq starts again at
// 1 — and a client that did not notice would drop every one of those chunks as
// a replay of numbers it had already seen.
//
// These run under `bun test` (`bun run test:unit`), with no browser: the module
// under test talks to a WebSocket and a scheduler, and both are seams.

/** fakeSocket is the browser's WebSocket, as far as this module needs one. */
class fakeSocket {
	static opened: fakeSocket[] = [];
	onopen: (() => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onerror: (() => void) | null = null;
	onclose: (() => void) | null = null;
	closed = false;
	readonly url: string;

	constructor(url: string) {
		this.url = url;
		fakeSocket.opened.push(this);
		queueMicrotask(() => this.onopen?.());
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.onclose?.();
	}

	/** deliver hands over one chunk exactly as the engine's bridge encodes it. */
	deliver(chunk: {
		attempt: number;
		seq: bigint;
		data?: string;
		truncated?: boolean;
		last?: boolean;
	}): void {
		const message = create(TaskLogChunkSchema, {
			runId: 'ork-1',
			taskId: 'flaky',
			attempt: chunk.attempt,
			seq: chunk.seq,
			data: new TextEncoder().encode(chunk.data ?? ''),
			truncated: chunk.truncated ?? false,
			last: chunk.last ?? false
		});
		this.onmessage?.({ data: JSON.stringify(toJson(TaskLogChunkSchema, message)) });
	}
}

/**
 * follower is one subscription with its two seams held: the socket it opens
 * and the frame it batches on. Nothing here is timing-dependent — a batch is
 * delivered when the test says so, which is what an animation frame does.
 */
function follower(): {
	deliveries: LogDelivery[];
	socket: () => fakeSocket;
	sockets: () => fakeSocket[];
	frame: () => void;
	stop: () => void;
} {
	fakeSocket.opened = [];
	(globalThis as { WebSocket?: unknown }).WebSocket = fakeSocket;

	const deliveries: LogDelivery[] = [];
	let batch: (() => void) | null = null;
	const stop = followTaskLogs({
		runId: 'ork-1',
		taskId: 'flaky',
		namespace: 'alpha',
		retryBaseMs: 0,
		schedule: (flush) => {
			batch = flush;
			return () => {
				batch = null;
			};
		},
		onLines: (delivery) => deliveries.push(delivery)
	});
	return {
		deliveries,
		socket: () => fakeSocket.opened[fakeSocket.opened.length - 1],
		sockets: () => fakeSocket.opened,
		frame: () => {
			const run = batch;
			batch = null;
			run?.();
		},
		stop
	};
}

test('a retry replaces the pane rather than appending to it', () => {
	const f = follower();
	f.socket().deliver({ attempt: 1, seq: 1n, data: 'try one line 1\ntry one line 2\n' });
	f.frame();
	expect(f.deliveries.length).toBe(1);
	expect(f.deliveries[0].attempt).toBe(1);
	expect(f.deliveries[0].restarted).toBe(false);
	expect(f.deliveries[0].lines.map((line) => line.text)).toEqual([
		'try one line 1',
		'try one line 2'
	]);

	// The task retries. seq starts again at 1 — a client that deduplicated on
	// seq alone would discard this and show a live, empty, permanently stale
	// pane.
	f.socket().deliver({ attempt: 2, seq: 1n, data: 'try two line 1\n' });
	f.frame();

	const restart = f.deliveries.find((delivery) => delivery.restarted);
	if (!restart) throw new Error('the retry was never reported as a restart');
	expect(restart.attempt).toBe(2);
	// Numbering begins again with the attempt, so a pane keyed on it can only
	// be correct if it cleared: two attempts appended would repeat every key.
	const afterRestart = f.deliveries.slice(f.deliveries.indexOf(restart));
	const lines = afterRestart.flatMap((delivery) => delivery.lines);
	expect(lines.map((line) => line.text)).toEqual(['try two line 1']);
	expect(lines[0].n).toBe(1);
	f.stop();
});

test('the tail of the attempt that failed is delivered before the restart', () => {
	const f = follower();
	// A chunk that does not end on a newline: the last line of the try that
	// died is still a line, and it must reach the reader before the pane is
	// cleared for its replacement.
	f.socket().deliver({ attempt: 1, seq: 1n, data: 'panic: boom' });
	f.socket().deliver({ attempt: 2, seq: 1n, data: 'try two\n' });
	f.frame();

	const texts = f.deliveries.flatMap((delivery) =>
		delivery.lines.map((line) => `${delivery.restarted ? 'after' : 'before'}:${line.text}`)
	);
	expect(texts).toEqual(['before:panic: boom', 'after:try two']);
	f.stop();
});

test('a reconnect after a retry resumes the new attempt, not the dead one', async () => {
	const f = follower();
	for (let seq = 1n; seq <= 20n; seq += 1n) {
		f.socket().deliver({ attempt: 1, seq, data: `try one line ${seq}\n` });
	}
	f.frame();
	f.socket().deliver({ attempt: 2, seq: 1n, data: 'try two line 1\n' });
	f.frame();

	// The socket dies, as a wifi handover kills one.
	f.socket().close();
	await new Promise((resolve) => setTimeout(resolve, 10));

	expect(f.sockets().length).toBe(2);
	const first = new URL(f.sockets()[0].url);
	expect(first.searchParams.get('from_seq')).toBe('0');
	expect(first.searchParams.get('from_attempt')).toBe(null);

	const resumed = new URL(f.sockets()[1].url);
	// 1, not 20: the cursor belongs to attempt 2 now, whose numbering starts
	// again. Handing back 20 without naming the attempt would ask the engine
	// for chunks past the end of a try that has written one.
	expect(resumed.searchParams.get('from_seq')).toBe('1');
	expect(resumed.searchParams.get('from_attempt')).toBe('2');
	expect(resumed.searchParams.get('namespace')).toBe('alpha');
	f.stop();
});

test('a resumed stream still drops a chunk it has already seen', () => {
	const f = follower();
	f.socket().deliver({ attempt: 1, seq: 1n, data: 'one\n' });
	f.socket().deliver({ attempt: 1, seq: 2n, data: 'two\n' });
	// A replay of the same attempt's chunks: the resume contract says nothing
	// at or before the cursor is delivered twice.
	f.socket().deliver({ attempt: 1, seq: 1n, data: 'one\n' });
	f.socket().deliver({ attempt: 1, seq: 2n, data: 'two\n' });
	f.frame();

	const lines = f.deliveries.flatMap((delivery) => delivery.lines.map((line) => line.text));
	expect(lines).toEqual(['one', 'two']);
	f.stop();
});
