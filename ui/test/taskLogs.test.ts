import { expect, test } from 'bun:test';
import { create, toJson } from '@bufbuild/protobuf';
import { TaskLogChunkSchema } from '../src/lib/gen/orkstack/v1/service_pb';
import { followTaskLogs, type LogDelivery, type LogSocketState } from '../src/lib/taskLogs';

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
	static openOnConstruct = true;
	onopen: (() => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onerror: (() => void) | null = null;
	onclose: ((event?: { code?: number; reason?: string }) => void) | null = null;
	closed = false;
	readonly url: string;

	constructor(url: string) {
		this.url = url;
		fakeSocket.opened.push(this);
		if (fakeSocket.openOnConstruct) queueMicrotask(() => this.onopen?.());
	}

	close(event?: { code?: number; reason?: string }): void {
		if (this.closed) return;
		this.closed = true;
		this.onclose?.(event);
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
function follower(
	overrides: { maxRetries?: number } = {}
): {
	deliveries: LogDelivery[];
	states: LogSocketState[];
	failures: string[];
	socket: () => fakeSocket;
	sockets: () => fakeSocket[];
	frame: () => void;
	stop: () => void;
} {
	fakeSocket.opened = [];
	(globalThis as { WebSocket?: unknown }).WebSocket = fakeSocket;

	const deliveries: LogDelivery[] = [];
	const states: LogSocketState[] = [];
	const failures: string[] = [];
	let batch: (() => void) | null = null;
	const stop = followTaskLogs({
		runId: 'ork-1',
		taskId: 'flaky',
		namespace: 'alpha',
		retryBaseMs: 0,
		...overrides,
		schedule: (flush) => {
			batch = flush;
			return () => {
				batch = null;
			};
		},
		onState: (state, detail) => {
			states.push(state);
			if (state === 'failed') failures.push(detail ?? '');
		},
		onLines: (delivery) => deliveries.push(delivery)
	});
	return {
		deliveries,
		states,
		failures,
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

// NO VIEW MAY SPIN FOR EVER. A socket the server will never accept — the
// engine is down, the origin is refused (ORK_WS_ALLOWED_ORIGINS), the run is
// not there — closes exactly like a wifi handover does. An unbounded retry
// loop therefore reads as RECONNECTING for ever and never says what is wrong,
// which is the one thing a failed request must do.
test('a stream that never delivers anything gives up and names the failure', async () => {
	fakeSocket.openOnConstruct = false; // nothing ever accepts the connection
	try {
		const f = follower({ maxRetries: 3 });
		// Every attempt dies the way a pre-upgrade rejection does: an abnormal
		// close, with no code the browser can pass on.
		for (let i = 0; i < 10; i += 1) {
			f.socket().close();
			await new Promise((resolve) => setTimeout(resolve, 2));
		}

		// One connection plus its three retries, and then nothing: the loop is
		// bounded, so a dead engine costs four attempts rather than one every
		// eight seconds until the tab is closed.
		expect(f.sockets().length).toBe(4);
		expect(f.states[f.states.length - 1]).toBe('failed');
		expect(f.failures).toHaveLength(1);
		// And the reason names something a reader can act on.
		expect(f.failures[0]).toContain('could not be opened');
		expect(f.failures[0]).toContain('ORK_WS_ALLOWED_ORIGINS');
		f.stop();
	} finally {
		fakeSocket.openOnConstruct = true;
	}
});

// PROGRESS BUYS THE NEXT RECONNECT. The budget above must not end a follow
// that is working: a long, silent task whose socket drops now and then is the
// normal case, and every drop after a delivered chunk starts from a full
// budget.
test('a delivered chunk resets the retry budget', async () => {
	const f = follower({ maxRetries: 2 });
	for (let round = 1n; round <= 5n; round += 1n) {
		f.socket().deliver({ attempt: 1, seq: round, data: `line ${round}\n` });
		f.socket().close({ code: 1006 });
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
	f.frame();

	// Five drops, five reconnects, and no give-up: each socket delivered.
	expect(f.sockets().length).toBe(6);
	expect(f.states).not.toContain('failed');
	const lines = f.deliveries.flatMap((delivery) => delivery.lines.map((line) => line.text));
	expect(lines).toEqual(['line 1', 'line 2', 'line 3', 'line 4', 'line 5']);
	f.stop();
});

// The failure a server CAN describe is described: a close frame's code and
// reason are the difference between "something went wrong" and a line an
// operator can act on.
test('a socket that opens and then dies without a chunk reports the close it saw', async () => {
	const f = follower({ maxRetries: 2 });
	for (let i = 0; i < 5; i += 1) {
		f.socket().close({ code: 1011, reason: 'task log stream failed' });
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
	expect(f.failures).toHaveLength(1);
	expect(f.failures[0]).toContain('1011');
	expect(f.failures[0]).toContain('task log stream failed');
	expect(f.failures[0]).toContain('without delivering anything');
	f.stop();
});

// THE TWO FILE DESCRIPTORS, TOLD APART IN BAND.
//
// One task attempt has one log stream — one Logger, one sink, one dense
// sequence — so the Orker marks a stderr line with a fixed prefix rather than
// carrying a field the chunk has no room for (pkg/logstream/linewriter.go).
// The browser is one of the readers that has to invert it: a pane that showed
// the marker verbatim would be showing the wire, and a pane that guessed at
// the origin would be re-deriving what the producer already stated.
test('a stderr line is marked and its marker never reaches the reader', () => {
	const f = follower();
	f.socket().deliver({
		attempt: 1,
		seq: 1n,
		data: 'building\n[stderr] warning: no such file\ndone\n'
	});
	f.frame();

	const lines = f.deliveries.flatMap((delivery) => delivery.lines);
	expect(lines.map((line) => line.text)).toEqual([
		'building',
		'warning: no such file',
		'done'
	]);
	expect(lines.map((line) => line.stream)).toEqual(['stdout', 'stderr', 'stdout']);
	f.stop();
});

// The escape is what makes the encoding invertible, and it is the producer's,
// not this module's guess: an application that prints "[stderr] ..." on STDOUT
// travels behind "[stdout] ", so the two cases render differently.
test('stdout that imitates the marker is unescaped and stays stdout', () => {
	const f = follower();
	f.socket().deliver({
		attempt: 1,
		seq: 1n,
		data: '[stdout] [stderr] not really stderr\n[stderr] [stderr] really stderr\n'
	});
	f.frame();

	const lines = f.deliveries.flatMap((delivery) => delivery.lines);
	expect(lines.map((line) => `${line.stream}:${line.text}`)).toEqual([
		'stdout:[stderr] not really stderr',
		'stderr:[stderr] really stderr'
	]);
	f.stop();
});

// A chunk ends on a line boundary, but a LINE does not end on a chunk
// boundary: the marker is decoded where whole lines are made, not where bytes
// arrive, or a marker split across two frames would be shown as text.
test('a marker split across two chunks is still recognised', () => {
	const f = follower();
	f.socket().deliver({ attempt: 1, seq: 1n, data: '[std' });
	f.socket().deliver({ attempt: 1, seq: 2n, data: 'err] split across frames\n' });
	f.frame();

	const lines = f.deliveries.flatMap((delivery) => delivery.lines);
	expect(lines).toHaveLength(1);
	expect(lines[0].text).toBe('split across frames');
	expect(lines[0].stream).toBe('stderr');
	f.stop();
});

// The Orker's own notices are the Orker's voice and carry no marker, so they
// read as stdout and stay notices: a cap must never be dressed up as the
// task's stderr.
test('the truncation notice is a notice, not stderr', () => {
	const f = follower();
	f.socket().deliver({
		attempt: 1,
		seq: 1n,
		data: '[ork] log truncated at 16 MiB\n',
		truncated: true
	});
	f.frame();

	const lines = f.deliveries.flatMap((delivery) => delivery.lines);
	expect(lines).toHaveLength(1);
	expect(lines[0].notice).toBe(true);
	expect(lines[0].stream).toBe('stdout');
	expect(lines[0].text).toBe('[ork] log truncated at 16 MiB');
	f.stop();
});

// Download is an explicit replay, not whatever happened to survive the pane's
// bounded scrollback. It shares the normal stream decoder so the same API
// endpoint, namespace and attempt rules apply to both reads.
test('a retained-log replay requests follow=false and preserves the stored bytes', () => {
	fakeSocket.opened = [];
	(globalThis as { WebSocket?: unknown }).WebSocket = fakeSocket;
	const bytes: Uint8Array[] = [];
	const states: LogSocketState[] = [];
	const stop = followTaskLogs({
		runId: 'ork-1', taskId: 'flaky', namespace: 'alpha', attempt: 2, follow: false,
		onChunk: (chunk) => bytes.push(chunk.slice()),
		onLines: () => {},
		onState: (state) => states.push(state),
		schedule: (flush) => { flush(); return () => {}; }
	});
	const socket = fakeSocket.opened[0];
	expect(new URL(socket.url).searchParams.get('follow')).toBe('false');
	expect(new URL(socket.url).searchParams.get('attempt')).toBe('2');
	socket.deliver({ attempt: 2, seq: 1n, data: '[stdout] retained\n', truncated: true });
	socket.deliver({ attempt: 2, seq: 1n, last: true });
	expect(new TextDecoder().decode(bytes[0])).toBe('[stdout] retained\n');
	expect(states).toContain('ended');
	stop();
});
