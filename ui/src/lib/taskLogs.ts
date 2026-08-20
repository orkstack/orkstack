import { fromJson } from '@bufbuild/protobuf';
import { TaskLogChunkSchema, type TaskLogChunk } from './gen/orkstack/v1/service_pb';
import { wsBaseUrl } from './ws';

// Following one task's output, live, in a browser.
//
// The engine serves a task attempt's chunks at
// /ws/runs/{id}/tasks/{task}/logs (internal/api/ws.go, TaskLogSocketPath).
// A chunk carries UTF-8 bytes ending on a line boundary and a dense 1-based
// `seq` per (run, task, attempt), which is BOTH the order and the resume
// token: hand the last seq back as `from_seq` and the stream continues with
// the next chunk, on any replica.
//
// Three properties this module owes the page it feeds, none of which a naive
// `socket.onmessage → lines.push()` has:
//
//   - ORDER is the server's, never the arrival order of a re-render. Lines are
//     numbered here as they are decoded and are only ever appended.
//   - A DROPPED SOCKET resumes rather than restarts. `from_seq` is the last
//     seq decoded, so a reconnect — to this replica or another one — replays
//     nothing twice and misses nothing.
//   - A CHATTY TASK cannot freeze the tab. Lines are handed over in batches on
//     an animation frame, so a task writing thousands of lines a second costs
//     one render per frame instead of one per line.

export type LogSocketState = 'connecting' | 'open' | 'closed' | 'error' | 'ended';

/** One line of a task's output, numbered in the order the engine sent it. */
export interface LogLine {
	/** 1-based position in this task attempt's whole output. Dense. */
	n: number;
	text: string;
	/**
	 * notice marks a line the ORKER wrote about the log rather than a line the
	 * task wrote: today, the truncation notice. It is shown differently so a
	 * cap is never mistaken for output.
	 */
	notice: boolean;
}

export interface LogDelivery {
	lines: LogLine[];
	/** The output passed its cap: the store holds no more of this attempt. */
	truncated: boolean;
	/** The stream ended because the task reached a terminal state. */
	ended: boolean;
}

export interface FollowTaskLogsOptions {
	runId: string;
	taskId: string;
	/** Wire value: empty is the default namespace. */
	namespace: string;
	/** 0 = the latest attempt that wrote anything. */
	attempt?: number;
	onLines: (delivery: LogDelivery) => void;
	onState?: (state: LogSocketState) => void;
	/**
	 * schedule defers a batch. It exists as a seam so a test can flush
	 * synchronously; the default is an animation frame with a timeout backstop.
	 */
	schedule?: (flush: () => void) => () => void;
	/** Bounded reconnect backoff, exposed for tests. */
	retryBaseMs?: number;
}

// A hidden tab gets no animation frames, so rAF alone would let a background
// pane buffer without bound. The backstop flushes anyway, which keeps the
// batch — and therefore memory — bounded by time as well as by frames.
const FRAME_BACKSTOP_MS = 250;

function scheduleFrame(flush: () => void): () => void {
	let fired = false;
	const once = () => {
		if (fired) return;
		fired = true;
		clearTimeout(timer);
		flush();
	};
	const timer = setTimeout(once, FRAME_BACKSTOP_MS);
	if (typeof globalThis.requestAnimationFrame === 'function') {
		globalThis.requestAnimationFrame(once);
	}
	return () => {
		fired = true;
		clearTimeout(timer);
	};
}

/**
 * followTaskLogs streams one task attempt into onLines and returns the
 * unsubscribe function. It follows until the task is terminal, reconnecting
 * with a resume cursor for as long as it is subscribed.
 */
export function followTaskLogs(opts: FollowTaskLogsOptions): () => void {
	const schedule = opts.schedule ?? scheduleFrame;
	const retryBase = opts.retryBaseMs ?? 500;

	let stopped = false;
	let socket: WebSocket | null = null;
	let reconnect: ReturnType<typeof setTimeout> | undefined;
	let cancelBatch: (() => void) | undefined;
	let scheduled = false;
	let retries = 0;

	// The resume cursor, and the only state a reconnect carries.
	let lastSeq = 0n;
	// Bytes decoded but not yet terminated by a newline. A chunk ends on a
	// line boundary EXCEPT where one line exceeded the chunk limit and was
	// split, so the tail of a chunk is not always a whole line.
	let carry = '';
	const decoder = new TextDecoder();
	let numbered = 0;

	let pending: LogLine[] = [];
	let pendingTruncated = false;
	let pendingEnded = false;

	const setState = (s: LogSocketState) => opts.onState?.(s);

	const flush = () => {
		scheduled = false;
		cancelBatch = undefined;
		if (pending.length === 0 && !pendingTruncated && !pendingEnded) return;
		const delivery: LogDelivery = {
			lines: pending,
			truncated: pendingTruncated,
			ended: pendingEnded
		};
		pending = [];
		pendingTruncated = false;
		pendingEnded = false;
		opts.onLines(delivery);
	};

	const dirty = () => {
		if (scheduled) return;
		scheduled = true;
		cancelBatch = schedule(flush);
	};

	// flushNow delivers the batch immediately instead of on the next frame.
	//
	// It exists for exactly one moment: the end of the stream. Batching buys
	// nothing once no more chunks are coming, and deferring the last batch is
	// actively wrong — the caller would be told the stream ENDED a frame before
	// it was given the lines that ended it, and a reader who looked in that
	// frame would see a finished pane one line short of the truth. Measured:
	// the terminator arrives in the same tick as the final data chunk, because
	// the engine writes them back to back.
	const flushNow = () => {
		cancelBatch?.();
		flush();
	};

	const emit = (text: string, notice: boolean) => {
		numbered += 1;
		pending.push({ n: numbered, text, notice });
		dirty();
	};

	const consume = (chunk: TaskLogChunk) => {
		if (chunk.seq > lastSeq) lastSeq = chunk.seq;
		if (chunk.data.length > 0) {
			// stream: true so a multi-byte character split across two chunks is
			// decoded as one character rather than two replacements.
			const merged = carry + decoder.decode(chunk.data, { stream: true });
			const parts = merged.split('\n');
			carry = parts.pop() ?? '';
			for (const part of parts) emit(part.replace(/\r$/, ''), chunk.truncated);
		}
		if (chunk.truncated) {
			// The notice may arrive without a trailing newline; it is still a
			// whole line, and the last one this attempt will ever have.
			if (carry !== '') {
				emit(carry, true);
				carry = '';
			}
			pendingTruncated = true;
			dirty();
		}
		if (chunk.last) {
			if (carry !== '') {
				emit(carry, false);
				carry = '';
			}
			pendingEnded = true;
			dirty();
		}
	};

	const connect = () => {
		if (stopped) return;
		setState('connecting');
		const query = new URLSearchParams({ follow: 'true', from_seq: lastSeq.toString() });
		if (opts.namespace) query.set('namespace', opts.namespace);
		if (opts.attempt) query.set('attempt', String(opts.attempt));
		const url =
			`${wsBaseUrl()}/ws/runs/${encodeURIComponent(opts.runId)}` +
			`/tasks/${encodeURIComponent(opts.taskId)}/logs?${query.toString()}`;
		socket = new WebSocket(url);

		socket.onopen = () => {
			retries = 0;
			setState('open');
		};

		socket.onmessage = (msg) => {
			let chunk: TaskLogChunk;
			try {
				chunk = fromJson(TaskLogChunkSchema, JSON.parse(String(msg.data)));
			} catch {
				return; // ignore a frame this build cannot read
			}
			// A resumed stream can only move forward. Anything at or before the
			// cursor was already decoded and numbered; replaying it would
			// duplicate lines the reader has already seen. The terminator is
			// exempt: it repeats the cursor's own seq by contract and carries
			// no data, so it is the end of the stream, not a duplicate of it.
			if (!chunk.last && chunk.seq <= lastSeq) return;
			consume(chunk);
			if (chunk.last) {
				// Everything first, then the news that there is no more.
				flushNow();
				stopped = true;
				socket?.close();
				setState('ended');
			}
		};

		socket.onerror = () => setState('error');

		socket.onclose = () => {
			socket = null;
			if (stopped) return;
			setState('closed');
			// Resume after the last seq decoded, with a bounded backoff.
			const delay = Math.min(retryBase * 2 ** retries, 8000);
			retries += 1;
			reconnect = setTimeout(connect, delay);
		};
	};

	connect();

	return () => {
		stopped = true;
		clearTimeout(reconnect);
		cancelBatch?.();
		socket?.close();
		socket = null;
	};
}
