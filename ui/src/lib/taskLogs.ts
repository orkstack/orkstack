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

export type LogSocketState = 'connecting' | 'open' | 'closed' | 'error' | 'ended' | 'failed';

/** Which of a task's two file descriptors a line came out of. */
export type LogStream = 'stdout' | 'stderr';

// The in-band markers the Orker frames a task's output with
// (pkg/logstream/linewriter.go). One task attempt has ONE log stream — one
// logger, one sink, one dense sequence — so the two file descriptors are told
// apart per line rather than by a field the chunk has no room for.
const STDERR_MARKER = '[stderr] ';
const STDOUT_MARKER = '[stdout] ';

/**
 * decodeStreamLine inverts that wire shape: it reports which descriptor a
 * streamed line came from, and the line the application actually wrote.
 *
 * It is the exact counterpart of `logstream.DecodeStreamLine`
 * (pkg/logstream/linewriter.go), and it is a
 * DECODE rather than a guess — the producer already stated the origin, and a
 * reader that re-derived it from the text would disagree with the CLI reading
 * the same bytes. `[stdout] ` is the escape that makes the encoding
 * invertible: a stdout line that itself begins with a marker travels behind
 * it, so "[stderr] x" printed on stdout and "x" printed on stderr do not
 * render as the same line. A line the Orker did not frame carries no marker
 * and reads as stdout, which is where an unmarked line belongs.
 */
export function decodeStreamLine(line: string): { text: string; stream: LogStream } {
	if (line.startsWith(STDERR_MARKER)) {
		return { text: line.slice(STDERR_MARKER.length), stream: 'stderr' };
	}
	if (line.startsWith(STDOUT_MARKER)) {
		return { text: line.slice(STDOUT_MARKER.length), stream: 'stdout' };
	}
	return { text: line, stream: 'stdout' };
}

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
	/**
	 * stream is the file descriptor the task wrote this line to, as the Orker
	 * marked it — not as this module inferred it. A pane tints stderr; the CLI
	 * prints the marker verbatim. Same information, two renderings.
	 */
	stream: LogStream;
}

export interface LogDelivery {
	lines: LogLine[];
	/** The output passed its cap: the store holds no more of this attempt. */
	truncated: boolean;
	/** The stream ended because the task reached a terminal state. */
	ended: boolean;
	/**
	 * attempt is the try these lines belong to, as the engine resolved it. It
	 * is 0 until the first chunk arrives, because "which attempt" is the
	 * store's answer and not the client's.
	 */
	attempt: number;
	/**
	 * restarted marks the first delivery of a LATER attempt: the task retried,
	 * the engine moved the follow onto the new try, and its numbering begins
	 * again at 1. A pane that appended these to what it already had would
	 * interleave two runs of the same task and repeat every line number.
	 */
	restarted: boolean;
}

export interface FollowTaskLogsOptions {
	runId: string;
	taskId: string;
	/** Wire value: empty is the default namespace. */
	namespace: string;
	/** 0 = the latest attempt that wrote anything. */
	attempt?: number;
	/** false replays the bytes retained so far and then ends; it never follows. */
	follow?: boolean;
	/** Raw retained bytes, for an explicit download. The pane still receives decoded lines. */
	onChunk?: (data: Uint8Array) => void;
	onLines: (delivery: LogDelivery) => void;
	/**
	 * onState reports the socket's state. `failed` is TERMINAL — nothing will
	 * reconnect after it — and carries a reason naming what went wrong, which
	 * is what a view needs to show instead of a spinner.
	 */
	onState?: (state: LogSocketState, detail?: string) => void;
	/**
	 * schedule defers a batch. It exists as a seam so a test can flush
	 * synchronously; the default is an animation frame with a timeout backstop.
	 */
	schedule?: (flush: () => void) => () => void;
	/** Bounded reconnect backoff, exposed for tests. */
	retryBaseMs?: number;
	/**
	 * maxRetries bounds the reconnects that deliver NOTHING. A socket the
	 * server will never accept — the engine is down, the origin is refused
	 * (ORK_WS_ALLOWED_ORIGINS), the run is gone — closes exactly like a wifi
	 * handover does, so an unbounded retry loop shows a reader "reconnecting"
	 * for ever and never says what is wrong. Progress resets the budget: a
	 * connection that delivers a chunk costs nothing against it, so a long
	 * silent follow across real network blips is unaffected.
	 */
	maxRetries?: number;
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
 * describeClose renders what the browser knows about a socket that closed.
 *
 * It is not much — a rejection before the upgrade arrives as an abnormal
 * closure with no status — but a code and a server's reason are the difference
 * between "something went wrong" and a line an operator can act on.
 */
function describeClose(event?: { code?: number; reason?: string }): string {
	const code = event?.code;
	const reason = event?.reason?.trim();
	if (!code && !reason) return 'the connection dropped';
	return `close ${code ?? 'unknown'}${reason ? `: ${reason}` : ''}`;
}

/**
 * followTaskLogs streams one task attempt into onLines and returns the
 * unsubscribe function. It follows until the task is terminal, reconnecting
 * with a resume cursor for as long as it is subscribed.
 */
export function followTaskLogs(opts: FollowTaskLogsOptions): () => void {
	const schedule = opts.schedule ?? scheduleFrame;
	const retryBase = opts.retryBaseMs ?? 500;
	const maxRetries = opts.maxRetries ?? 6;

	let stopped = false;
	let socket: WebSocket | null = null;
	let reconnect: ReturnType<typeof setTimeout> | undefined;
	let cancelBatch: (() => void) | undefined;
	let scheduled = false;
	// retries counts CONSECUTIVE connections that delivered nothing. A chunk
	// resets it, an open does not: a server that accepts the socket and then
	// drops it is exactly as unusable as one that refuses it, and resetting on
	// `open` would retry that pair for ever.
	let retries = 0;
	let everOpened = false;
	let lastClose = 'the connection dropped';

	// The resume cursor, and the only state a reconnect carries — BOTH halves
	// of it. A chunk's seq is dense per ATTEMPT, so a cursor without the
	// attempt it counted is meaningless the moment a task retries: handed back
	// alone after a long first try, it would skip the whole of a shorter
	// second one and the pane would sit empty, live and wrong.
	let lastSeq = 0n;
	let attempt = 0;
	// Bytes decoded but not yet terminated by a newline. A chunk ends on a
	// line boundary EXCEPT where one line exceeded the chunk limit and was
	// split, so the tail of a chunk is not always a whole line.
	let carry = '';
	let decoder = new TextDecoder();
	let numbered = 0;

	let pending: LogLine[] = [];
	let pendingTruncated = false;
	let pendingEnded = false;
	let pendingRestart = false;

	const setState = (s: LogSocketState, detail?: string) => opts.onState?.(s, detail);

	const flush = () => {
		scheduled = false;
		cancelBatch = undefined;
		if (pending.length === 0 && !pendingTruncated && !pendingEnded && !pendingRestart) return;
		const delivery: LogDelivery = {
			lines: pending,
			truncated: pendingTruncated,
			ended: pendingEnded,
			attempt,
			restarted: pendingRestart
		};
		pending = [];
		pendingTruncated = false;
		pendingEnded = false;
		pendingRestart = false;
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

	// Every line the reader will ever see passes through here, which is why
	// the marker is decoded here: a chunk ends on a line boundary but a LINE
	// does not end on a chunk boundary, so a marker split across two frames is
	// only whole once the carry has been joined.
	const emit = (line: string, notice: boolean) => {
		numbered += 1;
		const { text, stream } = decodeStreamLine(line);
		pending.push({ n: numbered, text, notice, stream });
		dirty();
	};

	// startAttempt moves the reader onto a later try of the same task.
	//
	// The engine's follow does this on its own when the task retries: a
	// Temporal activity retry is invisible in the task's status, so the pane
	// would otherwise watch a dead attempt for the rest of the task's life.
	// What arrives is a chunk of attempt N+1 whose seq starts again at 1.
	//
	// Everything the old attempt left is delivered FIRST, on its own, and only
	// then does the restart go out — so a pane clears once, at a known point,
	// rather than losing the tail of the try that failed inside the batch that
	// replaced it.
	const startAttempt = (next: number) => {
		if (carry !== '') {
			emit(carry, false);
			carry = '';
		}
		flushNow();
		attempt = next;
		lastSeq = 0n;
		numbered = 0;
		// A fresh decoder: a multi-byte character half-decoded by the attempt
		// that died is not the first character of the one that replaced it.
		decoder = new TextDecoder();
		pendingRestart = true;
		dirty();
	};

	const consume = (chunk: TaskLogChunk) => {
		// A stream only ever moves FORWARD through a task's attempts. Anything
		// older is a frame from a socket this reader has already left behind.
		if (attempt !== 0 && chunk.attempt < attempt) return;
		if (chunk.attempt > attempt && attempt !== 0) startAttempt(chunk.attempt);
		attempt = chunk.attempt;
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
		const query = new URLSearchParams({ follow: String(opts.follow ?? true), from_seq: lastSeq.toString() });
		// The cursor's other half. Without it the engine cannot tell a resume
		// of THIS attempt from a cursor left over from the one before it
		// (proto StreamTaskLogsRequest.from_attempt).
		if (attempt > 0) query.set('from_attempt', String(attempt));
		if (opts.namespace) query.set('namespace', opts.namespace);
		if (opts.attempt) query.set('attempt', String(opts.attempt));
		const url =
			`${wsBaseUrl()}/ws/runs/${encodeURIComponent(opts.runId)}` +
			`/tasks/${encodeURIComponent(opts.taskId)}/logs?${query.toString()}`;
		socket = new WebSocket(url);

		socket.onopen = () => {
			everOpened = true;
			setState('open');
		};

		socket.onmessage = (msg) => {
			let chunk: TaskLogChunk;
			try {
				chunk = fromJson(TaskLogChunkSchema, JSON.parse(String(msg.data)));
			} catch {
				return; // ignore a frame this build cannot read
			}
			// A frame is PROGRESS, and progress is what buys the next
			// reconnect: this connection is serving, so whatever went wrong
			// before it is over.
			retries = 0;
			// A resumed stream can only move forward. Anything at or before the
			// cursor was already decoded and numbered; replaying it would
			// duplicate lines the reader has already seen. The terminator is
			// exempt: it repeats the cursor's own seq by contract and carries
			// no data, so it is the end of the stream, not a duplicate of it.
			if (!chunk.last && chunk.attempt === attempt && chunk.seq <= lastSeq) return;
			if (chunk.data.length > 0) opts.onChunk?.(chunk.data);
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

		socket.onclose = (event?: { code?: number; reason?: string }) => {
			socket = null;
			if (stopped) return;
			lastClose = describeClose(event);
			if (retries >= maxRetries) {
				// TERMINAL, and it says what happened. A pane that kept
				// reconnecting here would show RECONNECTING for ever against
				// an engine that is down, an origin the engine refuses, or a
				// run that is not there — three fixable things, none of them
				// visible.
				stopped = true;
				setState('failed', failureReason());
				return;
			}
			setState('closed');
			// Resume after the last seq decoded, with a bounded backoff.
			const delay = Math.min(retryBase * 2 ** retries, 8000);
			retries += 1;
			reconnect = setTimeout(connect, delay);
		};
	};

	// failureReason names what a reader can act on. A browser is told very
	// little about a socket that failed — a rejection before the upgrade has no
	// status code here — so the two cases it CAN tell apart are told apart:
	// one that never opened at all, and one that opened and delivered nothing.
	const failureReason = (): string => {
		const attempts = retries + 1;
		return everOpened
			? `The log stream opened and closed ${attempts} times without delivering anything ` +
					`(${lastClose}). The engine may have lost the run's logs, or be failing to read ` +
					`the artifact store.`
			: `The log stream could not be opened after ${attempts} attempts (${lastClose}). ` +
					`The engine may be unreachable, or it may be refusing this page's origin ` +
					`(ORK_WS_ALLOWED_ORIGINS).`;
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
