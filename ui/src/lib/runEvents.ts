import { fromJson, type JsonObject, type JsonValue } from '@bufbuild/protobuf';
import { EventSchema, type Event } from './gen/orkstack/v1/run_pb';
import { isTerminalEventType } from './status';
import { describeClose } from './taskLogs';
import { wsBaseUrl } from './ws';

// 'failed' is TERMINAL: the stream stopped reconnecting, and the detail says why.
export type RunSocketState = 'connecting' | 'open' | 'closed' | 'error' | 'failed';

// The bridge writes enum values without their proto prefix ("RUNNING"), while
// the generated decoder only knows the declared names. Restore the prefix on
// the way in; frames that still use the long form pass through untouched.
function withProtoEnumNames(frame: JsonValue): JsonValue {
	if (typeof frame !== 'object' || frame === null || Array.isArray(frame)) return frame;
	const restored: JsonObject = { ...frame };
	if (typeof restored.type === 'string' && !restored.type.startsWith('EVENT_TYPE_')) {
		restored.type = `EVENT_TYPE_${restored.type}`;
	}
	if (typeof restored.status === 'string' && !restored.status.startsWith('STATUS_')) {
		restored.status = `STATUS_${restored.status}`;
	}
	return restored;
}

export interface WatchRunEventsOptions {
	/**
	 * Resume after this sequence rather than replaying from 0: what Retry
	 * passes after a stream stopped, so the events already applied are not
	 * delivered twice.
	 */
	fromSequence?: bigint;
	/**
	 * maxRetries bounds the reconnects that deliver NO frame — taskLogs.ts'
	 * rule, for its reason: a socket the server will never serve (the engine
	 * down, the origin refused, the run gone) closes exactly like a network
	 * blip, and an unbounded loop shows CONNECTING for ever without saying
	 * what is wrong. A delivered frame resets it; an open does not, because a
	 * server that accepts and then drops is as unusable as one that refuses.
	 */
	maxRetries?: number;
	/** The first reconnect delay; each one after doubles it, up to 8 s. */
	retryBaseMs?: number;
	/** The reconnect timer, a seam for tests. */
	schedule?: (fn: () => void, ms: number) => unknown;
	cancel?: (handle: unknown) => void;
}

// watchRunEvents subscribes to the engine's WebSocket bridge for a run. It
// replays the backlog (from_sequence=0, or opts.fromSequence), delivers every
// event in order, dedupes by sequence, and reconnects with a resume cursor
// until the run reaches a terminal event — or until maxRetries reconnects in a
// row delivered nothing, when it stops for good with state 'failed' and a
// reason naming what the browser could tell. Returns an unsubscribe function.
//
// namespace is where the run is addressed. It travels as a query parameter
// because the path is the run's address and a socket has no request body; the
// bridge answers 404 for a run of another namespace, on the same rule as the
// RPC, and an empty value means the default namespace.
export function watchRunEvents(
	runId: string,
	namespace: string,
	onEvent: (event: Event) => void,
	onState?: (state: RunSocketState, detail?: string) => void,
	opts: WatchRunEventsOptions = {}
): () => void {
	const maxRetries = opts.maxRetries ?? 6;
	const retryBase = opts.retryBaseMs ?? 1000;
	const schedule = opts.schedule ?? ((fn, ms) => setTimeout(fn, ms));
	const cancel = opts.cancel ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>));
	let stopped = false;
	let socket: WebSocket | null = null;
	let reconnect: unknown = undefined;
	let lastSequence = opts.fromSequence ?? 0n;
	// CONSECUTIVE connections that delivered no frame.
	let retries = 0;
	let everOpened = false;
	let lastClose = 'the connection dropped';

	const setState = (s: RunSocketState, detail?: string) => onState?.(s, detail);

	const failureReason = (): string => {
		const attempts = retries + 1;
		return everOpened
			? `the event stream opened and closed ${attempts} times without delivering anything (${lastClose})`
			: `the event stream could not be opened after ${attempts} attempts (${lastClose}); the engine may be ` +
					`unreachable, or refusing this page's origin (ORK_WS_ALLOWED_ORIGINS)`;
	};

	const connect = () => {
		if (stopped) return;
		setState('connecting');
		const query = new URLSearchParams({ from_sequence: lastSequence.toString() });
		if (namespace) query.set('namespace', namespace);
		const url = `${wsBaseUrl()}/ws/runs/${encodeURIComponent(runId)}?${query.toString()}`;
		socket = new WebSocket(url);

		socket.onopen = () => {
			everOpened = true;
			setState('open');
		};

		socket.onmessage = (msg) => {
			let event: Event;
			try {
				event = fromJson(EventSchema, withProtoEnumNames(JSON.parse(String(msg.data))));
			} catch {
				return; // ignore malformed frames
			}
			// A frame is progress: this connection is serving.
			retries = 0;
			if (event.sequence <= lastSequence) return; // dedupe on resume
			lastSequence = event.sequence;
			onEvent(event);
			if (isTerminalEventType(event.type)) {
				stopped = true;
				socket?.close();
				setState('closed');
			}
		};

		socket.onerror = () => setState('error');

		socket.onclose = (event?: { code?: number; reason?: string }) => {
			socket = null;
			if (stopped) return;
			lastClose = describeClose(event);
			if (retries >= maxRetries) {
				// TERMINAL, and it says why: nothing reconnects after this.
				stopped = true;
				setState('failed', failureReason());
				return;
			}
			setState('closed');
			// Resume from the last delivered sequence with a bounded backoff.
			const delay = Math.min(retryBase * 2 ** retries, 8000);
			retries += 1;
			reconnect = schedule(connect, delay);
		};
	};

	connect();

	return () => {
		stopped = true;
		if (reconnect !== undefined) cancel(reconnect);
		socket?.close();
		socket = null;
	};
}
