import { env } from '$env/dynamic/public';
import { fromJson, type JsonObject, type JsonValue } from '@bufbuild/protobuf';
import { EventSchema, type Event } from './gen/orkstack/v1/run_pb';
import { isTerminalEventType } from './status';

export type RunSocketState = 'connecting' | 'open' | 'closed' | 'error';

function wsBaseUrl(): string {
	if (env.PUBLIC_ENGINE_WS_URL) return env.PUBLIC_ENGINE_WS_URL.replace(/\/$/, '');
	const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
	return `${scheme}://${location.hostname}:8080`;
}

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

// watchRunEvents subscribes to the engine's WebSocket bridge for a run. It
// replays the backlog (from_sequence=0), delivers every event in order,
// dedupes by sequence, and reconnects with a resume cursor until the run
// reaches a terminal event. Returns an unsubscribe function.
//
// namespace is where the run is addressed. It travels as a query parameter
// because the path is the run's address and a socket has no request body; the
// bridge answers 404 for a run of another namespace, on the same rule as the
// RPC, and an empty value means the default namespace.
export function watchRunEvents(
	runId: string,
	namespace: string,
	onEvent: (event: Event) => void,
	onState?: (state: RunSocketState) => void
): () => void {
	let stopped = false;
	let socket: WebSocket | null = null;
	let lastSequence = 0n;
	let retries = 0;

	const setState = (s: RunSocketState) => onState?.(s);

	const connect = () => {
		if (stopped) return;
		setState('connecting');
		const query = new URLSearchParams({ from_sequence: lastSequence.toString() });
		if (namespace) query.set('namespace', namespace);
		const url = `${wsBaseUrl()}/ws/runs/${encodeURIComponent(runId)}?${query.toString()}`;
		socket = new WebSocket(url);

		socket.onopen = () => {
			retries = 0;
			setState('open');
		};

		socket.onmessage = (msg) => {
			let event: Event;
			try {
				event = fromJson(EventSchema, withProtoEnumNames(JSON.parse(String(msg.data))));
			} catch {
				return; // ignore malformed frames
			}
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

		socket.onclose = () => {
			socket = null;
			if (stopped) return;
			setState('closed');
			// Resume from the last delivered sequence with a bounded backoff.
			const delay = Math.min(1000 * 2 ** retries, 8000);
			retries += 1;
			setTimeout(connect, delay);
		};
	};

	connect();

	return () => {
		stopped = true;
		socket?.close();
		socket = null;
	};
}
