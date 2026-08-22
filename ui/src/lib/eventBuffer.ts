import { EventType, type Event } from './gen/orkstack/v1/run_pb';

// The run page's event buffer: what the socket has delivered, bounded.
//
// The page keeps ONE subscription to the run's event socket, and its callback
// feeds two consumers — the task/run state reducer, and this buffer. The
// reducer's work is a switch over the newest event and costs nothing to keep
// running; the buffer is what a reader looks at, and it is the half that can
// grow without limit. A run with a few thousand task events used to become a
// few thousand deeply reactive proxies and a few thousand DOM rows, held for
// the life of the tab.
//
// So the buffer holds the last EVENT_BUFFER_CAP events and COUNTS the rest.
// Bounded rendering is the property; a visible count is what keeps it honest.
// The whole stream is the run's history and is read with
// `orkctl run watch --json`, which is what the inspector's header says.

/**
 * EVENT_BUFFER_CAP is how many events the page keeps and may render.
 *
 * It is a cap on the BUFFER, not on the stream: the socket keeps delivering,
 * the reducer keeps reducing, and a run whose events outgrow this still
 * updates its tasks and its status pill.
 */
export const EVENT_BUFFER_CAP = 2000;

export interface EventBuffer {
	/** The tail of the stream, in the engine's order. Never longer than the cap. */
	events: Event[];
	/** How many events fell off the front. Shown; never a silence. */
	dropped: number;
	/** How many the socket delivered in total, dropped ones included. */
	total: number;
}

export function emptyEventBuffer(): EventBuffer {
	return { events: [], dropped: 0, total: 0 };
}

/**
 * appendEvent returns a NEW buffer with the event appended and the oldest
 * dropped once the cap is reached.
 *
 * New, because the page holds this in `$state.raw`: raw state propagates on
 * replacement, so a version of this that pushed in place would render the
 * first event and then nothing — live, and silently wrong.
 */
export function appendEvent(
	buffer: EventBuffer,
	event: Event,
	cap: number = EVENT_BUFFER_CAP
): EventBuffer {
	const next = buffer.events.concat(event);
	const overflow = next.length > cap ? next.length - cap : 0;
	return {
		events: overflow > 0 ? next.slice(overflow) : next,
		dropped: buffer.dropped + overflow,
		total: buffer.total + 1
	};
}

/** The filters the inspector's toolbar offers, in the order it offers them. */
export type EventFilter = 'all' | 'run' | 'tasks' | 'failures';

export const EVENT_FILTERS: { id: EventFilter; label: string }[] = [
	{ id: 'all', label: 'All' },
	{ id: 'run', label: 'Run' },
	{ id: 'tasks', label: 'Tasks' },
	{ id: 'failures', label: 'Failures' }
];

const RUN_LIFECYCLE = new Set<EventType>([
	EventType.RUN_CREATED,
	EventType.RUN_STARTED,
	EventType.RUN_SUCCEEDED,
	EventType.RUN_FAILED,
	EventType.RUN_CANCELED
]);

const TASK_LIFECYCLE = new Set<EventType>([
	EventType.TASK_SCHEDULED,
	EventType.TASK_STARTED,
	EventType.TASK_LOG,
	EventType.TASK_SUCCEEDED,
	EventType.TASK_FAILED
]);

/** isRunLifecycle: an event about the run itself, not about one of its tasks. */
export function isRunLifecycle(event: Event): boolean {
	return RUN_LIFECYCLE.has(event.type);
}

export function isTaskLifecycle(event: Event): boolean {
	return TASK_LIFECYCLE.has(event.type);
}

/**
 * isFailureEvent marks the rows a reader is usually looking for. A canceled
 * run is not one: a cancellation is somebody's decision, and painting it as a
 * failure would hide the failures among the choices.
 */
export function isFailureEvent(event: Event): boolean {
	return event.type === EventType.RUN_FAILED || event.type === EventType.TASK_FAILED;
}

/**
 * matchesFilter is a VIEW of the stream, never a regrouping of it: the
 * inspector filters and styles in place, so cause and effect keep the order
 * the engine numbered them in. A failure therefore appears both under its own
 * filter and under the lifecycle it belongs to — one event, two ways to reach
 * it.
 */
export function matchesFilter(event: Event, filter: EventFilter): boolean {
	switch (filter) {
		case 'run':
			return isRunLifecycle(event);
		case 'tasks':
			return isTaskLifecycle(event);
		case 'failures':
			return isFailureEvent(event);
		default:
			return true;
	}
}
