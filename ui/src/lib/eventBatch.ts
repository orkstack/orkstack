// The run page's event socket, applied once per frame.
//
// A socket frame used to be applied the moment it arrived: a copy of the
// event buffer, a copy of a fan-out's item array, a $state write, a derived
// recomputation and a forced layout in the inspector — PER EVENT. A cached
// fan-out replaying a thousand item events did all of that a thousand times
// before the first paint. Events are now queued and applied together on the
// next animation frame (taskLogs.ts' scheduleFrame, with its timeout backstop
// for a hidden tab), so a frame's worth of events is one buffer append, one
// write per task and one inspector update. Order is kept: a batch is the
// events in the order the socket delivered them, which watchRunEvents has
// already deduplicated by sequence.
import type { Event } from './gen/orkstack/v1/run_pb';
import { scheduleFrame } from './taskLogs';

export interface EventBatcher {
	push(event: Event): void;
	/** Applies whatever is queued now (the terminal event must not wait). */
	flush(): void;
	/** Drops the queue and the pending frame. */
	cancel(): void;
}

export function eventBatcher(
	apply: (events: Event[]) => void,
	schedule: (flush: () => void) => () => void = scheduleFrame
): EventBatcher {
	let queue: Event[] = [];
	let cancelFrame: (() => void) | undefined;
	const flush = () => {
		cancelFrame?.();
		cancelFrame = undefined;
		if (queue.length === 0) return;
		const batch = queue;
		queue = [];
		apply(batch);
	};
	return {
		push(event) {
			queue.push(event);
			cancelFrame ??= schedule(flush);
		},
		flush,
		cancel() {
			cancelFrame?.();
			cancelFrame = undefined;
			queue = [];
		}
	};
}

export interface SlicedApply {
	/** Queues events, applied in order, at most `size` per task. */
	apply(events: Event[]): void;
	/** Applies everything still queued, now. */
	drain(): void;
	/** Drops everything still queued. */
	cancel(): void;
}

// slicedApply applies a long stream in slices of at most `size` events, one
// slice per task (`next`, a zero-delay timeout by default), in order. A page
// replaying a finished run of thousands of tasks otherwise applied thousands
// of events in one task, and a click waited for all of them: between slices
// the page answers.
export function slicedApply(
	apply: (events: Event[]) => void,
	size: number,
	next: (run: () => void) => () => void = (run) => {
		const timer = setTimeout(run, 0);
		return () => clearTimeout(timer);
	}
): SlicedApply {
	let pending: Event[] = [];
	let cancelNext: (() => void) | undefined;
	const run = () => {
		cancelNext = undefined;
		const slice = pending.slice(0, size);
		pending = pending.slice(size);
		if (slice.length > 0) apply(slice);
		if (pending.length > 0) cancelNext = next(run);
	};
	return {
		apply(events) {
			if (events.length === 0) return;
			pending = pending.concat(events);
			cancelNext ??= next(run);
		},
		drain() {
			cancelNext?.();
			cancelNext = undefined;
			const all = pending;
			pending = [];
			if (all.length > 0) apply(all);
		},
		cancel() {
			cancelNext?.();
			cancelNext = undefined;
			pending = [];
		}
	};
}
