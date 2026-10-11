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
