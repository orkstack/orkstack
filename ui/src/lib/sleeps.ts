// The run page's sleeping-task logic, pure and unit-tested. A sleeping task
// starts no container and nothing outside the run resolves it: it waits a
// fixed time on the run's own timer, then succeeds. Every sentence here
// mirrors cmd/orkctl/sleep.go.
import { Status } from './gen/orkstack/v1/common_pb';
import type { TaskSleep } from './gen/orkstack/v1/run_pb';
import { formatRemaining, humanTimeout, type Timestamp } from './awaits';

function toMs(ts: Timestamp): number {
	return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
}

function fromMs(ms: number): Timestamp {
	const seconds = Math.floor(ms / 1000);
	return { seconds: BigInt(seconds), nanos: (ms - seconds * 1000) * 1_000_000 };
}

/**
 * sleepWakeAt is the snapshot's wake-up time when the page has read one, and
 * otherwise TASK_SLEEPING's own timestamp plus the pinned definition's
 * seconds — equal by construction: the engine stamps both from one
 * workflow.Now.
 */
export function sleepWakeAt(
	task: { sleep?: TaskSleep },
	seconds: number,
	sleepingAt: Timestamp | undefined
): Timestamp | undefined {
	if (task.sleep?.wakeAt) return { seconds: task.sleep.wakeAt.seconds, nanos: task.sleep.wakeAt.nanos };
	if (!sleepingAt || seconds <= 0) return undefined;
	return fromMs(toMs(sleepingAt) + seconds * 1000);
}

/** The live REMAINING: `in 9m`, `waking` once the time has passed, `-` with no wake-up time. */
export function sleepRemaining(wakeAt: Timestamp | undefined, now: Timestamp): string {
	if (!wakeAt) return '-';
	const left = toMs(wakeAt) - toMs(now);
	if (left < 0) return 'waking';
	return `in ${formatRemaining(left)}`;
}

/** The panel's one line: what the sleep is doing, or did. */
export function sleepStateLabel(task: { status: Status; seconds: number; opened?: boolean }): string {
	const length = task.seconds > 0 ? humanTimeout(task.seconds) : '';
	switch (task.status) {
		case Status.SLEEPING:
			return length ? `Sleeping for ${length}` : 'Sleeping';
		case Status.SUCCEEDED:
			return length ? `Woke after ${length}` : 'Woke';
		case Status.FAILED:
			return 'Failed';
		case Status.CANCELED:
			return task.opened ? 'Canceled while sleeping' : 'Canceled before it started sleeping';
		case Status.SKIPPED:
			return 'Skipped: it never started sleeping';
		default:
			return length ? `Will sleep ${length} once its needs are complete` : 'Will sleep once its needs are complete';
	}
}

/**
 * The sleep's length for the panel's Length fact, when its state line does not
 * already say it (a failed, canceled or skipped sleep); '' otherwise.
 */
export function sleepLengthWords(task: { status: Status; seconds: number }): string {
	if (task.seconds <= 0) return '';
	return task.status === Status.FAILED || task.status === Status.CANCELED || task.status === Status.SKIPPED ? humanTimeout(task.seconds) : '';
}

/** The wake-up time on the clock a reader reads, 24-hour: `wakes 14:05:00`; '' with none. */
export function wakesWords(wakeAt: Timestamp | undefined): string {
	if (!wakeAt) return '';
	return `wakes ${new Date(toMs(wakeAt)).toLocaleTimeString('en-GB', { hour12: false })}`;
}
