// A run's progress: its ended tasks plus its fan-out items, over all of them.
// "Ended" is any terminal status, Skipped and Canceled included, so a failed
// run and a canceled run both read 100% — the ring's colour and the status
// pill beside it say how it ended, never the number. A fan-out counts its
// items in place of itself once they exist (render has 24 items: 24 units,
// not 1); where only its count is known (a runs-list row carries no items)
// its items count as ended exactly when the task has.
import { Status } from './gen/orkstack/v1/common_pb';
import type { TaskRun } from './gen/orkstack/v1/run_pb';
import { isTerminal } from './status';

export interface Progress {
	ended: number;
	total: number;
	/** 0–100, rounded; 100 for a run with nothing left to end. */
	percent: number;
	/** What was counted, for the words: "14 of 30 tasks and items ended". */
	what: 'tasks' | 'tasks and items';
	/** Tasks and items that reached an Orker and ended (succeeded or failed). */
	ran: number;
	/** Tasks and items the DAG decided not to run. */
	skipped: number;
}

/** What progress reads of a task: a TaskRun, or the run page's own view of one. */
export interface Countable {
	status?: Status;
	items: readonly { status?: Status }[];
	each?: Pick<NonNullable<TaskRun['each']>, 'expanded' | 'count'>;
}

export function runProgress(tasks: readonly Countable[]): Progress {
	let ended = 0;
	let total = 0;
	let ran = 0;
	let skipped = 0;
	let items = false;
	const one = (status: Status | undefined, n = 1) => {
		total += n;
		if (isTerminal(status)) ended += n;
		if (status === Status.SUCCEEDED || status === Status.FAILED) ran += n;
		if (status === Status.SKIPPED) skipped += n;
	};
	for (const task of tasks) {
		if (task.items.length > 0) {
			items = true;
			for (const item of task.items) one(item.status);
		} else if (task.each?.expanded && task.each.count > 0) {
			items = true;
			one(task.status, task.each.count);
		} else {
			one(task.status);
		}
	}
	const percent = total === 0 ? 100 : Math.round((100 * ended) / total);
	return { ended, total, percent, what: items ? 'tasks and items' : 'tasks', ran, skipped };
}

/** "14 of 30 tasks and items ended". */
export function progressWords(p: Progress): string {
	return `${p.ended} of ${p.total} ${p.what} ended`;
}
