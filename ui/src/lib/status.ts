import { Status } from './gen/orkstack/v1/common_pb';
import { EventType } from './gen/orkstack/v1/run_pb';
import type { Timestamp } from '@bufbuild/protobuf/wkt';

export const STATUS_LABEL: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'UNSPECIFIED',
	[Status.PENDING]: 'PENDING',
	[Status.RUNNING]: 'RUNNING',
	[Status.SUCCEEDED]: 'SUCCEEDED',
	[Status.FAILED]: 'FAILED',
	[Status.CANCELED]: 'CANCELED',
	[Status.SKIPPED]: 'SKIPPED'
};

export const STATUS_CLASSES: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'border-dotted border-unspecified-border bg-unspecified-bg text-unspecified-fg',
	[Status.PENDING]: 'border-pending-border bg-pending-bg text-pending-fg',
	[Status.RUNNING]: 'border-running-border bg-running-bg text-running-fg',
	[Status.SUCCEEDED]: 'border-succeeded-border bg-succeeded-bg text-succeeded-fg',
	[Status.FAILED]: 'border-failed-border bg-failed-bg text-failed-fg',
	[Status.CANCELED]: 'border-canceled-border bg-canceled-bg text-canceled-fg',
	[Status.SKIPPED]: 'border-dashed border-skipped-border bg-skipped-bg text-skipped-fg'
};

export function statusLabel(status: Status | undefined): string {
	return STATUS_LABEL[status ?? Status.UNSPECIFIED];
}

export function statusClasses(status: Status | undefined): string {
	return STATUS_CLASSES[status ?? Status.UNSPECIFIED];
}

export function isTerminal(status: Status | undefined): boolean {
	return (
		status === Status.SUCCEEDED ||
		status === Status.FAILED ||
		status === Status.CANCELED ||
		// SKIPPED is terminal too: the DAG decided this task will not run, and
		// nothing after that decision moves it. Leaving it out made a skipped
		// task read as still-pending for ever — the run page kept promising
		// "values are not available yet" for a task that will never have any.
		status === Status.SKIPPED
	);
}

/**
 * taskDispatched reports whether a task ever actually reached an Orker — which
 * is NOT the same question as whether its status is terminal.
 *
 * It relies on one backend guarantee: the engine stamps `TaskRun.started_at`
 * only in `schedule()`, the single place a task is really dispatched. A task
 * the run resolved without ever running it (skipped by its condition, or
 * canceled because the run ended first) carries a `finished_at` and no
 * `started_at`. That absence is the only thing distinguishing "canceled
 * before it started" from "was running, then canceled" — both are
 * `STATUS_CANCELED`, deliberately, since both are the truth about the
 * lifecycle.
 *
 * SKIPPED answers false as well, but callers usually want to keep showing its
 * pill: a skip is a decision a reader should see, not an absence.
 *
 * Old runs, archived while the engine still stamped `started_at` on both, read
 * as dispatched and keep their old pill. That degradation is deliberate: a
 * timestamp is evidence, and this function will not fabricate its absence.
 */
export function taskDispatched(
	status: Status | undefined,
	startedAt: Timestamp | undefined
): boolean {
	switch (status) {
		case undefined:
		case Status.UNSPECIFIED:
		case Status.PENDING:
		case Status.SKIPPED:
			return false;
		case Status.CANCELED:
			return startedAt !== undefined;
		default:
			return true;
	}
}

export const EVENT_TYPE_LABEL: Record<EventType, string> = {
	[EventType.UNSPECIFIED]: 'UNSPECIFIED',
	[EventType.RUN_CREATED]: 'RUN_CREATED',
	[EventType.RUN_STARTED]: 'RUN_STARTED',
	[EventType.TASK_SCHEDULED]: 'TASK_SCHEDULED',
	[EventType.TASK_STARTED]: 'TASK_STARTED',
	[EventType.TASK_LOG]: 'TASK_LOG',
	[EventType.TASK_SUCCEEDED]: 'TASK_SUCCEEDED',
	[EventType.TASK_FAILED]: 'TASK_FAILED',
	[EventType.RUN_SUCCEEDED]: 'RUN_SUCCEEDED',
	[EventType.RUN_FAILED]: 'RUN_FAILED',
	[EventType.RUN_CANCELED]: 'RUN_CANCELED'
};

export function eventTypeLabel(t: EventType | undefined): string {
	return EVENT_TYPE_LABEL[t ?? EventType.UNSPECIFIED];
}

export function isTerminalEventType(t: EventType | undefined): boolean {
	return t === EventType.RUN_SUCCEEDED || t === EventType.RUN_FAILED || t === EventType.RUN_CANCELED;
}

export function fmtTimestamp(ts: Timestamp | undefined): string {
	if (!ts) return '—';
	const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
	return new Date(ms).toLocaleString();
}

export function fmtTime(ts: Timestamp | undefined): string {
	if (!ts) return '—';
	const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
	return new Date(ms).toLocaleTimeString();
}
