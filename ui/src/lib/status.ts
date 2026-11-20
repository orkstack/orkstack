import { Status } from './gen/orkstack/v1/common_pb';
import { EventType } from './gen/orkstack/v1/run_pb';
import type { Timestamp } from '@bufbuild/protobuf/wkt';

export const STATUS_LABEL: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'Unspecified',
	[Status.PENDING]: 'Pending',
	[Status.RUNNING]: 'Running',
	[Status.SUCCEEDED]: 'Succeeded',
	[Status.FAILED]: 'Failed',
	[Status.CANCELED]: 'Canceled',
	[Status.SKIPPED]: 'Skipped',
	[Status.AWAITING]: 'Awaiting',
	[Status.SLEEPING]: 'Sleeping',
	[Status.PAUSED]: 'Paused'
};

export const STATUS_CLASSES: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'bg-unspecified-bg text-unspecified-fg',
	[Status.PENDING]: 'pill-outline bg-pending-bg text-pending-fg',
	[Status.RUNNING]: 'bg-running-bg text-running-fg',
	[Status.SUCCEEDED]: 'bg-succeeded-bg text-succeeded-fg',
	[Status.FAILED]: 'bg-failed-bg text-failed-fg',
	[Status.CANCELED]: 'bg-canceled-bg text-canceled-fg',
	[Status.SKIPPED]: 'pill-outline bg-skipped-bg text-skipped-fg',
	[Status.AWAITING]: 'bg-awaiting-bg text-awaiting-fg',
	[Status.SLEEPING]: 'bg-sleeping-bg text-sleeping-fg',
	// A run status only. It shares waiting-for-capacity's amber (pill-wait):
	// both are a run held from starting new work; its own two-bar mark
	// (statusMarks.ts) and its word tell it apart.
	[Status.PAUSED]: 'pill-wait'
};

export function statusLabel(status: Status | undefined): string {
	return STATUS_LABEL[status ?? Status.UNSPECIFIED];
}

export function statusClasses(status: Status | undefined): string {
	return STATUS_CLASSES[status ?? Status.UNSPECIFIED];
}

/**
 * isTerminal is runStatus.ts's, the one classifier. SKIPPED is terminal:
 * leaving it out once made a skipped task read as still-pending for ever.
 */
export { isTerminal } from './runStatus';

/**
 * taskDispatched reports whether a task ever actually reached an Orker — which
 * is NOT the same question as whether its status is terminal.
 *
 * It relies on one backend guarantee: the engine stamps `TaskRun.scheduled_at`
 * only in `schedule()`, the single place a task is really dispatched. A task
 * the run resolved without ever running it (skipped by its condition, or
 * canceled because the run ended first) carries a `finished_at` and no
 * `scheduled_at`. That absence is the only thing distinguishing "canceled
 * before it started" from "was running, then canceled" — both are
 * `STATUS_CANCELED`, deliberately, since both are the truth about the
 * lifecycle. `started_at` cannot answer it: it is the Orker's own report, and
 * a task canceled while it ran has none.
 *
 * SKIPPED answers false as well, but callers usually want to keep showing its
 * pill: a skip is a decision a reader should see, not an absence.
 *
 * A run recorded before `scheduled_at` existed held the dispatch instant in
 * `started_at`; callers pass `scheduledAt ?? startedAt` so it keeps reading as
 * it always did. A timestamp is evidence, and this function will not fabricate
 * its absence.
 */
export function taskDispatched(
	status: Status | undefined,
	scheduledAt: Timestamp | undefined
): boolean {
	switch (status) {
		case undefined:
		case Status.UNSPECIFIED:
		case Status.PENDING:
		case Status.SKIPPED:
			return false;
		case Status.CANCELED:
			return scheduledAt !== undefined;
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
	[EventType.RUN_CANCELED]: 'RUN_CANCELED',
	[EventType.TASK_AWAITING]: 'TASK_AWAITING',
	[EventType.TASK_RESOLVED]: 'TASK_RESOLVED',
	[EventType.STREAM_GAP]: 'STREAM_GAP',
	[EventType.TASK_SLEEPING]: 'TASK_SLEEPING',
	[EventType.RUN_QUEUED]: 'RUN_QUEUED',
	[EventType.TASK_QUEUED]: 'TASK_QUEUED',
	[EventType.RUN_PAUSED]: 'RUN_PAUSED',
	[EventType.RUN_UNPAUSED]: 'RUN_UNPAUSED'
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

/**
 * keepsEventMessage is whether a row's message survives the terminal re-read
 * when the snapshot carries no error for it: a SKIPPED or CANCELED task is
 * explained only by its TASK_LOG sentence (a join's or a condition's skip, a
 * fail-fast cancel), which the snapshot has no field for.
 */
export function keepsEventMessage(status: Status | undefined): boolean {
	return status === Status.SKIPPED || status === Status.CANCELED;
}
