// The run page's composed-task logic, pure and unit-tested. A composed task
// (`uses: orkflow:<name>@<version>`) starts a CHILD RUN of another Orkflow and
// reaches no Orker; the page links to that child and rolls its tasks up
// live. Every sentence mirrors cmd/orkctl/compose.go.
import { Status } from './gen/orkstack/v1/common_pb';
import { EventType, type Event, type Run, type TaskChild } from './gen/orkstack/v1/run_pb';
import { withNamespace } from './namespace';
import { statusLabel } from './status';

/** "name@version": the child as the parent pinned it. */
export function childLabel(child: Pick<TaskChild, 'orkflowName' | 'orkflowVersion'> | undefined): string {
	if (!child) return '';
	return `${child.orkflowName}@${child.orkflowVersion}`;
}

/** The child run's page, in the parent's namespace. */
export function childRunHref(namespace: string, runId: string): string {
	return withNamespace(`/runs/${encodeURIComponent(runId)}`, namespace);
}

/** The pinned child Orkflow's page at its version. */
export function childOrkflowHref(namespace: string, name: string, version: string): string {
	return withNamespace(`/orkflows/${encodeURIComponent(name)}?version=${encodeURIComponent(version)}`, namespace);
}

/**
 * A parent's children: the runs list filtered to runs whose origin is this
 * run (run-origin's coordinates, `orkctl run list --origin parent-run:<id>`).
 */
export function childrenListHref(namespace: string, parentRunId: string): string {
	return withNamespace(`/runs?origin=parent-run&originId=${encodeURIComponent(parentRunId)}`, namespace);
}

/** One child task in the roll-up: its id and status. */
export interface ChildTaskRow {
	id: string;
	status: Status;
	/** Set for a child task that itself composes: its own child, not expanded here. */
	child?: TaskChild;
}

/** The roll-up of one child run: every task, in declaration order. */
export interface ChildRollup {
	tasks: ChildTaskRow[];
}

/** childRollup reads a child run's snapshot into its roll-up. */
export function childRollup(run: Pick<Run, 'taskRuns'> | undefined): ChildRollup {
	return {
		tasks: (run?.taskRuns ?? []).map((tr) => ({
			id: tr.taskId,
			status: tr.status,
			...(tr.child ? { child: tr.child } : {})
		}))
	};
}

/**
 * rollupSummary is the panel's one line: "4 tasks: 3 succeeded, 1 running",
 * statuses in a fixed order, a status no task has left out.
 */
export function rollupSummary(rollup: ChildRollup): string {
	const order = [
		Status.SUCCEEDED,
		Status.RUNNING,
		Status.AWAITING,
		Status.PENDING,
		Status.FAILED,
		Status.CANCELED,
		Status.SKIPPED
	];
	const counts = new Map<Status, number>();
	for (const task of rollup.tasks) counts.set(task.status, (counts.get(task.status) ?? 0) + 1);
	const parts = order
		.filter((status) => (counts.get(status) ?? 0) > 0)
		.map((status) => `${counts.get(status)} ${statusLabel(status).toLowerCase()}`);
	const n = rollup.tasks.length;
	const head = `${n} ${n === 1 ? 'task' : 'tasks'}`;
	return parts.length > 0 ? `${head}: ${parts.join(', ')}` : head;
}

const taskEventStatus: Partial<Record<EventType, Status>> = {
	[EventType.TASK_SCHEDULED]: Status.RUNNING,
	[EventType.TASK_STARTED]: Status.RUNNING,
	[EventType.TASK_AWAITING]: Status.AWAITING,
	[EventType.TASK_SUCCEEDED]: Status.SUCCEEDED,
	[EventType.TASK_FAILED]: Status.FAILED
};

/**
 * applyChildEvent moves ONE row of the roll-up from one of the child's own
 * events. A TASK_* event sets its task's status (the event's own, or the
 * type's when the event carries none); a TASK_LOG moves a row only when it
 * carries SKIPPED or CANCELED — the two the engine resolves a task with that
 * way. An item's event, a run event, or a task the roll-up does not hold
 * changes nothing. Returns a new roll-up when something moved, the same one
 * otherwise.
 */
export function applyChildEvent(rollup: ChildRollup, event: Event): ChildRollup {
	if (!event.taskId || event.item !== undefined) return rollup;
	let status: Status | undefined;
	if (event.type === EventType.TASK_LOG) {
		if (event.status === Status.SKIPPED || event.status === Status.CANCELED) status = event.status;
	} else if (event.type in taskEventStatus) {
		status = event.status !== Status.UNSPECIFIED ? event.status : taskEventStatus[event.type];
	}
	if (status === undefined) return rollup;
	const index = rollup.tasks.findIndex((task) => task.id === event.taskId);
	if (index < 0 || rollup.tasks[index].status === status) return rollup;
	const tasks = rollup.tasks.slice();
	tasks[index] = { ...tasks[index], status };
	return { tasks };
}
