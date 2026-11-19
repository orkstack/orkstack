// The run page's pause rules, pure and unit-tested: whether Pause run and
// Unpause run show, what disables them and why, the Ends after choices, and
// the Paused card's words. The server decides every request (PauseRun and
// UnpauseRun refuse with a reason, docs/REASONS.md "Pausing a run"); these
// only keep the page from offering what would be refused, and say why.
import { Status } from './gen/orkstack/v1/common_pb';
import { JoinPolicy } from './gen/orkstack/v1/activity_pb';
import { PauseEnd, type Run, type TaskRun } from './gen/orkstack/v1/run_pb';
import { isTerminal } from './runStatus';

/** One pause control: shown or not, and when shown, why it is disabled. */
export interface PauseControl {
	shown: boolean;
	/** The visible reason it is disabled; '' when it can be pressed. */
	disabled: string;
	/** A run the reason names and links to (the parent whose pause holds it). */
	parentRunId?: string;
}

export interface PauseControls {
	pause: PauseControl;
	unpause: PauseControl;
}

/** What the page knows beyond the run: a request in flight, and permission. */
export interface PauseContext {
	/** A pause or unpause request sent and not yet seen applied. */
	inFlight?: 'pause' | 'unpause';
	/** The caller holds run.pause where the run lives. */
	allowed: boolean;
}

const hidden: PauseControl = { shown: false, disabled: '' };

function isHandler(task: TaskRun): boolean {
	return task.join === JoinPolicy.ANY_FAILED || task.join === JoinPolicy.ALL_DONE;
}

/** hasHandlerToStart reports a failure handler that has not started: one a pause would hold. */
export function hasHandlerToStart(run: Pick<Run, 'taskRuns'>): boolean {
	return run.taskRuns.some((t) => isHandler(t) && t.status === Status.PENDING);
}

/**
 * pauseControls decides both buttons from the run (design §5.7's disabled
 * states). status is the page's live run status, which an event may have
 * moved past the snapshot.
 */
export function pauseControls(run: Run, status: Status, ctx: PauseContext): PauseControls {
	if (isTerminal(status) || !ctx.allowed) return { pause: hidden, unpause: hidden };
	const shown = (why: string): PauseControl => ({ shown: true, disabled: why });
	// The live status decides paused; the snapshot only says by whom, and it
	// can lag the event (a stale pause on a running run offers no Unpause).
	if (status !== Status.PAUSED) {
		let why = '';
		if (ctx.inFlight === 'pause') why = 'Pausing…';
		else if (!run.pausable) why = 'Started before pausing was available';
		else if (run.queued) why = 'Waiting for its concurrency slot';
		else if (status === Status.PENDING) why = 'Not started yet';
		else if (run.error && !hasHandlerToStart(run)) why = 'Failing; its failure handlers have all started';
		return { pause: shown(why), unpause: hidden };
	}
	// A run its parent's pause holds is unpaused by unpausing the parent
	// (UnpauseRun refuses it, RUN_PAUSE_INHERITED), whether or not it has a
	// pause of its own as well.
	const parent = run.pausedByParent?.parentRunId;
	if (parent !== undefined) {
		return {
			pause: hidden,
			unpause: { shown: true, disabled: `Paused because parent run ${parent} is paused`, parentRunId: parent }
		};
	}
	if (run.pause === undefined) return { pause: hidden, unpause: shown('Reading the pause…') };
	return { pause: hidden, unpause: shown(ctx.inFlight === 'unpause' ? 'Unpausing…' : '') };
}

/** One choice of the Pause dialog's Ends after: a label and its length; null sends no timeout. */
export interface EndsAfter {
	label: string;
	seconds: number | null;
}

const DAY = 86_400;

/**
 * endsAfterChoices are the lengths the dialog offers: the deployment's
 * default first (null: send no timeout), then fixed lengths up to 31 days.
 */
export function endsAfterChoices(): EndsAfter[] {
	return [
		{ label: "The deployment's default (7 days unless set)", seconds: null },
		{ label: '1 hour', seconds: 3_600 },
		{ label: '1 day', seconds: DAY },
		{ label: '3 days', seconds: 3 * DAY },
		{ label: '7 days', seconds: 7 * DAY },
		{ label: '14 days', seconds: 14 * DAY },
		{ label: '31 days', seconds: 31 * DAY }
	];
}

/** pausedChildCount is the child runs paused with it: composed tasks still going. */
export function pausedChildCount(tasks: Array<{ childRunId?: string; status: Status | undefined }>): number {
	return tasks.filter((t) => t.childRunId && !isTerminal(t.status ?? Status.UNSPECIFIED)).length;
}

/**
 * pausedLine is the Paused card's status line (design §5.7, "Wording while
 * paused"): what still runs, and whether failure handlers wait.
 */
export function pausedLine(running: number, failing: boolean): string {
	const parts: string[] = [];
	if (running > 0) {
		parts.push(running === 1 ? '1 task still running finishes first' : `${running} tasks still running finish first`);
	} else {
		parts.push('Nothing is running; nothing new starts');
	}
	if (failing) parts.push('its failure handlers wait until it is unpaused');
	return parts.join('; ');
}

/** howLastPauseEnded is the "How it ended" line for a run's last pause. */
export function howLastPauseEnded(end: PauseEnd): string {
	switch (end) {
		case PauseEnd.UNPAUSED:
			return 'Unpaused';
		case PauseEnd.TIMED_OUT:
			return 'Its pause timed out';
		case PauseEnd.RUN_ENDED:
			return 'Ended with the run';
		default:
			return 'Ended';
	}
}
