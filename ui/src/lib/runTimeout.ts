// The run page's run-timeout words, pure and unit-tested. A run's own
// `timeout:` (docs/DSL.md "Run timeout") has a deadline from its start, and
// once it is failing, a hard end after which no failure handler runs. Every
// state here mirrors `orkctl run get`'s Timeout line (cmd/orkctl/run_timeout.go).
import { Status } from './gen/orkstack/v1/common_pb';
import type { RunTimeout } from './gen/orkstack/v1/run_pb';
import { humanTimeout, type Timestamp } from './awaits';
import { isLive } from './runStatus';

/** The reason a task the run timeout stopped is recorded with (docs/REASONS.md). */
export const TASK_STOPPED_BY_RUN_TIMEOUT = 'TASK_STOPPED_BY_RUN_TIMEOUT';

export type RunTimeoutFact = {
	/** The header's value: `6h · 1h12m left`. */
	text: string;
	/** The deadline in full, for the tooltip; '' before the clock starts. */
	tip: string;
	/** Where the clock stands, for the fact's styling. */
	state: 'waiting' | 'counting' | 'passed' | 'finished' | 'timed-out';
};

function toMs(ts: Timestamp): number {
	return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
}

/** Time left as `cmd/orkctl`'s timeLeft spells it: `1h12m`, `2h`, `30m`, `42s`; under a minute rounded up to the second. */
function leftWords(ms: number): string {
	if (ms < 60_000) return `${Math.ceil(ms / 1000)}s`;
	const minutes = Math.floor(ms / 60_000);
	const h = Math.floor(minutes / 60);
	const m = minutes % 60;
	if (h === 0) return `${m}m`;
	return m === 0 ? `${h}h` : `${h}h${m}m`;
}

function clockWords(ms: number): string {
	return new Date(ms).toLocaleTimeString('en-GB', { hour12: false });
}

/**
 * runTimeoutFact is the run header's Timeout fact, or null for a run whose
 * Orkflow declares none. The deadline is the snapshot's; until the page has
 * read one it is RUN_STARTED's own timestamp plus the declared seconds —
 * equal by construction, the engine stamps both from one workflow.Now. A hard
 * end, timed_out and hard_end_reached come only from a snapshot.
 */
export function runTimeoutFact(
	timeout: RunTimeout | undefined,
	status: Status,
	startedAt: Timestamp | undefined,
	now: Timestamp,
	finishedAt?: Timestamp
): RunTimeoutFact | null {
	const seconds = Number(timeout?.seconds ?? 0n);
	if (!timeout || seconds <= 0) return null;
	const limit = humanTimeout(seconds);
	const live = isLive(status);
	// The fallback is for a live page only: a finished run's snapshot is final.
	const deadlineMs = timeout.deadline ? toMs(timeout.deadline) : live && startedAt ? toMs(startedAt) + seconds * 1000 : null;
	// A deadline no Date holds (a malformed answer) is treated as none.
	if (deadlineMs === null || Number.isNaN(new Date(deadlineMs).getTime())) {
		if (timeout.timedOut) return { text: `${limit} · timed out, deadline unknown`, tip: '', state: 'timed-out' };
		return { text: `${limit} · ${live ? 'starts when the run starts' : 'the run never started'}`, tip: '', state: 'waiting' };
	}
	const tip = `Deadline ${new Date(deadlineMs).toISOString()}`;
	let text: string;
	let state: RunTimeoutFact['state'];
	if (timeout.timedOut) {
		text = `${limit} · timed out at ${clockWords(deadlineMs)}`;
		state = 'timed-out';
	} else if (!live) {
		// A run seen ending at or past its deadline whose snapshot does not yet
		// say it timed out (the re-read is on its way) says only that it passed.
		const late = finishedAt !== undefined && toMs(finishedAt) >= deadlineMs;
		text = `${limit} · deadline ${clockWords(deadlineMs)}${late ? ', passed' : ''}`;
		state = 'finished';
	} else {
		const left = deadlineMs - toMs(now);
		// The engine records the timeout at its next workflow task, so a read
		// can land between the deadline and the run's ending.
		text = left > 0 ? `${limit} · ${leftWords(left)} left` : `${limit} · deadline passed`;
		state = left > 0 ? 'counting' : 'passed';
	}
	if (timeout.hardEnd) {
		const hardEnd = clockWords(toMs(timeout.hardEnd));
		if (timeout.hardEndReached) text += ` · failure handlers stopped at ${hardEnd}`;
		else if (live) text += ` · failure handlers stop at ${hardEnd}`;
	}
	return { text, tip, state };
}

/** Whether a row failed because its run's own timeout stopped it. */
export function taskStoppedByRunTimeout(task: { status: Status; error?: { code: string } }): boolean {
	return task.status === Status.FAILED && task.error?.code === TASK_STOPPED_BY_RUN_TIMEOUT;
}
