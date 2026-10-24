import { Status } from './gen/orkstack/v1/common_pb';
import { AttemptOutcome, type TaskAttempt } from './gen/orkstack/v1/run_pb';
import { formatDuration } from './runDetail';
import { isTerminal } from './status';

// WHERE A TASK'S TIME WENT, DERIVED IN ONE PLACE.
//
// This is the browser's copy of the task-timing derivation — the one
// `orkctl run timing` implements in cmd/orkctl/timing.go — and it returns the
// same seven reasons, spelled the same way. Given a task's snapshot
// (S = scheduled_at, the engine's dispatch instant; B = started_at, the
// Orker's start of the attempt that decided the task; F = finished_at) and
// its attempt records:
//
//   wait  = B − S      run = F − B      total = F − S
//
// Every quantity is one of three things, and they are never collapsed:
//
//   - 'na'           it does not apply (never scheduled, or not finished yet);
//   - 'unavailable'  it happened and the evidence is gone or inconsistent,
//                    with the reason — never drawn as 0;
//   - 'value'        two timestamps some process actually recorded.

/** A protobuf Timestamp, or the page's own `now` in the same shape. */
export type Timestamp = { seconds: bigint; nanos: number };

export const TIMING_REASON = {
	noOrkerStart: 'no orker reported a start',
	clockSkew: 'clock skew',
	preContract: 'recorded before scheduled time existed',
	reclaimed: 'attempt records reclaimed',
	noRecord: 'no attempt record',
	noPhases: 'record predates phase spans',
	noStore: 'no task attempt store'
} as const;

export type TimingReason = (typeof TIMING_REASON)[keyof typeof TIMING_REASON];

export type Quantity =
	| { kind: 'na' }
	| { kind: 'unavailable'; reason: TimingReason }
	| { kind: 'value'; from: Timestamp; to: Timestamp; ms: number };

const NA: Quantity = { kind: 'na' };

function unavailable(reason: TimingReason): Quantity {
	return { kind: 'unavailable', reason };
}

/** Milliseconds between two timestamps, at full precision (nanos kept). */
function diffMs(from: Timestamp, to: Timestamp): number {
	return Number(to.seconds - from.seconds) * 1000 + (to.nanos - from.nanos) / 1e6;
}

/** span is to − from, or unavailable(clock skew) when the ends are inverted. */
export function span(from: Timestamp, to: Timestamp): Quantity {
	const ms = diffMs(from, to);
	if (!Number.isFinite(ms) || ms < 0) return unavailable(TIMING_REASON.clockSkew);
	return { kind: 'value', from, to, ms };
}

/** The snapshot fields the derivation reads, as TaskRun and the page's TaskView both carry them. */
export interface TimedTask {
	status: Status;
	attempt: number;
	/**
	 * Set for an awaited task (TaskRun.await present): never queued and never
	 * reported on by an Orker, so wait, the wait for an Orker and phases do not apply and
	 * RUN is the time it spent awaiting — the derivation's await row.
	 */
	awaited?: boolean;
	scheduledAt?: Timestamp;
	startedAt?: Timestamp;
	finishedAt?: Timestamp;
}

export interface TaskTiming {
	/** Reached schedule(); false means every quantity is 'na'. */
	scheduled: boolean;
	/** scheduled_at unset with started_at set: a snapshot written before scheduled_at existed. */
	preContract: boolean;
	wait: Quantity;
	run: Quantity;
	total: Quantity;
	/** RUNNING only: now − scheduled_at. */
	sinceScheduled: Quantity;
	/** attempts[1].started_at − scheduled_at. */
	queueWait: Quantity;
	/** Every attempt row is unavailable because the records were reclaimed. */
	reclaimed: boolean;
	/** An awaited task: `run` is the time it awaited, everything else 'na'. */
	awaited: boolean;
}

/**
 * recordsReclaimed: a timeline with no record the Orker filed, for a task the
 * snapshot proves an Orker reported on. An Orker that reports a start has
 * always written a record for that attempt, so rows that are ALL missing mean
 * the cleanup worker reclaimed them, not that every Orker crashed.
 */
function recordsReclaimed(task: TimedTask, rows: TaskAttempt[]): boolean {
	if (rows.length === 0) return isTerminal(task.status);
	if (!task.startedAt) return false;
	return rows.every((row) => row.outcome === AttemptOutcome.UNREPORTED);
}

/**
 * taskTiming implements the derivation for one task. `attempts` is the task's
 * ListTaskAttempts rows, or `undefined` when the engine has no attempt store
 * (FailedPrecondition). `now` is the clock "since scheduled" reads.
 */
export function taskTiming(
	task: TimedTask,
	attempts: TaskAttempt[] | undefined,
	now?: Timestamp
): TaskTiming {
	const S = task.scheduledAt;
	const B = task.startedAt;
	const F = task.finishedAt;
	const terminal = isTerminal(task.status);
	const timing: TaskTiming = {
		scheduled: S !== undefined || B !== undefined,
		preContract: S === undefined && B !== undefined,
		wait: NA,
		run: NA,
		total: NA,
		sinceScheduled: NA,
		queueWait: NA,
		reclaimed: false,
		awaited: task.awaited === true
	};
	if (timing.awaited) {
		// scheduled_at and started_at are both the instant it started
		// awaiting; an await that never opened has neither.
		timing.scheduled = B !== undefined;
		timing.preContract = false;
		if (timing.scheduled && terminal && F) {
			timing.run = span(B!, F);
			timing.total = timing.run;
		}
		return timing;
	}
	if (!timing.scheduled) return timing;

	if (timing.preContract) {
		timing.wait = unavailable(TIMING_REASON.preContract);
		timing.run = unavailable(TIMING_REASON.preContract);
		if (terminal && F && B) timing.total = span(B, F);
	} else if (!terminal) {
		if (task.status === Status.RUNNING && S && now) timing.sinceScheduled = span(S, now);
	} else {
		if (!B) {
			timing.wait = unavailable(TIMING_REASON.noOrkerStart);
			timing.run = unavailable(TIMING_REASON.noOrkerStart);
		} else {
			timing.wait = span(S!, B);
			if (F) timing.run = span(B, F);
		}
		if (F) timing.total = span(S!, F);
	}

	if (attempts === undefined) {
		timing.queueWait = unavailable(TIMING_REASON.noStore);
		return timing;
	}
	if (timing.preContract) {
		timing.queueWait = unavailable(TIMING_REASON.preContract);
		return timing;
	}
	if (recordsReclaimed(task, attempts)) {
		timing.reclaimed = true;
		timing.queueWait = unavailable(TIMING_REASON.reclaimed);
		return timing;
	}
	const first = attempts.find((row) => row.attempt === 1);
	if (!first || first.outcome === AttemptOutcome.UNREPORTED || !first.startedAt) {
		timing.queueWait = unavailable(TIMING_REASON.noRecord);
	} else {
		timing.queueWait = span(S!, first.startedAt);
	}
	return timing;
}

export interface PhaseTiming {
	phase: string;
	duration: Quantity;
}

export type AttemptPhases =
	| { kind: 'phases'; phases: PhaseTiming[] }
	| { kind: 'unavailable'; reason: TimingReason };

/**
 * attemptPhases lists one attempt's §7.1 phases with their durations, or says
 * why they are not known: an UNREPORTED row has no record (or its records were
 * reclaimed), and a record written before phase spans existed has none.
 */
export function attemptPhases(attempt: TaskAttempt, reclaimed = false): AttemptPhases {
	if (attempt.outcome === AttemptOutcome.UNREPORTED) {
		return {
			kind: 'unavailable',
			reason: reclaimed ? TIMING_REASON.reclaimed : TIMING_REASON.noRecord
		};
	}
	if (attempt.phases.length === 0) return { kind: 'unavailable', reason: TIMING_REASON.noPhases };
	return {
		kind: 'phases',
		phases: attempt.phases.map((span_) => ({
			phase: span_.phase,
			duration:
				span_.startedAt && span_.finishedAt
					? span(span_.startedAt, span_.finishedAt)
					: unavailable(TIMING_REASON.noRecord)
		}))
	};
}

/** formatQuantity renders a quantity the way the page prints it: a duration, 'unavailable', or '—'. */
export function formatQuantity(q: Quantity): string {
	switch (q.kind) {
		case 'na':
			return '—';
		case 'unavailable':
			return 'unavailable';
		case 'value':
			return formatDuration(q.from, q.to);
	}
}

/** The phases line: `image 1.2s · run 50s · commit 700ms`. */
export function phasesLine(phases: PhaseTiming[]): string {
	return phases.map((p) => `${p.phase} ${formatQuantity(p.duration)}`).join(' · ');
}

/** One segment of a task on its run's timeline, as fractions of the run's span. */
export interface TimelineSegment {
	key: 'wait' | 'run';
	/** Where it starts: (its start − the run's start) ÷ the run's span. */
	left: number;
	/** How long it is: its duration ÷ the run's span. */
	width: number;
	/** False when the split between waiting and running is not known. */
	known: boolean;
}

/**
 * timelinePlacement puts one task on its RUN's timeline, the one every row
 * of the run shares, so a reader sees when each task ran relative to the
 * others, not only how long: a segment starts at (its start − the run's
 * start) ÷ the run's span and is as wide as its duration ÷ that span.
 * Times are epoch milliseconds. WAIT runs from scheduled to the Orker's
 * start, RUN from that start to finished; with no start known, the span
 * from scheduled to finished is split in two segments marked unknown.
 * Null when the run's span or the task's own ends are not known.
 */
export function timelinePlacement(
	run: { start: number | null; end: number | null },
	task: { scheduled: number | null; started: number | null; finished: number | null }
): TimelineSegment[] | null {
	if (run.start === null || run.end === null || run.end <= run.start) return null;
	const from = task.scheduled ?? task.started;
	if (from === null || task.finished === null) return null;
	const span_ = run.end - run.start;
	const at = (t: number) => Math.min(Math.max((t - run.start!) / span_, 0), 1);
	const segment = (key: 'wait' | 'run', a: number, b: number, known: boolean): TimelineSegment => ({
		key,
		left: at(a),
		width: Math.max(at(b) - at(a), 0),
		known
	});
	if (task.started !== null) {
		const begun = Math.max(task.started, from);
		return [segment('wait', from, begun, true), segment('run', begun, Math.max(task.finished, begun), true)];
	}
	const mid = from + (task.finished - from) / 2;
	return [segment('wait', from, mid, false), segment('run', mid, task.finished, false)];
}
