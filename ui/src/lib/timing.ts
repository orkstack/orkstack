import type { Duration } from '@bufbuild/protobuf/wkt';
import {
	TimingReason,
	type TaskAttempt,
	type TaskTiming as TaskRunTiming,
	type TimingQuantity
} from './gen/orkstack/v1/run_pb';
import type { TaskAttemptsTiming } from './gen/orkstack/v1/run_service_pb';
import { formatMilliseconds } from './runDetail';

// WHERE A TASK'S TIME WENT, AS THE ENGINE DERIVED IT.
//
// The engine derives task timing once (internal/api/timing.go): GetRun sets
// TaskRun.timing, ListTaskAttempts sets each attempt's timing, each phase's
// duration and each timeline's wait for an Orker. `orkctl run timing` prints
// the same fields, so the two clients say the same thing about the same run.
// This module only reads them:
//
//   - 'na'           it does not apply (never scheduled, or not finished yet);
//   - 'unavailable'  it happened and the evidence is gone or inconsistent,
//                    with the reason — never drawn as 0;
//   - 'value'        two instants some process actually recorded.

export const TIMING_REASON = {
	noOrkerStart: 'no orker reported a start',
	clockSkew: 'clock skew',
	preContract: 'recorded before scheduled time existed',
	reclaimed: 'attempt records reclaimed',
	noRecord: 'no attempt record',
	noPhases: 'record predates phase spans',
	noStore: 'no task attempt store'
} as const;

export type TimingReasonWords = (typeof TIMING_REASON)[keyof typeof TIMING_REASON];

/** The words the CLI prints for each of the engine's reasons. */
const REASON_WORDS: Partial<Record<TimingReason, TimingReasonWords>> = {
	[TimingReason.NO_ORKER_START]: TIMING_REASON.noOrkerStart,
	[TimingReason.CLOCK_SKEW]: TIMING_REASON.clockSkew,
	[TimingReason.PRE_CONTRACT]: TIMING_REASON.preContract,
	[TimingReason.RECLAIMED]: TIMING_REASON.reclaimed,
	[TimingReason.NO_RECORD]: TIMING_REASON.noRecord,
	[TimingReason.NO_PHASES]: TIMING_REASON.noPhases,
	[TimingReason.NO_ATTEMPT_STORE]: TIMING_REASON.noStore
};

/** reasonWords spells one of the engine's reasons as both clients print it. */
export function reasonWords(reason: TimingReason): TimingReasonWords | undefined {
	return REASON_WORDS[reason];
}

export type Quantity =
	| { kind: 'na' }
	| { kind: 'unavailable'; reason: TimingReasonWords }
	| { kind: 'value'; ms: number };

const NA: Quantity = { kind: 'na' };

function unavailable(reason: TimingReasonWords): Quantity {
	return { kind: 'unavailable', reason };
}

/** Whole milliseconds of a protobuf Duration, as formatDuration counts them. */
function durationMs(d: Duration): number {
	return Number(d.seconds) * 1000 + Math.floor(d.nanos / 1e6);
}

/** quantityOf reads one of the engine's quantities: unset is 'na'. */
export function quantityOf(q: TimingQuantity | undefined): Quantity {
	if (!q) return NA;
	if (q.duration) return { kind: 'value', ms: durationMs(q.duration) };
	const reason = reasonWords(q.reason);
	return reason ? unavailable(reason) : NA;
}

export interface TaskTiming {
	/** Reached schedule(); false means every quantity is 'na'. */
	scheduled: boolean;
	/** scheduled_at unset with started_at set: a snapshot written before scheduled_at existed. */
	preContract: boolean;
	wait: Quantity;
	run: Quantity;
	total: Quantity;
	/** The wait for an Orker: attempts[1].started_at − scheduled_at. */
	queueWait: Quantity;
}

/**
 * taskTiming reads one task's timing: its snapshot's (TaskRun.timing) and, for
 * a container task, its timeline's wait for an Orker. `attempts` is that
 * timeline's TaskAttemptsTiming, or `undefined` when the engine has no
 * attempt store (FailedPrecondition).
 */
export function taskTiming(
	timing: TaskRunTiming | undefined,
	attempts: TaskAttemptsTiming | undefined,
	container: boolean
): TaskTiming {
	const scheduled = timing?.scheduled ?? false;
	return {
		scheduled,
		preContract: timing?.preContract ?? false,
		wait: quantityOf(timing?.wait),
		run: quantityOf(timing?.run),
		total: quantityOf(timing?.total),
		queueWait:
			!container || !scheduled
				? NA
				: attempts === undefined
					? unavailable(TIMING_REASON.noStore)
					: quantityOf(attempts.queueWait)
	};
}

export interface PhaseTiming {
	phase: string;
	duration: Quantity;
}

export type AttemptPhases =
	| { kind: 'phases'; phases: PhaseTiming[] }
	| { kind: 'unavailable'; reason: TimingReasonWords };

/**
 * attemptPhases lists one attempt's §7.1 phases with their durations, or says
 * why they are not known: an UNREPORTED row has no record (or its records were
 * reclaimed), and a record written before phase spans existed has none.
 */
export function attemptPhases(attempt: TaskAttempt): AttemptPhases {
	const reason = attempt.timing ? reasonWords(attempt.timing.phasesReason) : undefined;
	if (reason) return { kind: 'unavailable', reason };
	return {
		kind: 'phases',
		phases: attempt.phases.map((span) => ({ phase: span.phase, duration: quantityOf(span.duration) }))
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
			return formatMilliseconds(q.ms);
	}
}

/** One attempt's phases gathered for its bar: what came before the container ran, the run, what came after. */
export interface PhaseGroup {
	key: 'setup' | 'run' | 'finish';
	label: string;
	ms: number;
	phases: string[];
}

export function phaseGroups(phases: readonly PhaseTiming[]): PhaseGroup[] {
	const groups: PhaseGroup[] = [];
	let seenRun = false;
	const add = (key: PhaseGroup['key'], label: string, phase: string, ms: number) => {
		const group = groups.find((g) => g.key === key);
		if (group) {
			group.ms += ms;
			group.phases.push(phase);
		} else groups.push({ key, label, ms, phases: [phase] });
	};
	for (const p of phases) {
		const ms = p.duration.kind === 'value' ? p.duration.ms : 0;
		if (p.phase === 'run') {
			seenRun = true;
			add('run', 'Run', p.phase, ms);
		} else if (seenRun) add('finish', 'Finish', p.phase, ms);
		else add('setup', 'Setup', p.phase, ms);
	}
	return groups;
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
