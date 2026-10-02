import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { TimestampSchema } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import {
	AttemptOutcome,
	PhaseSpanSchema,
	TaskAttemptSchema,
	type TaskAttempt
} from '../src/lib/gen/orkstack/v1/run_pb';
import {
	TIMING_REASON,
	attemptPhases,
	formatQuantity,
	phasesLine,
	taskTiming,
	type Quantity
} from '../src/lib/timing';

// The task-timing derivation's scenario table, T1-T8, through the browser's
// copy of it. The reason strings asserted here are the ones `orkctl run
// timing` prints (cmd/orkctl/timing.go), so the two clients cannot drift.

const BASE = 1_790_000_000n;
function at(secondsAfter: number): ReturnType<typeof create<typeof TimestampSchema>> {
	const whole = Math.floor(secondsAfter);
	return create(TimestampSchema, {
		seconds: BASE + BigInt(whole),
		nanos: Math.round((secondsAfter - whole) * 1e9)
	});
}

function ms(q: Quantity): number | undefined {
	return q.kind === 'value' ? Math.round(q.ms) : undefined;
}

const SUCCESS = [
	'start', 'commit-check', 'workspace', 'stage-inputs', 'image', 'render',
	'create', 'run', 'exit', 'validate', 'commit'
];

function record(attempt: number, outcome: AttemptOutcome, start: number, names: string[]): TaskAttempt {
	let t = start;
	const phases = names.map((phase) => {
		const span = create(PhaseSpanSchema, { phase, startedAt: at(t), finishedAt: at(t + 0.1) });
		t += 0.1;
		return span;
	});
	return create(TaskAttemptSchema, {
		taskId: 't',
		attempt,
		outcome,
		startedAt: at(start),
		finishedAt: at(t),
		phase: names[names.length - 1] ?? '',
		phases
	});
}

function unreported(attempt: number): TaskAttempt {
	return create(TaskAttemptSchema, { taskId: 't', attempt, outcome: AttemptOutcome.UNREPORTED });
}

test('U1 (T1): a first-try success splits into wait, run and total, and its queue wait is its wait', () => {
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 1, scheduledAt: at(0), startedAt: at(2.1), finishedAt: at(65.1) },
		[record(1, AttemptOutcome.SUCCEEDED, 2.1, SUCCESS)]
	);
	expect(ms(timing.wait)).toBe(2100);
	expect(ms(timing.run)).toBe(63000);
	expect(ms(timing.total)).toBe(65100);
	expect(ms(timing.queueWait)).toBe(2100);
	const phases = attemptPhases(record(1, AttemptOutcome.SUCCEEDED, 2.1, SUCCESS));
	expect(phases.kind).toBe('phases');
	if (phases.kind === 'phases') {
		expect(phases.phases.map((p) => p.phase)).toEqual(SUCCESS);
		expect(phases.phases.every((p) => ms(p.duration) === 100)).toBe(true);
	}
});

test('U2 (T2): a task decided on attempt 3 whose attempt 1 left no record', () => {
	const rows = [
		unreported(1),
		record(2, AttemptOutcome.FAILED, 60, SUCCESS.slice(0, 9)),
		record(3, AttemptOutcome.SUCCEEDED, 251, SUCCESS)
	];
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 3, scheduledAt: at(0), startedAt: at(251), finishedAt: at(303) },
		rows
	);
	expect(ms(timing.wait)).toBe(251000);
	expect(ms(timing.run)).toBe(52000);
	expect(ms(timing.total)).toBe(303000);
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: TIMING_REASON.noRecord });
	const [a1, a2, a3] = rows.map((row) => attemptPhases(row));
	expect(a1).toEqual({ kind: 'unavailable', reason: 'no attempt record' });
	expect(a2.kind === 'phases' && a2.phases.length).toBe(9);
	expect(a3.kind === 'phases' && a3.phases.length).toBe(11);
});

test('U3 (T3): no Orker reported a start — wait and run are unavailable, total is a number', () => {
	const timing = taskTiming(
		{ status: Status.FAILED, attempt: 1, scheduledAt: at(0), finishedAt: at(2) },
		[]
	);
	expect(timing.wait).toEqual({ kind: 'unavailable', reason: 'no orker reported a start' });
	expect(timing.run).toEqual({ kind: 'unavailable', reason: 'no orker reported a start' });
	expect(ms(timing.total)).toBe(2000);
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: 'attempt records reclaimed' });
	expect(formatQuantity(timing.wait)).toBe('unavailable');
});

test('U4 (T4): a task that was never scheduled has nothing to say, not zeros', () => {
	const timing = taskTiming({ status: Status.SKIPPED, attempt: 0 }, []);
	expect(timing.scheduled).toBe(false);
	for (const q of [timing.wait, timing.run, timing.total, timing.queueWait]) {
		expect(q).toEqual({ kind: 'na' });
		expect(formatQuantity(q)).toBe('—');
	}
});

test('U5 (T5): a snapshot written before scheduled_at existed', () => {
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 1, startedAt: at(0), finishedAt: at(12) },
		[create(TaskAttemptSchema, { taskId: 't', attempt: 1, outcome: AttemptOutcome.SUCCEEDED, startedAt: at(1), finishedAt: at(11), phase: 'commit' })]
	);
	expect(timing.preContract).toBe(true);
	expect(timing.wait).toEqual({ kind: 'unavailable', reason: 'recorded before scheduled time existed' });
	expect(timing.run).toEqual({ kind: 'unavailable', reason: 'recorded before scheduled time existed' });
	expect(ms(timing.total)).toBe(12000);
	expect(
		attemptPhases(create(TaskAttemptSchema, { attempt: 1, outcome: AttemptOutcome.SUCCEEDED, phase: 'commit' }))
	).toEqual({ kind: 'unavailable', reason: 'record predates phase spans' });
});

test('U6 (T6): two skewed clocks are unavailable, never negative', () => {
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 1, scheduledAt: at(3), startedAt: at(0), finishedAt: at(13) },
		[record(1, AttemptOutcome.SUCCEEDED, 0, SUCCESS)]
	);
	expect(timing.wait).toEqual({ kind: 'unavailable', reason: 'clock skew' });
	expect(ms(timing.run)).toBe(13000);
	expect(ms(timing.total)).toBe(10000);
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: 'clock skew' });
});

test('U7 (T7): while a task runs, only "since scheduled" is known', () => {
	const timing = taskTiming(
		{ status: Status.RUNNING, attempt: 1, scheduledAt: at(0) },
		[unreported(1)],
		at(75)
	);
	expect(ms(timing.sinceScheduled)).toBe(75000);
	expect(timing.wait).toEqual({ kind: 'na' });
	expect(timing.run).toEqual({ kind: 'na' });
	expect(timing.total).toEqual({ kind: 'na' });
});

test('U8 (T8): an engine with no attempt store still gives the snapshot half', () => {
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 1, scheduledAt: at(0), startedAt: at(2.1), finishedAt: at(65.1) },
		undefined
	);
	expect(ms(timing.wait)).toBe(2100);
	expect(ms(timing.run)).toBe(63000);
	expect(ms(timing.total)).toBe(65100);
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: 'no task attempt store' });
});

test('reclaimed records of a task an Orker reported on read as reclaimed, not crashed', () => {
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 1, scheduledAt: at(0), startedAt: at(2), finishedAt: at(5) },
		[unreported(1)]
	);
	expect(timing.reclaimed).toBe(true);
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: 'attempt records reclaimed' });
	expect(attemptPhases(unreported(1), true)).toEqual({ kind: 'unavailable', reason: 'attempt records reclaimed' });
});

test('the phases line names every span in order', () => {
	const phases = attemptPhases(record(1, AttemptOutcome.SUCCEEDED, 0, ['image', 'run', 'commit']));
	expect(phases.kind === 'phases' && phasesLine(phases.phases)).toBe('image 100ms · run 100ms · commit 100ms');
});

test('the seven reasons are the CLI\'s seven', () => {
	expect(Object.values(TIMING_REASON).sort()).toEqual([
		'attempt records reclaimed',
		'clock skew',
		'no attempt record',
		'no orker reported a start',
		'no task attempt store',
		'record predates phase spans',
		'recorded before scheduled time existed'
	]);
});

test('an awaited task is awaited, never queued: run is F − B and nothing else applies', async () => {
	const { taskTiming } = await import('../src/lib/timing');
	const { Status } = await import('../src/lib/gen/orkstack/v1/common_pb');
	const opened = { seconds: 1_000n, nanos: 0 };
	const resolved = { seconds: 1_000n + 5400n, nanos: 0 };
	const timing = taskTiming(
		{ status: Status.SUCCEEDED, attempt: 0, awaited: true, scheduledAt: opened, startedAt: opened, finishedAt: resolved },
		[]
	);
	expect(timing.awaited).toBe(true);
	expect(timing.scheduled).toBe(true);
	expect(timing.wait.kind).toBe('na');
	expect(timing.queueWait.kind).toBe('na');
	expect(timing.run.kind === 'value' && timing.run.ms).toBe(5_400_000);
	const pending = taskTiming({ status: Status.PENDING, attempt: 0, awaited: true }, undefined);
	expect(pending.scheduled).toBe(false);
	expect(pending.queueWait.kind).toBe('na');
});
