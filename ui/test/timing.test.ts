import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { DurationSchema } from '@bufbuild/protobuf/wkt';
import {
	AttemptOutcome,
	AttemptTimingSchema,
	PhaseSpanSchema,
	TaskAttemptSchema,
	TaskTimingSchema,
	TimingQuantitySchema,
	TimingReason
} from '../src/lib/gen/orkstack/v1/run_pb';
import { TaskAttemptsTimingSchema } from '../src/lib/gen/orkstack/v1/run_service_pb';
import {
	TIMING_REASON,
	attemptPhases,
	formatQuantity,
	phasesLine,
	quantityOf,
	reasonWords,
	taskTiming
} from '../src/lib/timing';

// The engine derives task timing (internal/api/timing.go, where its scenarios
// are tested); the page reads what it answered. These pin the reading: three
// states never collapsed, the CLI's words for each reason, and the one rule
// the page adds — an engine without an attempt store says so.

function value(ms: number) {
	return create(TimingQuantitySchema, {
		duration: create(DurationSchema, { seconds: BigInt(Math.floor(ms / 1000)), nanos: (ms % 1000) * 1e6 })
	});
}

function because(reason: TimingReason) {
	return create(TimingQuantitySchema, { reason });
}

test('a quantity is not applicable, unavailable with its reason, or a value — never a zero for not known', () => {
	expect(quantityOf(undefined)).toEqual({ kind: 'na' });
	expect(quantityOf(because(TimingReason.CLOCK_SKEW))).toEqual({ kind: 'unavailable', reason: 'clock skew' });
	expect(quantityOf(value(2100))).toEqual({ kind: 'value', ms: 2100 });
	expect(quantityOf(value(0))).toEqual({ kind: 'value', ms: 0 });
	expect(formatQuantity(quantityOf(undefined))).toBe('—');
	expect(formatQuantity(quantityOf(because(TimingReason.NO_RECORD)))).toBe('unavailable');
	expect(formatQuantity(quantityOf(value(63_000)))).toBe('1m 3s');
	expect(formatQuantity(quantityOf(value(999)))).toBe('999ms');
});

test("the seven reasons are the CLI's seven, one for each of the engine's", () => {
	expect(Object.values(TIMING_REASON).sort()).toEqual([
		'attempt records reclaimed',
		'clock skew',
		'no attempt record',
		'no orker reported a start',
		'no task attempt store',
		'record predates phase spans',
		'recorded before scheduled time existed'
	]);
	const spelled = Object.values(TimingReason)
		.filter((r): r is TimingReason => typeof r === 'number' && r !== TimingReason.UNSPECIFIED)
		.map(reasonWords);
	expect(spelled.sort()).toEqual(Object.values(TIMING_REASON).sort());
});

test("a task's timing is its snapshot's, and its wait for an Orker its timeline's", () => {
	const snapshot = create(TaskTimingSchema, { scheduled: true, wait: value(2100), run: value(63_000), total: value(65_100) });
	const timeline = create(TaskAttemptsTimingSchema, { taskId: 't', queueWait: because(TimingReason.NO_RECORD) });
	const timing = taskTiming(snapshot, timeline, true);
	expect(timing.wait).toEqual({ kind: 'value', ms: 2100 });
	expect(timing.run).toEqual({ kind: 'value', ms: 63_000 });
	expect(timing.total).toEqual({ kind: 'value', ms: 65_100 });
	expect(timing.queueWait).toEqual({ kind: 'unavailable', reason: 'no attempt record' });
});

test('an engine with no attempt store names it, for a scheduled container task only', () => {
	const snapshot = create(TaskTimingSchema, { scheduled: true, wait: value(1000) });
	expect(taskTiming(snapshot, undefined, true).queueWait).toEqual({ kind: 'unavailable', reason: 'no task attempt store' });
	// An awaited, fan-out or composed task is never queued, and a task never
	// scheduled has nothing to say.
	expect(taskTiming(snapshot, undefined, false).queueWait.kind).toBe('na');
	expect(taskTiming(create(TaskTimingSchema, {}), undefined, true).queueWait.kind).toBe('na');
	expect(taskTiming(undefined, undefined, true)).toEqual({
		scheduled: false,
		preContract: false,
		wait: { kind: 'na' },
		run: { kind: 'na' },
		total: { kind: 'na' },
		queueWait: { kind: 'na' }
	});
});

test("an attempt's phases carry the engine's durations, or the reason they cannot be listed", () => {
	const listed = create(TaskAttemptSchema, {
		attempt: 1,
		outcome: AttemptOutcome.SUCCEEDED,
		timing: create(AttemptTimingSchema, {}),
		phases: ['image', 'run', 'commit'].map((phase) => create(PhaseSpanSchema, { phase, duration: value(100) }))
	});
	const phases = attemptPhases(listed);
	expect(phases.kind === 'phases' && phasesLine(phases.phases)).toBe('image 100ms · run 100ms · commit 100ms');
	const reclaimed = create(TaskAttemptSchema, {
		attempt: 1,
		outcome: AttemptOutcome.UNREPORTED,
		timing: create(AttemptTimingSchema, { phasesReason: TimingReason.RECLAIMED })
	});
	expect(attemptPhases(reclaimed)).toEqual({ kind: 'unavailable', reason: 'attempt records reclaimed' });
	const missingEnd = create(TaskAttemptSchema, {
		attempt: 1,
		outcome: AttemptOutcome.SUCCEEDED,
		timing: create(AttemptTimingSchema, {}),
		phases: [create(PhaseSpanSchema, { phase: 'commit', duration: because(TimingReason.NO_RECORD) })]
	});
	expect(attemptPhases(missingEnd)).toEqual({
		kind: 'phases',
		phases: [{ phase: 'commit', duration: { kind: 'unavailable', reason: 'no attempt record' } }]
	});
});

test("a task is placed on its run's timeline: offset and width are shares of the run's span", async () => {
	const { timelinePlacement } = await import('../src/lib/timing');
	const run = { start: 1_000, end: 11_000 };
	// Scheduled at 2 s, started at 3 s, finished at 6 s of a 10 s run.
	const [wait, work] = timelinePlacement(run, { scheduled: 3_000, started: 4_000, finished: 7_000 })!;
	const close = (s: { key: string; left: number; width: number; known: boolean }, key: string, left: number, width: number, known: boolean) => {
		expect(s.key).toBe(key);
		expect(Math.round(s.left * 1e6) / 1e6).toBe(left);
		expect(Math.round(s.width * 1e6) / 1e6).toBe(width);
		expect(s.known).toBe(known);
	};
	close(wait, 'wait', 0.2, 0.1, true);
	close(work, 'run', 0.3, 0.3, true);
	// No Orker start known: the span is split in two, marked unknown.
	const [first, second] = timelinePlacement(run, { scheduled: 1_000, started: null, finished: 5_000 })!;
	close(first, 'wait', 0, 0.2, false);
	close(second, 'run', 0.2, 0.2, false);
	// A time outside the run is clamped to its ends; an unknown end places nothing.
	expect(timelinePlacement(run, { scheduled: 0, started: 500, finished: 20_000 })!.map((s) => [s.left, s.width])).toEqual([
		[0, 0],
		[0, 1]
	]);
	expect(timelinePlacement({ start: 1_000, end: null }, { scheduled: 1, started: 2, finished: 3 })).toBeNull();
	expect(timelinePlacement(run, { scheduled: 2_000, started: 3_000, finished: null })).toBeNull();
});
