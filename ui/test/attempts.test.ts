import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { AttemptOutcome, RetryState, TaskAttemptSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import type { TaskAttempt } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	attemptOutcomeClasses,
	attemptOutcomeLabel,
	attemptsSeen,
	groupAttemptsByTask,
	isUnreported,
	retryBadgeLabel,
	retryCount,
	retryStateLabel,
	runRetryCount
} from '../src/lib/attempts';
import { STATUS_CLASSES } from '../src/lib/status';

// Real TaskAttempt messages, not shaped literals: these helpers exist to read
// what the wire actually carries, and a synthesized UNREPORTED row is defined
// by the fields it does NOT have.
function attempt(taskId: string, n: number, outcome: AttemptOutcome): TaskAttempt {
	return create(TaskAttemptSchema, {
		schemaVersion: 1,
		runId: 'ork-1',
		taskId,
		attempt: n,
		outcome
	});
}

/** A gap-filler: the reader synthesized it, so it carries nothing but its number. */
function unreported(taskId: string, n: number): TaskAttempt {
	return attempt(taskId, n, AttemptOutcome.UNREPORTED);
}

test('a task nobody has tried yet has no retries and no badge', () => {
	expect(attemptsSeen([])).toBe(0);
	expect(retryCount([])).toBe(0);
	expect(retryBadgeLabel([], true)).toBeNull();
	expect(retryBadgeLabel([], false)).toBeNull();
	// An empty list is a valid answer, not an error: the run may be young, or
	// its records may have been reclaimed.
	expect(runRetryCount([])).toBe(0);
});

test('one attempt is not a retry', () => {
	const rows = [attempt('build', 1, AttemptOutcome.SUCCEEDED)];
	expect(attemptsSeen(rows)).toBe(1);
	expect(retryCount(rows)).toBe(0);
	// Nothing to say: a task tried once is the ordinary case and gets no pill.
	expect(retryBadgeLabel(rows, true)).toBeNull();
	expect(retryBadgeLabel(rows, false)).toBeNull();
});

test('a failed try followed by a good one counts as one retry', () => {
	const rows = [
		attempt('flaky', 1, AttemptOutcome.FAILED),
		attempt('flaky', 2, AttemptOutcome.SUCCEEDED)
	];
	expect(retryCount(rows)).toBe(1);
	expect(retryBadgeLabel(rows, true)).toBe('retried ×1');
	// The same task while it is still going describes the try IN FLIGHT
	// instead: "retried ×1" would be a past tense about something happening.
	expect(retryBadgeLabel(rows, false)).toBe('attempt 2');
});

test('several retries are counted from the highest recorded try, not the row count', () => {
	const rows = [
		attempt('flaky', 1, AttemptOutcome.FAILED),
		attempt('flaky', 2, AttemptOutcome.FAILED),
		attempt('flaky', 3, AttemptOutcome.FAILED),
		attempt('flaky', 4, AttemptOutcome.SUCCEEDED)
	];
	expect(attemptsSeen(rows)).toBe(4);
	expect(retryCount(rows)).toBe(3);
	expect(retryBadgeLabel(rows, true)).toBe('retried ×3');
});

test('an UNREPORTED gap in the middle does not inflate the retry count', () => {
	// Attempt 2's Orker died before it could file a record. The timeline stays
	// gapless — renumbering 3 as 2 would lie about which try wrote which log —
	// but the count is evidence-based: three tries are named, two are recorded,
	// and the highest RECORDED one is what the count is read from.
	const rows = [
		attempt('flaky', 1, AttemptOutcome.FAILED),
		unreported('flaky', 2),
		attempt('flaky', 3, AttemptOutcome.SUCCEEDED)
	];
	expect(attemptsSeen(rows)).toBe(3);
	expect(retryCount(rows)).toBe(2);
	expect(retryBadgeLabel(rows, true)).toBe('retried ×2');
	// A synthesized row carries a number and an outcome and nothing else.
	const gap = rows[1];
	expect(isUnreported(gap)).toBe(true);
	expect(gap.error).toBeUndefined();
	expect(gap.startedAt).toBeUndefined();
	expect(gap.finishedAt).toBeUndefined();
	expect(gap.phase).toBe('');
	expect(gap.orkerId).toBe('');
});

test('a timeline whose only evidence is a gap asserts tries, never retries', () => {
	// Every record is missing — the Orkers that ran this task all died. Nothing
	// here is evidence that a retry HAPPENED for a reason anybody recorded, so
	// the badge states what is actually known: two tries were named.
	const rows = [unreported('lost', 1), unreported('lost', 2)];
	expect(attemptsSeen(rows)).toBe(2);
	expect(retryCount(rows)).toBe(0);
	expect(retryBadgeLabel(rows, true)).toBe('2 attempts');
	expect(retryBadgeLabel(rows, false)).toBe('attempt 2');
	// And a lone unreported first try is still just one try.
	expect(retryBadgeLabel([unreported('lost', 1)], true)).toBeNull();
});

test('a canceled try is a recorded try', () => {
	const rows = [
		attempt('stopped', 1, AttemptOutcome.FAILED),
		attempt('stopped', 2, AttemptOutcome.CANCELED)
	];
	expect(retryCount(rows)).toBe(1);
	expect(retryBadgeLabel(rows, true)).toBe('retried ×1');
});

test('grouping keeps the response order, which is the page order', () => {
	// The server orders tasks as Run.task_runs does and attempts ascending
	// within them. Neither is re-derived here: a client sort would put "build"
	// after "apply" for a reason no reader could see.
	const rows = [
		attempt('apply', 1, AttemptOutcome.SUCCEEDED),
		attempt('build', 1, AttemptOutcome.FAILED),
		attempt('build', 2, AttemptOutcome.SUCCEEDED),
		attempt('ship', 1, AttemptOutcome.SUCCEEDED)
	];
	const grouped = groupAttemptsByTask(rows);
	expect([...grouped.keys()]).toEqual(['apply', 'build', 'ship']);
	expect(grouped.get('build')?.map((a) => a.attempt)).toEqual([1, 2]);
	expect(grouped.get('apply')).toHaveLength(1);
	expect(grouped.get('nobody')).toBeUndefined();
});

test('the run chip sums what each task was retried, not what it was tried', () => {
	const rows = [
		attempt('apply', 1, AttemptOutcome.SUCCEEDED),
		attempt('build', 1, AttemptOutcome.FAILED),
		attempt('build', 2, AttemptOutcome.SUCCEEDED),
		attempt('ship', 1, AttemptOutcome.FAILED),
		unreported('ship', 2),
		attempt('ship', 3, AttemptOutcome.SUCCEEDED)
	];
	// build retried once, ship twice, apply not at all.
	expect(runRetryCount(rows)).toBe(3);
	// A run where nothing retried contributes nothing, so the header can hide
	// the chip entirely rather than draw "Retries: 0".
	expect(runRetryCount([attempt('apply', 1, AttemptOutcome.SUCCEEDED)])).toBe(0);
});

test('why retrying stopped is said only when the snapshot knows', () => {
	// A snapshot written before TaskRun.retry_state existed carries the zero
	// value. Saying nothing is the honest rendering; "not retried" would be an
	// assertion nothing made.
	expect(retryStateLabel(RetryState.UNSPECIFIED)).toBeNull();
	expect(retryStateLabel(undefined)).toBeNull();

	// The distinction the field exists for: a budget that ran out versus a
	// failure nothing would ever have tried again.
	expect(retryStateLabel(RetryState.MAXIMUM_ATTEMPTS_REACHED, 3)).toBe(
		'retries exhausted after 3 attempts'
	);
	// The count is only appended when the timeline actually knows one.
	expect(retryStateLabel(RetryState.MAXIMUM_ATTEMPTS_REACHED)).toBe('retries exhausted');
	expect(retryStateLabel(RetryState.MAXIMUM_ATTEMPTS_REACHED, 1)).toBe('retries exhausted');
	expect(retryStateLabel(RetryState.NON_RETRYABLE_FAILURE)).toBe(
		'not retried: the failure is not retryable'
	);
	expect(retryStateLabel(RetryState.RETRY_POLICY_NOT_SET)).toBe(
		'not retried: this task declares no retry policy'
	);
	expect(retryStateLabel(RetryState.CANCEL_REQUESTED)).toBe(
		'not retried: cancellation was requested'
	);
	expect(retryStateLabel(RetryState.TIMEOUT)).toBe('retrying stopped: the task ran out of time');
	expect(retryStateLabel(RetryState.INTERNAL_SERVER_ERROR)).toContain('internal error');
});

test('outcomes are labelled by their enum name and painted from the status palette', () => {
	expect(attemptOutcomeLabel(AttemptOutcome.SUCCEEDED)).toBe('SUCCEEDED');
	expect(attemptOutcomeLabel(AttemptOutcome.FAILED)).toBe('FAILED');
	expect(attemptOutcomeLabel(AttemptOutcome.CANCELED)).toBe('CANCELED');
	expect(attemptOutcomeLabel(AttemptOutcome.UNREPORTED)).toBe('UNREPORTED');
	expect(attemptOutcomeLabel(undefined)).toBe('UNSPECIFIED');

	// The palette is the one the rest of the page already uses — a succeeded
	// try is the green a succeeded task is.
	expect(attemptOutcomeClasses(AttemptOutcome.SUCCEEDED)).toBe(STATUS_CLASSES[Status.SUCCEEDED]);
	expect(attemptOutcomeClasses(AttemptOutcome.FAILED)).toBe(STATUS_CLASSES[Status.FAILED]);
	expect(attemptOutcomeClasses(AttemptOutcome.CANCELED)).toBe(STATUS_CLASSES[Status.CANCELED]);

	// UNREPORTED IS NOT A FAILURE. It borrows SKIPPED's dashed grey, never the
	// failed rose: nothing failed, the record is absent.
	const unreportedClasses = attemptOutcomeClasses(AttemptOutcome.UNREPORTED);
	expect(unreportedClasses).toBe(STATUS_CLASSES[Status.SKIPPED]);
	expect(unreportedClasses).toContain('border-dashed');
	expect(unreportedClasses).not.toBe(STATUS_CLASSES[Status.FAILED]);
	expect(unreportedClasses).not.toContain('failed');
});

test('the cloud and the task panel read one retry phrase off the failing task', async () => {
	const { retryPhrase } = await import('../src/lib/attempts');
	// The budget ran out: the reason, with the count where it is the point.
	expect(retryPhrase({ retryable: true, retryState: RetryState.MAXIMUM_ATTEMPTS_REACHED, attempts: 1 })).toBe('retries exhausted');
	expect(retryPhrase({ retryable: true, retryState: RetryState.MAXIMUM_ATTEMPTS_REACHED, attempts: 3 })).toBe(
		'retries exhausted after 3 attempts'
	);
	// No reason recorded: only "not retryable" is a fact worth a word.
	expect(retryPhrase({ retryable: false, attempts: 1 })).toBe('not retryable');
	expect(retryPhrase({ retryable: true, attempts: 1 })).toBeNull();
	// No task to blame (a run that failed on its own): nothing about retries.
	expect(retryPhrase(undefined)).toBeNull();
});
