import { Status } from './gen/orkstack/v1/common_pb';
import { AttemptOutcome, RetryState, type TaskAttempt } from './gen/orkstack/v1/run_pb';
import { STATUS_CLASSES } from './status';

// WHAT A TASK'S TRIES SAY, DERIVED IN ONE PLACE.
//
// Temporal owns retries, so a task's snapshot only ever describes its FINAL
// try. `ListTaskAttempts` is the only source that describes every one of them,
// and this module is the only place the page turns that list into a count, a
// label or a colour — so the badge, the timeline and the run header cannot
// disagree about how many times something was tried.
//
// Two properties of the response are relied on and never re-derived here:
// the server orders rows by `Run.task_runs` and then ascending by attempt, and
// the numbering within a task is GAPLESS from 1 — an attempt whose Orker died
// before it could file a record comes back as an UNREPORTED placeholder rather
// than as a hole. Grouping therefore preserves arrival order rather than
// sorting, and "how many tries were there" can be read off the highest number
// instead of counted.

/**
 * UNREPORTED_CAPTION is the exact sentence docs/UI_CLI_PARITY.md requires both
 * clients to say for a synthesized row: an UNREPORTED attempt is MISSING
 * EVIDENCE, not a failure, and a reader must not be left thinking the page has
 * a bug. Kept as a constant so the CLI's wording and this one stay one string.
 */
export const UNREPORTED_CAPTION =
	'no record from the orker — it crashed or lost its heartbeat';

const OUTCOME_LABEL: Record<AttemptOutcome, string> = {
	[AttemptOutcome.UNSPECIFIED]: 'UNSPECIFIED',
	[AttemptOutcome.SUCCEEDED]: 'SUCCEEDED',
	[AttemptOutcome.FAILED]: 'FAILED',
	[AttemptOutcome.CANCELED]: 'CANCELED',
	[AttemptOutcome.UNREPORTED]: 'UNREPORTED'
};

// The outcome palette IS the status palette — a succeeded try is the green a
// succeeded task is, a failed one the rose. UNREPORTED deliberately borrows
// SKIPPED's dashed grey rather than FAILED's rose: nothing failed, the record
// is simply absent, and painting it red would report an outcome that was never
// observed.
const OUTCOME_CLASSES: Record<AttemptOutcome, string> = {
	[AttemptOutcome.UNSPECIFIED]: STATUS_CLASSES[Status.UNSPECIFIED],
	[AttemptOutcome.SUCCEEDED]: STATUS_CLASSES[Status.SUCCEEDED],
	[AttemptOutcome.FAILED]: STATUS_CLASSES[Status.FAILED],
	[AttemptOutcome.CANCELED]: STATUS_CLASSES[Status.CANCELED],
	[AttemptOutcome.UNREPORTED]: STATUS_CLASSES[Status.SKIPPED]
};

export function attemptOutcomeLabel(outcome: AttemptOutcome | undefined): string {
	return OUTCOME_LABEL[outcome ?? AttemptOutcome.UNSPECIFIED];
}

export function attemptOutcomeClasses(outcome: AttemptOutcome | undefined): string {
	return OUTCOME_CLASSES[outcome ?? AttemptOutcome.UNSPECIFIED];
}

/** isUnreported marks the rows the reader synthesized: no Orker filed them. */
export function isUnreported(attempt: TaskAttempt): boolean {
	return attempt.outcome === AttemptOutcome.UNREPORTED;
}

/**
 * groupAttemptsByTask splits one `ListTaskAttempts` response into per-task
 * timelines, KEEPING the order the server sent — task order is `Run.task_runs`
 * order, which is the order the reader already sees on the page, and attempt
 * order is ascending within it. A Map preserves insertion order, so callers
 * that iterate it get the response's contract for free.
 */
export function groupAttemptsByTask(attempts: TaskAttempt[]): Map<string, TaskAttempt[]> {
	const grouped = new Map<string, TaskAttempt[]>();
	for (const attempt of attempts) {
		const key = attemptsKey(attempt.taskId, attempt.item);
		const rows = grouped.get(key) ?? [];
		rows.push(attempt);
		grouped.set(key, rows);
	}
	return grouped;
}

/**
 * attemptsKey is the timeline a row belongs to: the task's own, or — for an
 * item of a fan-out — that item's, keyed `convert[97]`. Every item numbers
 * its attempts from 1 on its own, so folding a fan-out's rows into one
 * timeline would read 120 first tries as 119 retries.
 */
export function attemptsKey(taskId: string, item?: number): string {
	return item === undefined ? taskId : `${taskId}[${item}]`;
}

/**
 * attemptsSeen is the highest attempt number this timeline names — every try
 * anything knows happened, whether or not a record survives for it.
 *
 * It is read off the numbers rather than off `rows.length` because the length
 * is only equal to it while the response is gapless, and a length is not an
 * assertion about numbering.
 */
export function attemptsSeen(attempts: TaskAttempt[]): number {
	let highest = 0;
	for (const attempt of attempts) {
		if (attempt.attempt > highest) highest = attempt.attempt;
	}
	return highest;
}

/**
 * retryCount is how many times a task was tried AGAIN, counted from the rows
 * an Orker actually filed.
 *
 * Neither obvious shortcut is correct. `rows.length - 1` counts synthesized
 * placeholders as evidence, and `TaskRun.attempt - 1` is unreliable by
 * construction: it is 0 for a task Temporal timed out (the error carries no
 * attempt) and, for an adopted commit, the COMMITTING attempt rather than the
 * last one. So the count comes from the highest attempt number that has a real
 * record, minus the first try, floored at zero.
 */
export function retryCount(attempts: TaskAttempt[]): number {
	let highest = 0;
	for (const attempt of attempts) {
		if (isUnreported(attempt)) continue;
		if (attempt.attempt > highest) highest = attempt.attempt;
	}
	return Math.max(highest - 1, 0);
}

/**
 * runRetryCount sums the retries of every task in one response — what the run
 * header's chip states. A run with no retries answers 0, and the header shows
 * nothing rather than a zero nobody needed.
 */
export function runRetryCount(attempts: TaskAttempt[]): number {
	let total = 0;
	for (const rows of groupAttemptsByTask(attempts).values()) total += retryCount(rows);
	return total;
}

/**
 * retryBadgeLabel is the pill beside a task's status, or null when the task
 * has been tried once and there is nothing to say.
 *
 * A task still running is described by WHICH TRY IS IN FLIGHT — "attempt 2" is
 * what a reader watching it needs — while a finished one is described by how
 * many times it had to be tried again. The third case is the one only an Orker
 * crash produces: more tries happened than left records, so no retry can be
 * asserted from evidence and the badge states the count of tries instead.
 */
/**
 * retryStateLabel says WHY retrying stopped, in one phrase, or null when the
 * snapshot does not carry the answer.
 *
 * `TaskRun.retry_state` is read off Temporal's own `ActivityError.RetryState`,
 * and it is the difference between two failures that look identical on the
 * page: one the platform gave up on after its budget ran out, and one it
 * refused to try again at all. A snapshot written before the field existed
 * carries UNSPECIFIED, and says nothing rather than guessing.
 *
 * `attempts` is how many tries the timeline knows of; it is appended only when
 * the exhausted budget is the reason, where the number is the point.
 */
export function retryStateLabel(
	state: RetryState | undefined,
	attempts = 0
): string | null {
	switch (state) {
		case RetryState.NON_RETRYABLE_FAILURE:
			return 'not retried: the failure is not retryable';
		case RetryState.TIMEOUT:
			return 'retrying stopped: the task ran out of time';
		case RetryState.MAXIMUM_ATTEMPTS_REACHED:
			return attempts > 1
				? `retries exhausted after ${attempts} attempts`
				: 'retries exhausted';
		case RetryState.RETRY_POLICY_NOT_SET:
			return 'not retried: this task declares no retry policy';
		case RetryState.CANCEL_REQUESTED:
			return 'not retried: cancellation was requested';
		case RetryState.INTERNAL_SERVER_ERROR:
			return 'retrying stopped: the durable execution service reported an internal error';
		default:
			return null;
	}
}

/** What a failed task's tries add up to: the facts every failure surface reads. */
export interface FailureTries {
	/** The failure's own flag: could the platform have tried it again? */
	retryable: boolean;
	/** Why retrying stopped, when the snapshot carries it. */
	retryState?: RetryState;
	/** How many tries the attempt timeline knows of. */
	attempts: number;
}

/**
 * retryPhrase is the ONE phrase a failure says about its retries, derived
 * from the failing task's tries — the same words in the run's cloud and in
 * the task's own panel, so the two can never contradict each other.
 *
 * Why retrying stopped is the answer when the snapshot knows it. Without
 * it, only "not retryable" is a fact worth stating; a bare "retryable" on a
 * failure that has already ended says nothing a reader can act on, and
 * beside a run's own wrapper (never retryable itself) it read as the
 * opposite of the task's truth. Null means: say nothing.
 */
export function retryPhrase(tries: FailureTries | undefined): string | null {
	if (!tries) return null;
	const stopped = retryStateLabel(tries.retryState, tries.attempts);
	if (stopped) return stopped;
	return tries.retryable ? null : 'not retryable';
}

export function retryBadgeLabel(attempts: TaskAttempt[], terminal: boolean): string | null {
	const seen = attemptsSeen(attempts);
	if (seen < 2) return null;
	if (!terminal) return `attempt ${seen}`;
	const retries = retryCount(attempts);
	return retries > 0 ? `retried ×${retries}` : `${seen} attempts`;
}

/** What the run page's attempt seed read answered. */
export interface AttemptSeed {
	attempts: TaskAttempt[];
	/** Why the timeline could not be read, if it could not. */
	error: string | null;
	/**
	 * The engine has no attempt store at all (FailedPrecondition): not a
	 * transient read failure, but "no record will ever exist" — the timing
	 * derivation names it as its own reason.
	 */
	storeAbsent: boolean;
}

/**
 * readAttemptSeed reads the retry history of EVERY task of a run in one call
 * (`taskId: ''`, which the engine answers run-wide). The run page asks it
 * AFTER it has rendered — the run, its statuses and its logs need only
 * GetRun — and fills timing in when it lands. It is read tolerantly: a
 * deployment that cannot say what a task's earlier tries did still renders
 * everything else.
 */
export async function readAttemptSeed(
	runs: {
		listTaskAttempts(req: { runId: string; namespace: string; taskId: string }): Promise<{ attempts: TaskAttempt[] }>;
	},
	runId: string,
	namespace: string,
	describe: (err: unknown) => string,
	isStoreAbsent: (err: unknown) => boolean
): Promise<AttemptSeed> {
	try {
		const res = await runs.listTaskAttempts({ runId, namespace, taskId: '' });
		return { attempts: res.attempts, error: null, storeAbsent: false };
	} catch (err) {
		return { attempts: [], error: describe(err), storeAbsent: isStoreAbsent(err) };
	}
}

/** The run and terminality a seed read was issued for. */
export interface SeedKey {
	runId: string;
	terminal: boolean;
}

/**
 * seedStillCurrent says whether a later loader answer can keep the seed
 * already read (or in flight) instead of reading it again. Only one case
 * can: the same run, terminal then and terminal now. A finished run's
 * attempt records are final, and the re-read its socket's replayed terminal
 * event starts would otherwise ask the whole timeline a second time on every
 * open of a finished run. A run that is live, became terminal since, or is
 * another run reads again — its timeline may have grown.
 */
export function seedStillCurrent(prev: SeedKey | null, runId: string, terminal: boolean): boolean {
	return prev !== null && prev.runId === runId && prev.terminal && terminal;
}
