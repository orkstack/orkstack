import type { Duration, Timestamp } from '@bufbuild/protobuf/wkt';

// UNAVAILABLE is what a number that cannot be answered renders as.
//
// It is never a zero, and this is the whole rule the dashboard is built on. A
// blank success rate and a 0% success rate look the same to nobody: one says
// nothing finished, the other says everything failed. GetStats carries the
// distinction explicitly — success_rate_defined, duration_sample_count — for
// exactly this, and `orkctl stats` spells it the same way (cmd/orkctl/stats.go).
export const UNAVAILABLE = 'unavailable';

/** durationMs converts a proto Duration to milliseconds. */
export function durationMs(d: Duration | undefined): number | null {
	if (!d) return null;
	return Number(d.seconds) * 1000 + d.nanos / 1e6;
}

/** timestampMs converts a proto Timestamp to milliseconds. */
export function timestampMs(ts: Timestamp | undefined): number | null {
	if (!ts) return null;
	return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
}

/**
 * fmtDuration renders a duration a person reads at a glance. An absent
 * duration is UNAVAILABLE, never "0s": zero is a real measurement — a run that
 * finished instantly — and a missing one must not borrow its face.
 */
export function fmtDuration(d: Duration | undefined): string {
	const ms = durationMs(d);
	if (ms === null) return UNAVAILABLE;
	if (ms < 1000) return `${Math.round(ms)}ms`;
	const seconds = ms / 1000;
	if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
	const minutes = Math.floor(seconds / 60);
	const rest = Math.round(seconds - minutes * 60);
	if (minutes < 60) return `${minutes}m ${rest}s`;
	const hours = Math.floor(minutes / 60);
	return `${hours}h ${minutes - hours * 60}m`;
}

/** fmtPercent renders a 0..1 rate as a percentage. */
export function fmtPercent(rate: number): string {
	return `${(rate * 100).toFixed(rate >= 0.995 || rate === 0 ? 0 : 1)}%`;
}

/**
 * fmtCount renders a tally. An ABSENT key is unavailable; a present zero is
 * zero. GetStats always sends the four status keys, so a blank here means the
 * answer did not contain the number, which is a fact worth showing rather than
 * hiding behind a plausible 0.
 */
export function fmtCount(value: bigint | undefined): string {
	return value === undefined ? UNAVAILABLE : value.toString();
}

/**
 * fmtInstantMs renders a moment in milliseconds.
 *
 * Split out so that a window read from the URL — `?since=<iso>`, which is not
 * a proto Timestamp — is shown in the SAME format as the window the engine
 * answered with. Two spellings of one instant on one page is how a control and
 * the header line come to disagree about the same number.
 */
export function fmtInstantMs(ms: number): string {
	return Number.isFinite(ms) ? new Date(ms).toLocaleString() : UNAVAILABLE;
}

/** fmtWindow renders the question that was answered: "since X, until Y". */
export function fmtInstant(ts: Timestamp | undefined): string {
	const ms = timestampMs(ts);
	return ms === null ? UNAVAILABLE : fmtInstantMs(ms);
}

/** fmtAge renders how old a measurement is, in words. */
export function fmtAge(ts: Timestamp | undefined, now: number): string {
	const ms = timestampMs(ts);
	if (ms === null) return UNAVAILABLE;
	const seconds = Math.max(0, Math.round((now - ms) / 1000));
	if (seconds < 2) return 'just now';
	if (seconds < 60) return `${seconds}s ago`;
	const minutes = Math.round(seconds / 60);
	if (minutes < 60) return `${minutes}m ago`;
	return `${Math.round(minutes / 60)}h ago`;
}

/**
 * finishedRuns counts the window's runs that have ENDED: succeeded, failed or
 * canceled.
 *
 * It is deliberately not `every key added up`. Three of the four keys are
 * about the window's population — the runs it created — but `running` is a
 * fact about NOW (proto/orkstack/v1/stats.proto: "The one exception is
 * `running` ... it is a fact about NOW rather than about the window", and
 * internal/engine/stats.go: "a run that started last week and is still going
 * is running"). Adding it to a denominator mixes two populations: one
 * long-lived run started before the window inflates the total, and a count
 * drawn only from runs the window created becomes a share of a set it is not
 * drawn from.
 *
 * It is the same set `success_rate` is a rate over and the same one the
 * percentiles are sampled from, and the same arithmetic `orkctl stats` does
 * (cmd/orkctl/stats.go, successRateLabel). One number, one population, in one
 * place — a second spelling is how the CLI and the UI came to disagree about
 * the denominator of the same number.
 */
export function finishedRuns(counts: Record<string, bigint> | undefined): number {
	const at = (key: string) => Number(counts?.[key] ?? 0n);
	return at('succeeded') + at('failed') + at('canceled');
}
