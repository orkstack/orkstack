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

/** fmtWindow renders the question that was answered: "since X, until Y". */
export function fmtInstant(ts: Timestamp | undefined): string {
	const ms = timestampMs(ts);
	return ms === null ? UNAVAILABLE : new Date(ms).toLocaleString();
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
