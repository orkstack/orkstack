import type { Duration, Timestamp } from '@bufbuild/protobuf/wkt';
import { FailureCategory } from './gen/orkstack/v1/common_pb';
import { FailureGroupBy, type FailureGroup, type GetFailureBreakdownResponse } from './gen/orkstack/v1/stats_pb';

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

// ---- failure fingerprints --------------------------------------------------
//
// The dashboard's fingerprint rows and `orkctl stats failures` render the same
// GetFailureBreakdownResponse, and these helpers are the browser's half of
// that parity: the same spellings, the same `unavailable` versus `-` rule, the
// same footer sentences from the same fields (cmd/orkctl/stats_failures.go).

/** DASH is an empty value the snapshot genuinely does not carry. */
export const DASH = '-';

/** Every fingerprint dimension, in the enum's (and the table's) order. */
export const FINGERPRINT_DIMENSIONS: FailureGroupBy[] = [
	FailureGroupBy.ORKFLOW,
	FailureGroupBy.TASK,
	FailureGroupBy.CODE,
	FailureGroupBy.CATEGORY,
	FailureGroupBy.IMAGE,
	FailureGroupBy.POOL,
	FailureGroupBy.PHASE,
	FailureGroupBy.ORKER
];

/** The dimensions an empty group_by resolves to, server-side and here. */
export const DEFAULT_FINGERPRINT: FailureGroupBy[] = [
	FailureGroupBy.ORKFLOW,
	FailureGroupBy.TASK,
	FailureGroupBy.CODE,
	FailureGroupBy.CATEGORY
];

/** groupBySpelling is a dimension in `--group-by`'s lower-case vocabulary. */
export function groupBySpelling(dim: FailureGroupBy): string {
	return dim === FailureGroupBy.UNSPECIFIED ? '' : (FailureGroupBy[dim] ?? '').toLowerCase();
}

/**
 * fmtCategory renders a §4 failure category in the JSON dialect's short form
 * (`ACTIVITY_FAILED`); an unspecified one is a dash, never a guess.
 */
export function fmtCategory(category: FailureCategory): string {
	return category === FailureCategory.UNSPECIFIED ? DASH : (FailureCategory[category] ?? String(category));
}

/**
 * fingerprintCell renders one dimension of one group.
 *
 * An empty image or phase in a group whose attempt evidence is missing is
 * UNAVAILABLE — the record was reclaimed or never written. Every other empty
 * value is a dash: the snapshot genuinely carries none. The proto names these
 * as two distinct facts (stats.proto, FailureGroup), and they must not render
 * alike.
 */
export function fingerprintCell(group: FailureGroup, dim: FailureGroupBy): { text: string; unavailable: boolean } {
	let value = '';
	switch (dim) {
		case FailureGroupBy.ORKFLOW: value = group.orkflowName; break;
		case FailureGroupBy.TASK: value = group.taskId; break;
		case FailureGroupBy.CODE: value = group.code; break;
		case FailureGroupBy.CATEGORY: return { text: fmtCategory(group.category), unavailable: false };
		case FailureGroupBy.IMAGE: value = group.imagePinned; break;
		case FailureGroupBy.POOL: value = group.pool; break;
		case FailureGroupBy.PHASE: value = group.phase; break;
		case FailureGroupBy.ORKER: value = group.orkerId; break;
	}
	if (!value && group.attemptEvidenceMissing > 0n && (dim === FailureGroupBy.IMAGE || dim === FailureGroupBy.PHASE)) {
		return { text: UNAVAILABLE, unavailable: true };
	}
	return { text: value || DASH, unavailable: false };
}

function plural(n: bigint, noun: string): string {
	return `${n} ${noun}${n === 1n ? '' : 's'}`;
}

/**
 * fingerprintFooter is the sentences under the table, in the order `orkctl
 * stats failures` prints them. `category` is the one the request asked for,
 * which the response does not echo.
 */
export function fingerprintFooter(resp: GetFailureBreakdownResponse, category: FailureCategory = FailureCategory.UNSPECIFIED): string[] {
	const lines = [
		`scanned ${resp.failedRunsScanned} of ${resp.failedRunsInWindow} failed runs in the window; ${resp.failedRunsGrouped} grouped into ${plural(resp.groupsTotal, 'group')}`
	];
	const shown = BigInt(resp.groups.length);
	if (shown < resp.groupsTotal) lines.push(`showing ${shown} of ${resp.groupsTotal} groups (raise --limit)`);
	if (category !== FailureCategory.UNSPECIFIED) {
		lines.push(`category ${fmtCategory(category)}: ${resp.failedRunsGrouped} of ${resp.failedRunsScanned} failed runs`);
	}
	if (resp.attemptEvidenceMissing > 0n) {
		const groups = BigInt(resp.groups.filter((group) => group.attemptEvidenceMissing > 0n).length);
		lines.push(
			`image and phase unavailable for ${plural(groups, 'group')}: attempt records for ${plural(resp.attemptEvidenceMissing, 'run')} were reclaimed or never written`
		);
	}
	return lines;
}
