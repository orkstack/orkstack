import { FailureGroupBy, type FlakeGroup, type GetFlakeIndexResponse } from './gen/orkstack/v1/stats_pb';
import { DASH, UNAVAILABLE, fmtCategory, groupBySpelling } from './stats';

// ---- flake index -----------------------------------------------------------
//
// The dashboard's Stability rows, the run page's retry history and `orkctl
// stats flakes` render the same GetFlakeIndexResponse, and these helpers are
// the browser's half of that parity: the same rate spelling, the same
// `unavailable` for a task no run in the window has a record of — never 0% —
// and the same footer sentences from the same fields
// (cmd/orkctl/stats_flakes.go).

/**
 * The dimension chips, in the order the page offers them. ORKFLOW has no chip:
 * it is always a column, because a task id names a task only within its
 * orkflow. CODE and CATEGORY are not dimensions here at all — a flake group
 * REPORTS them as its dominant cause.
 */
export const FLAKE_DIMENSIONS: FailureGroupBy[] = [
	FailureGroupBy.TASK,
	FailureGroupBy.POOL,
	FailureGroupBy.ORKER,
	FailureGroupBy.PHASE,
	FailureGroupBy.IMAGE
];

/** The chips ticked when the page opens: the server's own default. */
export const DEFAULT_FLAKE_DIMENSIONS: FailureGroupBy[] = [FailureGroupBy.TASK];

/**
 * flakeGroupBy is what the chips send, in the enum's order whatever order they
 * were ticked in, so the columns and the CLI's --group-by read the same way
 * every time. Nothing ticked asks "which orkflow retries most".
 */
export function flakeGroupBy(ticked: FailureGroupBy[]): FailureGroupBy[] {
	const dims = FLAKE_DIMENSIONS.filter((dim) => ticked.includes(dim)).sort((a, b) => a - b);
	return dims.length ? dims : [FailureGroupBy.ORKFLOW];
}

/** fmtRetryRate renders a group's rate as the CLI does, or UNAVAILABLE. */
export function fmtRetryRate(group: FlakeGroup): string {
	if (!group.retryRateDefined) return UNAVAILABLE;
	return `${(group.retryRate * 100).toFixed(1)}%`;
}

/** fmtAttemptsPerRun renders the nearest-rank attempts per run, or a dash. */
export function fmtAttemptsPerRun(group: FlakeGroup): string {
	if (!group.retryRateDefined) return DASH;
	return `p50 ${group.p50Attempts} · p95 ${group.p95Attempts}`;
}

/**
 * dominantCauseLabel renders the ONE triple the most retries share — never
 * three independent modes — with its share: `CATEGORY CODE phase (n/retries)`,
 * each empty component a dash, the whole a dash when nothing retried.
 */
export function dominantCauseLabel(group: FlakeGroup): string {
	if (group.retryAttempts === 0n) return DASH;
	return `${fmtCategory(group.dominantCategory)} ${group.dominantCode || DASH} ${group.dominantPhase || DASH} (${group.dominantCount}/${group.retryAttempts})`;
}

/** flakeCell renders one keyed dimension of one group; empty is a dash. */
export function flakeCell(group: FlakeGroup, dim: FailureGroupBy): string {
	switch (dim) {
		case FailureGroupBy.ORKFLOW: return group.orkflowName || DASH;
		case FailureGroupBy.TASK: return group.taskId || DASH;
		case FailureGroupBy.IMAGE: return group.imagePinned || DASH;
		case FailureGroupBy.POOL: return group.pool || DASH;
		case FailureGroupBy.PHASE: return group.phase || DASH;
		case FailureGroupBy.ORKER: return group.orkerId || DASH;
		default: return DASH;
	}
}

/**
 * flakeFooter is the sentences under the table, in the order `orkctl stats
 * flakes` prints them (cmd/orkctl/stats_flakes.go, flakeFooter).
 */
export function flakeFooter(resp: GetFlakeIndexResponse): string[] {
	const dispatched = resp.runsDispatched;
	const complete = resp.runsEvidenceComplete;
	const missing = resp.runsEvidenceMissing;
	const lines = [
		`scanned ${resp.runsScanned} of ${resp.runsInWindow} finished runs in the window; ${dispatched} dispatched a task; attempt records complete for ${complete}, missing for ${missing}`
	];
	if (missing > 0n) {
		lines.push(
			`${missing} runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean`
		);
	}
	const shown = BigInt(resp.groups.length);
	if (shown < resp.groupsTotal) lines.push(`showing ${shown} of ${resp.groupsTotal} groups (raise --limit)`);
	if (dispatched > 0n && complete === 0n) {
		lines.push('retry rates are unavailable: no run in this window has an attempt record');
	}
	if (!resp.bodyKeyed) {
		lines.push(
			'unreported and evidence-missing counts are totals only: a group keyed by orker, phase or image cannot attribute them'
		);
	}
	return lines;
}

/**
 * flakeBadgeLabel is the task card's badge. The in-run label (this run
 * retried the task) always wins and is unchanged; only when this run did NOT
 * retry and the task's history says it has retried elsewhere does the badge
 * say `flaky N%`. No history, or none that retried: nothing to say.
 */
export function flakeBadgeLabel(inRunLabel: string | null, history: FlakeGroup | undefined): string | null {
	if (inRunLabel) return inRunLabel;
	if (!history || !history.retryRateDefined || history.runsRetried === 0n) return null;
	return `flaky ${Math.round(history.retryRate * 100)}%`;
}

/**
 * flakeHistoryTitle is what the badge's title gains once history is known:
 * how many of this orkflow's runs retried the task over the window. It is
 * empty when there is no history, and says so plainly when the window holds
 * no record of the task — never a 0%.
 */
export function flakeHistoryTitle(history: FlakeGroup | undefined, window: string): string {
	if (!history) return '';
	if (!history.retryRateDefined) {
		return `; retry history unavailable: no attempt record of this task in the last ${window}`;
	}
	return `; retried in ${history.runsRetried} of ${history.runsObserved} runs of this Orkflow in the last ${window}`;
}

/**
 * shellWord quotes a value for a POSIX shell only when it needs it: a value
 * of safe characters is already one word and stays readable
 * (`--group-by task,orker`), anything else is single-quoted.
 */
export function shellWord(value: string): string {
	if (value !== '' && /^[A-Za-z0-9_,./:=@%+-]+$/.test(value)) return value;
	return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

/**
 * flakesCli is the `orkctl stats flakes` command that asks what the page
 * asked. Every value is shell-safe; --orkflow is present only when one is
 * named, and --group-by only when dimensions are.
 */
export function flakesCli(namespace: string, window: string, dims: FailureGroupBy[], orkflow?: string): string {
	const parts = ['orkctl', '--namespace', shellWord(namespace || 'default'), 'stats', 'flakes'];
	if (orkflow) parts.push('--orkflow', shellWord(orkflow));
	parts.push('--since', shellWord(window));
	if (dims.length) parts.push('--group-by', shellWord(dims.map(groupBySpelling).join(',')));
	return parts.join(' ');
}
