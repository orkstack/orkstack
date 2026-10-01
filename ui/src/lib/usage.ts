import type { Duration } from '@bufbuild/protobuf/wkt';
import type { Budget } from './gen/orkstack/v1/namespace_pb';
import { FailureGroupBy, type GetUsageResponse, type UsageRow } from './gen/orkstack/v1/stats_pb';
import { shellWord } from './flakes';
import { DASH, groupBySpelling, timestampMs } from './stats';

// ---- namespace usage and budgets ---------------------------------------------
//
// The dashboard's Usage card, Admin → Usage and Admin → Namespaces' Budget
// column render the same GetUsageResponse and Budget `orkctl stats usage` and
// `orkctl namespace list` render, and these helpers are the browser's half of
// that parity: the same units, the same reserved-not-used caveat, and the same
// footer sentences from the same fields (cmd/orkctl/stats_usage.go,
// cmd/orkctl/namespace.go).

/** The sentence every usage surface prints, always — the CLI's own literal. */
export const USAGE_CAVEAT =
	'cpu-seconds and GiB-seconds are RESERVED cpu and memory × attempt time, not measured utilization.';

/**
 * BUDGET_WARN_RATIO tints a bar as a warning once this share of a bounded
 * dimension is reserved. A presentation tint only, never a rule: only the
 * server's budget_reached means the bound was reached.
 */
export const BUDGET_WARN_RATIO = 0.8;

/** Admin → Usage's window presets, the `--since` values it offers. */
export const USAGE_WINDOWS = ['24h', '168h', '720h'] as const;

/** How often the Usage card re-asks, on its own timer. */
export const USAGE_REFRESH_MS = 60_000;

/** The dimensions a usage row can be keyed by, in the enum's order. */
export const USAGE_DIMENSIONS: FailureGroupBy[] = [FailureGroupBy.ORKFLOW, FailureGroupBy.TASK, FailureGroupBy.POOL];

/** fmtUnitSeconds renders one of the three sums: whole seconds, no grouping. */
export function fmtUnitSeconds(n: number): string {
	return String(Math.round(n));
}

function durationNanos(d: Duration | undefined): bigint {
	if (!d) return 0n;
	return d.seconds * 1_000_000_000n + BigInt(d.nanos);
}

/**
 * fmtGoDuration spells a duration the way Go's time.Duration.String does —
 * `720h0m0s`, `1h0m0s`, `1m30s` — so a budget's period reads the same here
 * as in the CLI and in every server sentence.
 */
export function fmtGoDuration(d: Duration | undefined): string {
	let nanos = durationNanos(d);
	if (nanos === 0n) return '0s';
	const negative = nanos < 0n;
	if (negative) nanos = -nanos;
	const hours = nanos / 3_600_000_000_000n;
	const minutes = (nanos / 60_000_000_000n) % 60n;
	const secondNanos = nanos % 60_000_000_000n;
	const whole = secondNanos / 1_000_000_000n;
	const frac = secondNanos % 1_000_000_000n;
	let seconds = String(whole);
	if (frac > 0n) seconds += '.' + String(frac).padStart(9, '0').replace(/0+$/, '');
	let out = '';
	if (hours > 0n) out = `${hours}h${minutes}m${seconds}s`;
	else if (minutes > 0n) out = `${minutes}m${seconds}s`;
	else out = `${seconds}s`;
	return negative ? `-${out}` : out;
}

/** budgetConfigLabel is the CLI's `namespace list` BUDGET cell. */
export function budgetConfigLabel(b: Budget | undefined | null): string {
	if (!b) return 'unbounded';
	const bounds: string[] = [];
	if (b.cpuSeconds > 0n) bounds.push(`${b.cpuSeconds} cpu-s`);
	if (b.gibSeconds > 0n) bounds.push(`${b.gibSeconds} GiB-s`);
	return `${bounds.join(' + ')}/${fmtGoDuration(b.period)} ${b.enforce ? 'enforced' : 'soft'}`;
}

/** budgetModeLabel is the card's sentence for what the budget does. */
export function budgetModeLabel(b: Budget): string {
	return b.enforce ? 'enforced — new runs are refused at the bound' : 'soft — reported, never refuses';
}

export interface BudgetShareCell {
	dim: 'cpu' | 'gib';
	unit: 'cpu-seconds' | 'GiB-seconds';
	short: 'cpu-s' | 'GiB-s';
	used: number;
	bound: number;
	ratio: number;
}

function dimensions(b: Budget, total: UsageRow | undefined): BudgetShareCell[] {
	const cells: BudgetShareCell[] = [];
	if (b.cpuSeconds > 0n) {
		const used = total?.cpuSeconds ?? 0;
		const bound = Number(b.cpuSeconds);
		cells.push({ dim: 'cpu', unit: 'cpu-seconds', short: 'cpu-s', used, bound, ratio: used / bound });
	}
	if (b.gibSeconds > 0n) {
		const used = total?.gibSeconds ?? 0;
		const bound = Number(b.gibSeconds);
		cells.push({ dim: 'gib', unit: 'GiB-seconds', short: 'GiB-s', used, bound, ratio: used / bound });
	}
	return cells;
}

/**
 * budgetShareCells is one cell per bounded dimension — ONLY when the answer
 * is the budget's own window (budget_window): a share of a budget over any
 * other window is a number that means nothing.
 */
export function budgetShareCells(resp: GetUsageResponse): BudgetShareCell[] {
	if (!resp.budget || !resp.budgetWindow) return [];
	return dimensions(resp.budget, resp.total);
}

/** shareLabel is a bar's label: `used / bound unit (P%)`. */
export function shareLabel(cell: BudgetShareCell): string {
	return `${fmtUnitSeconds(cell.used)} / ${cell.bound} ${cell.unit} (${Math.round(cell.ratio * 100)}%)`;
}

/** shareTone is a bar's tint: danger when reached, warning past the ratio. */
export function shareTone(cell: BudgetShareCell, reached: boolean): 'danger' | 'warning' | 'ok' {
	if (reached) return 'danger';
	return cell.ratio >= BUDGET_WARN_RATIO ? 'warning' : 'ok';
}

/**
 * budgetCellLabel is `orkctl stats usage --group-by namespace`'s BUDGET cell:
 * the worst bounded dimension's share in the budget window, the budget alone
 * outside it, or the word unbounded.
 */
export function budgetCellLabel(resp: GetUsageResponse): string {
	const b = resp.budget;
	if (!b) return 'unbounded';
	const period = fmtGoDuration(b.period);
	const cells = dimensions(b, resp.total);
	if (!resp.budgetWindow) {
		return `${cells.map((c) => `${c.bound} ${c.short}`).join(' + ')} per ${period} (window differs)`;
	}
	let worst = cells[0];
	for (const cell of cells) if (cell.ratio > worst.ratio) worst = cell;
	let label = `${Math.round(worst.ratio * 100)}% of ${worst.bound} ${worst.short} per ${period}, ${b.enforce ? 'enforced' : 'soft'}`;
	if (resp.budgetReached) label += ', reached';
	return label;
}

function rfc3339(ms: number | null): string {
	return ms === null ? DASH : new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z');
}

/**
 * usageFooter is the sentences under the table, in the order and under the
 * conditions `orkctl stats usage` prints them (cmd/orkctl/stats_usage.go,
 * usageFooter). The caveat is always first.
 */
export function usageFooter(resp: GetUsageResponse): string[] {
	const total = resp.total;
	const lines = [USAGE_CAVEAT];
	if (resp.runsInWindow > 0n) {
		lines.push(
			`scanned ${resp.runsScanned} of ${resp.runsInWindow} finished runs in the window; ${resp.runsDispatched} dispatched a task; attempt records complete for ${resp.runsEvidenceComplete}, missing for ${resp.runsEvidenceMissing}`
		);
	}
	if (resp.runsEvidenceMissing > 0n) {
		lines.push(
			`${resp.runsEvidenceMissing} runs' attempt records were reclaimed or never written: their consumption is NOT in these numbers`
		);
	}
	if (resp.coverageSince) {
		lines.push(`coverage begins ${rfc3339(timestampMs(resp.coverageSince))}, the oldest run in this window with attempt records`);
	} else if (resp.runsDispatched > 0n) {
		lines.push(
			'no run in this window has an attempt record: every sum is zero because nothing was measured, not because nothing ran'
		);
	}
	const without = total?.attemptsWithoutResources ?? 0n;
	if (without > 0n) {
		lines.push(
			`${without} attempts carry no reservation (recorded before this release, or refused): their time is in TASK-SECONDS, not in CPU- or GiB-SECONDS`
		);
	}
	const unreported = total?.unreportedAttempts ?? 0n;
	if (unreported > 0n) {
		lines.push(`${unreported} attempts left no record (their Orker died): their consumption is unknown and not counted`);
	}
	const unmeasured = total?.attemptsUnmeasured ?? 0n;
	if (unmeasured > 0n) {
		lines.push(`${unmeasured} attempt records have no usable start and finish time and are counted in nothing`);
	}
	lines.push('runs still in flight are counted when they finish');
	return lines;
}

/** usageCell renders one keyed dimension of one row; empty is a dash. */
export function usageCell(row: UsageRow, dim: FailureGroupBy): string {
	switch (dim) {
		case FailureGroupBy.ORKFLOW: return row.orkflowName || DASH;
		case FailureGroupBy.TASK: return row.taskId || DASH;
		case FailureGroupBy.POOL: return row.pool || DASH;
		default: return DASH;
	}
}

/**
 * usageCli is the `orkctl stats usage` command that asks what the card asked:
 * the namespace always stated, --group-by only when dimensions are.
 */
export function usageCli(namespace: string, window: string, dims: FailureGroupBy[] = []): string {
	const parts = ['orkctl', '--namespace', shellWord(namespace || 'default'), 'stats', 'usage', '--since', shellWord(window)];
	if (dims.length) parts.push('--group-by', shellWord(dims.map(groupBySpelling).join(',')));
	return parts.join(' ');
}

/** adminUsageCli is Admin → Usage's command: every namespace side by side. */
export function adminUsageCli(window: string): string {
	return `orkctl stats usage --group-by namespace --since ${shellWord(window)}`;
}
