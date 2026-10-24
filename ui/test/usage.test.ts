import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { durationFromMs, timestampFromMs } from '@bufbuild/protobuf/wkt';
import { BudgetSchema } from '../src/lib/gen/orkstack/v1/namespace_pb';
import { FailureGroupBy, GetUsageResponseSchema, UsageRowSchema } from '../src/lib/gen/orkstack/v1/stats_pb';
import {
	BUDGET_WARN_RATIO,
	USAGE_CAVEAT,
	adminUsageCli,
	budgetCellLabel,
	budgetConfigLabel,
	budgetShareCells,
	fmtGoDuration,
	fmtUnitSeconds,
	usageCli,
	usageFooter
} from '../src/lib/usage';

// The Usage card, Admin → Usage and the Namespaces Budget column are `orkctl
// stats usage` and `orkctl namespace list` in a browser: the strings below
// are the CLI's own (cmd/orkctl/testdata/stats-usage*.golden and
// namespace-*.golden).

const HOUR = 3_600_000;

function budget(cpu: bigint, gib: bigint, hours: number, enforce: boolean) {
	return create(BudgetSchema, { cpuSeconds: cpu, gibSeconds: gib, period: durationFromMs(hours * HOUR), enforce });
}

test('fmtUnitSeconds rounds to whole seconds with no grouping, as the CLI does', () => {
	expect(fmtUnitSeconds(0)).toBe('0');
	expect(fmtUnitSeconds(8205762.4)).toBe('8205762');
	expect(fmtUnitSeconds(1474.6)).toBe('1475');
});

test('fmtGoDuration spells a period as Go time.Duration.String does', () => {
	expect(fmtGoDuration(durationFromMs(720 * HOUR))).toBe('720h0m0s');
	expect(fmtGoDuration(durationFromMs(HOUR))).toBe('1h0m0s');
	expect(fmtGoDuration(durationFromMs(90_000))).toBe('1m30s');
	expect(fmtGoDuration(durationFromMs(0))).toBe('0s');
});

test('budgetConfigLabel is the CLI list cell', () => {
	expect(budgetConfigLabel(undefined)).toBe('unbounded');
	expect(budgetConfigLabel(budget(5000000n, 0n, 720, true))).toBe('5000000 cpu-s/720h0m0s enforced');
	expect(budgetConfigLabel(budget(5000000n, 20000000n, 720, false))).toBe('5000000 cpu-s + 20000000 GiB-s/720h0m0s soft');
	expect(budgetConfigLabel(budget(1n, 0n, 1, true))).toBe('1 cpu-s/1h0m0s enforced');
});

function response(fields: Record<string, unknown>) {
	return create(GetUsageResponseSchema, fields);
}

test('budgetShareCells exist only in the budget window, one per bounded dimension', () => {
	const total = create(UsageRowSchema, { cpuSeconds: 755, gibSeconds: 1475 });
	const b = budget(500n, 2000n, 720, true);
	expect(budgetShareCells(response({ total, budget: b, budgetWindow: false }))).toEqual([]);
	const cells = budgetShareCells(response({ total, budget: b, budgetWindow: true, budgetReached: true }));
	expect(cells.map((c) => c.dim)).toEqual(['cpu', 'gib']);
	expect([cells[0].unit, cells[0].used, cells[0].bound]).toEqual(['cpu-seconds', 755, 500]);
	expect(Math.abs(cells[0].ratio - 1.51) < 1e-12).toBe(true);
	expect(Math.abs(cells[1].ratio - 0.7375) < 1e-12).toBe(true);
	expect(budgetShareCells(response({ total, budgetWindow: true }))).toEqual([]);
	expect(BUDGET_WARN_RATIO).toBe(0.8);
});

test('budgetCellLabel is the --group-by namespace BUDGET cell for its three cases', () => {
	const total = create(UsageRowSchema, { cpuSeconds: 755 });
	expect(budgetCellLabel(response({ total }))).toBe('unbounded');
	expect(budgetCellLabel(response({ total, budget: budget(500n, 0n, 720, true), budgetWindow: true, budgetReached: true }))).toBe(
		'151% of 500 cpu-s per 720h0m0s, enforced, reached'
	);
	expect(budgetCellLabel(response({ total, budget: budget(1n, 0n, 1, true), budgetWindow: false }))).toBe(
		'1 cpu-s per 1h0m0s (window differs)'
	);
	expect(budgetCellLabel(response({ total, budget: budget(10000n, 0n, 720, false), budgetWindow: true }))).toBe(
		'8% of 10000 cpu-s per 720h0m0s, soft'
	);
});

test('usageFooter returns the CLI sentences under the same conditions', () => {
	// stats-usage-missing.golden
	const missing = response({
		total: create(UsageRowSchema, { attemptsWithoutResources: 1n, unreportedAttempts: 1n }),
		runsInWindow: 2n,
		runsScanned: 2n,
		runsDispatched: 2n,
		runsEvidenceComplete: 1n,
		runsEvidenceMissing: 1n,
		coverageSince: timestampFromMs(Date.parse('2026-01-02T02:04:05Z'))
	});
	expect(usageFooter(missing)).toEqual([
		USAGE_CAVEAT,
		'scanned 2 of 2 finished runs in the window; 2 dispatched a task; attempt records complete for 1, missing for 1',
		"1 runs' attempt records were reclaimed or never written: their consumption is NOT in these numbers",
		'coverage begins 2026-01-02T02:04:05Z, the oldest run in this window with attempt records',
		'1 attempts carry no reservation (recorded before this release, or refused): their time is in TASK-SECONDS, not in CPU- or GiB-SECONDS',
		'1 attempts left no record (their Orker died): their consumption is unknown and not counted',
		'runs still in flight are counted when they finish'
	]);
	// stats-usage-empty.golden
	expect(usageFooter(response({ total: create(UsageRowSchema, {}) }))).toEqual([
		USAGE_CAVEAT,
		'runs still in flight are counted when they finish'
	]);
	const nothingMeasured = usageFooter(
		response({ total: create(UsageRowSchema, { attemptsUnmeasured: 2n }), runsInWindow: 1n, runsScanned: 1n, runsDispatched: 1n, runsEvidenceMissing: 1n })
	);
	expect(nothingMeasured).toContain(
		'no run in this window has an attempt record: every sum is zero because nothing was measured, not because nothing ran'
	);
	expect(nothingMeasured).toContain('2 attempt records have no usable start and finish time and are counted in nothing');
	expect(USAGE_CAVEAT).toBe('cpu-seconds and GiB-seconds are RESERVED cpu and memory × attempt time, not measured utilization.');
});

test('usageCli and adminUsageCli are the commands the pages ran', () => {
	expect(usageCli('budgets-e2e', '1h0m0s')).toBe('orkctl --namespace budgets-e2e stats usage --since 1h0m0s');
	expect(usageCli('', '24h', [FailureGroupBy.ORKFLOW, FailureGroupBy.POOL])).toBe(
		'orkctl --namespace default stats usage --since 24h --group-by orkflow,pool'
	);
	expect(usageCli("o'brien", '2026-01-02T00:00:00.000Z')).toBe(
		"orkctl --namespace 'o'\"'\"'brien' stats usage --since 2026-01-02T00:00:00.000Z"
	);
	expect(adminUsageCli('24h')).toBe('orkctl stats usage --group-by namespace --since 24h');
});
