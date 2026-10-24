import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { FailureCategory } from '../src/lib/gen/orkstack/v1/common_pb';
import {
	FailureGroupBy,
	FlakeGroupSchema,
	GetFlakeIndexResponseSchema
} from '../src/lib/gen/orkstack/v1/stats_pb';
import {
	FLAKE_DIMENSIONS,
	dominantCauseLabel,
	flakeBadgeLabel,
	flakeFooter,
	flakeGroupBy,
	flakeHistoryTitle,
	flakesCli,
	fmtAttemptsPerRun,
	fmtRetryRate
} from '../src/lib/flakes';
import { DASH, UNAVAILABLE, groupBySpelling } from '../src/lib/stats';

// The Stability rows and the run page's retry history are `orkctl stats
// flakes` in a browser: the strings below are the CLI's own
// (cmd/orkctl/testdata/stats-flakes*.golden).

test('fmtRetryRate renders unavailable when undefined and a percentage otherwise', () => {
	expect(fmtRetryRate(create(FlakeGroupSchema, { retryRateDefined: false }))).toBe(UNAVAILABLE);
	expect(fmtRetryRate(create(FlakeGroupSchema, { retryRateDefined: true, retryRate: 0 }))).toBe('0.0%');
	expect(fmtRetryRate(create(FlakeGroupSchema, { retryRateDefined: true, retryRate: 2 / 3 }))).toBe('66.7%');
	expect(fmtRetryRate(create(FlakeGroupSchema, { retryRateDefined: true, retryRate: 1 }))).toBe('100.0%');
	expect(fmtAttemptsPerRun(create(FlakeGroupSchema, { retryRateDefined: false }))).toBe(DASH);
	expect(fmtAttemptsPerRun(create(FlakeGroupSchema, { retryRateDefined: true, p50Attempts: 2, p95Attempts: 3 }))).toBe(
		'p50 2 · p95 3'
	);
});

test('flakeBadgeLabel keeps the in-run label and says flaky only when this run did not retry', () => {
	const history = create(FlakeGroupSchema, { retryRateDefined: true, retryRate: 0.5, runsRetried: 1n, runsObserved: 2n });
	expect(flakeBadgeLabel('retried ×1', history)).toBe('retried ×1');
	expect(flakeBadgeLabel('retried ×1', undefined)).toBe('retried ×1');
	expect(flakeBadgeLabel(null, history)).toBe('flaky 50%');
	expect(flakeBadgeLabel(null, undefined)).toBeNull();
	expect(flakeBadgeLabel(null, create(FlakeGroupSchema, { retryRateDefined: true, runsObserved: 2n }))).toBeNull();
	expect(flakeBadgeLabel(null, create(FlakeGroupSchema, { retryRateDefined: false, runsEvidenceMissing: 3n }))).toBeNull();
	expect(flakeHistoryTitle(history, '7 days')).toBe('; retried in 1 of 2 runs of this Orkflow in the last 7 days');
	expect(flakeHistoryTitle(undefined, '7 days')).toBe('');
	expect(flakeHistoryTitle(create(FlakeGroupSchema, {}), '7 days')).toContain('unavailable');
});

test('dominantCauseLabel renders one triple with its share and a dash for none', () => {
	const group = create(FlakeGroupSchema, {
		retryAttempts: 151n,
		dominantCode: 'IMAGE_PULL_FAILED',
		dominantCategory: FailureCategory.INFRASTRUCTURE_ERROR,
		dominantPhase: 'image',
		dominantCount: 143n
	});
	expect(dominantCauseLabel(group)).toBe('INFRASTRUCTURE_ERROR IMAGE_PULL_FAILED image (143/151)');
	const canceled = create(FlakeGroupSchema, { retryAttempts: 2n, dominantPhase: 'exit', dominantCount: 2n });
	expect(dominantCauseLabel(canceled)).toBe('- - exit (2/2)');
	expect(dominantCauseLabel(create(FlakeGroupSchema, {}))).toBe(DASH);
});

test('flakesCli quotes every value and omits --orkflow when unset', () => {
	expect(flakesCli('payments', '24h', [FailureGroupBy.TASK, FailureGroupBy.ORKER])).toBe(
		'orkctl --namespace payments stats flakes --since 24h --group-by task,orker'
	);
	expect(flakesCli('', '168h', [], 'nightly')).toBe(
		'orkctl --namespace default stats flakes --orkflow nightly --since 168h'
	);
	expect(flakesCli("it's", '2026-09-24T10:00:00.000Z', [FailureGroupBy.TASK], 'a b')).toBe(
		`orkctl --namespace 'it'"'"'s' stats flakes --orkflow 'a b' --since 2026-09-24T10:00:00.000Z --group-by task`
	);
	expect(flakesCli('x', '$(rm)', [])).toBe("orkctl --namespace x stats flakes --since '$(rm)'");
});

test('the chips are the six server-accepted dimensions but orkflow, sent in enum order', () => {
	expect(FLAKE_DIMENSIONS.map(groupBySpelling)).toEqual(['task', 'pool', 'orker', 'phase', 'image']);
	expect(flakeGroupBy([FailureGroupBy.ORKER, FailureGroupBy.TASK]).map(groupBySpelling)).toEqual(['task', 'orker']);
	expect(flakeGroupBy([]).map(groupBySpelling)).toEqual(['orkflow']);
});

test('flakeFooter prints the CLI sentences from the same fields', () => {
	const resp = create(GetFlakeIndexResponseSchema, {
		runsInWindow: 7n,
		runsScanned: 7n,
		runsDispatched: 7n,
		runsEvidenceComplete: 6n,
		runsEvidenceMissing: 1n,
		groupsTotal: 4n,
		bodyKeyed: true,
		groups: [create(FlakeGroupSchema, {}), create(FlakeGroupSchema, {}), create(FlakeGroupSchema, {}), create(FlakeGroupSchema, {})]
	});
	expect(flakeFooter(resp)).toEqual([
		'scanned 7 of 7 finished runs in the window; 7 dispatched a task; attempt records complete for 6, missing for 1',
		"1 runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean"
	]);
	const keyed = create(GetFlakeIndexResponseSchema, {
		runsInWindow: 1n, runsScanned: 1n, runsDispatched: 1n, runsEvidenceMissing: 1n, groupsTotal: 2n, bodyKeyed: false,
		groups: [create(FlakeGroupSchema, {})]
	});
	expect(flakeFooter(keyed)).toEqual([
		'scanned 1 of 1 finished runs in the window; 1 dispatched a task; attempt records complete for 0, missing for 1',
		"1 runs' attempt records were reclaimed or never written: they are EXCLUDED from every rate above, not counted as clean",
		'showing 1 of 2 groups (raise --limit)',
		'retry rates are unavailable: no run in this window has an attempt record',
		'unreported and evidence-missing counts are totals only: a group keyed by orker, phase or image cannot attribute them'
	]);
});
