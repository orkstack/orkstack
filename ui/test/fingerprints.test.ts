import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { FailureCategory } from '../src/lib/gen/orkstack/v1/common_pb';
import {
	FailureGroupBy,
	FailureGroupSchema,
	GetFailureBreakdownResponseSchema
} from '../src/lib/gen/orkstack/v1/stats_pb';
import {
	DASH,
	DEFAULT_FINGERPRINT,
	FINGERPRINT_DIMENSIONS,
	UNAVAILABLE,
	fingerprintCell,
	fingerprintFooter,
	fmtCategory,
	groupBySpelling
} from '../src/lib/stats';

// The dashboard's fingerprint rows are `orkctl stats failures` in a browser:
// the same spellings, the same `unavailable` versus `-`, the same footer. The
// strings below are the CLI's own (cmd/orkctl/testdata/stats-failures*.golden).

test('every dimension has the CLI flag spelling, in the enum order', () => {
	expect(FINGERPRINT_DIMENSIONS.map(groupBySpelling)).toEqual([
		'orkflow', 'task', 'code', 'category', 'image', 'pool', 'phase', 'orker'
	]);
	expect(DEFAULT_FINGERPRINT.map(groupBySpelling).join(',')).toBe('orkflow,task,code,category');
	// Every value of the generated enum but UNSPECIFIED is a dimension here: a
	// value added to the proto without a column fails this test.
	const generated = Object.values(FailureGroupBy).filter((v) => typeof v === 'number' && v !== 0);
	expect(generated.length).toBe(FINGERPRINT_DIMENSIONS.length);
});

test('a category is its short form, and an unspecified one is a dash', () => {
	expect(fmtCategory(FailureCategory.ACTIVITY_FAILED)).toBe('ACTIVITY_FAILED');
	expect(fmtCategory(FailureCategory.CANCELLED)).toBe('CANCELLED');
	expect(fmtCategory(FailureCategory.UNSPECIFIED)).toBe(DASH);
});

test('reclaimed evidence is unavailable; a value the run never had is a dash', () => {
	const reclaimed = create(FailureGroupSchema, { taskId: 'reconcile', code: 'CONTAINER_EXIT_NONZERO', attemptEvidenceMissing: 1n });
	expect(fingerprintCell(reclaimed, FailureGroupBy.IMAGE)).toEqual({ text: UNAVAILABLE, unavailable: true });
	expect(fingerprintCell(reclaimed, FailureGroupBy.PHASE)).toEqual({ text: UNAVAILABLE, unavailable: true });
	// Only image and phase come from attempt records; a missing pool is not
	// "unavailable", it is a task that never reached one.
	expect(fingerprintCell(reclaimed, FailureGroupBy.POOL)).toEqual({ text: DASH, unavailable: false });

	const runLevel = create(FailureGroupSchema, { orkflowName: 'nightly', code: 'RUN_START_FAILED' });
	expect(fingerprintCell(runLevel, FailureGroupBy.TASK)).toEqual({ text: DASH, unavailable: false });
	expect(fingerprintCell(runLevel, FailureGroupBy.IMAGE)).toEqual({ text: DASH, unavailable: false });
	expect(fingerprintCell(runLevel, FailureGroupBy.CATEGORY)).toEqual({ text: DASH, unavailable: false });
	expect(fingerprintCell(runLevel, FailureGroupBy.ORKFLOW)).toEqual({ text: 'nightly', unavailable: false });
});

test('the footer is the CLI footer, sentence for sentence', () => {
	const resp = create(GetFailureBreakdownResponseSchema, {
		groups: [
			create(FailureGroupSchema, { runCount: 2n }),
			create(FailureGroupSchema, { runCount: 1n, attemptEvidenceMissing: 1n })
		],
		failedRunsInWindow: 6n,
		failedRunsScanned: 6n,
		failedRunsGrouped: 6n,
		groupsTotal: 4n,
		attemptEvidenceMissing: 1n
	});
	expect(fingerprintFooter(resp)).toEqual([
		'scanned 6 of 6 failed runs in the window; 6 grouped into 4 groups',
		'showing 2 of 4 groups (raise --limit)',
		'image and phase unavailable for 1 group: attempt records for 1 run were reclaimed or never written'
	]);
	const filtered = create(GetFailureBreakdownResponseSchema, {
		groups: [create(FailureGroupSchema, { runCount: 2n })],
		failedRunsInWindow: 6n, failedRunsScanned: 6n, failedRunsGrouped: 2n, groupsTotal: 1n
	});
	expect(fingerprintFooter(filtered, FailureCategory.ACTIVITY_FAILED)).toEqual([
		'scanned 6 of 6 failed runs in the window; 2 grouped into 1 group',
		'category ACTIVITY_FAILED: 2 of 6 failed runs'
	]);
});
