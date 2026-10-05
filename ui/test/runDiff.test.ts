import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { RunSchema, type Run } from '../src/lib/gen/orkstack/v1/run_pb';
import { DiffRunsResponseSchema, RunFieldDiffSchema } from '../src/lib/gen/orkstack/v1/service_pb';
import {
	DIFF_FIELDS,
	defaultCompareTarget,
	diffSummary,
	parseOnly,
	reasonText,
	renderDiffValue,
	unavailableLines
} from '../src/lib/runDiff';

const run = (id: string, status: Status): Run => create(RunSchema, { id, status });

// U1
test('the default comparison target is the newest other SUCCEEDED run, else the newest other, else none', () => {
	// ListRuns order: newest first.
	const runs = [
		run('ork-5', Status.FAILED),
		run('ork-4', Status.SUCCEEDED),
		run('ork-3', Status.RUNNING),
		run('ork-2', Status.SUCCEEDED)
	];
	expect(defaultCompareTarget(runs, 'ork-5')).toBe('ork-4');
	// The current run is never its own target, even when it is the newest success.
	expect(defaultCompareTarget(runs, 'ork-4')).toBe('ork-2');
	const noSuccess = [run('ork-9', Status.FAILED), run('ork-8', Status.CANCELED), run('ork-7', Status.FAILED)];
	expect(defaultCompareTarget(noSuccess, 'ork-9')).toBe('ork-8');
	expect(defaultCompareTarget([run('ork-1', Status.SUCCEEDED)], 'ork-1')).toBe('');
	expect(defaultCompareTarget([], 'ork-1')).toBe('');
	// It never re-sorts: the given order is the order of recency.
	const unsorted = [run('ork-a', Status.SUCCEEDED), run('ork-z', Status.SUCCEEDED)];
	expect(defaultCompareTarget(unsorted, 'ork-x')).toBe('ork-a');
});

// U2
test('does-not-apply renders -, missing evidence renders unavailable, a value renders itself', () => {
	expect(renderDiffValue('', true)).toBe('-');
	expect(renderDiffValue('', false)).toBe('unavailable');
	expect(renderDiffValue('gpu', true)).toBe('gpu');
});

// U3
test('the only parameter splits, trims and drops empties, and keeps unknown names for the server to refuse', () => {
	expect(parseOnly('image, pool,,')).toEqual(['image', 'pool']);
	expect(parseOnly(null)).toEqual([]);
	expect(parseOnly('')).toEqual([]);
	expect(parseOnly('images')).toEqual(['images']);
	expect(DIFF_FIELDS).toEqual([
		'definition', 'input', 'presence', 'status', 'attempt', 'pool', 'pin', 'orker',
		'exit_code', 'error', 'duration', 'image', 'platform_digest', 'cached'
	]);
});

// U4
test('reason sentences, the summary and the footnotes are the CLI golden strings', () => {
	const sentences: Record<string, string> = {
		definition_unavailable: 'the pinned definition is no longer stored',
		definition_digest_mismatch: 'the stored definition does not hash to the digest this run pinned',
		task_not_in_definition: 'the pinned definition has no task of this id',
		no_attempt_store: 'this engine has no task attempt store configured',
		no_outcome_attempt: 'the snapshot names no outcome attempt',
		attempt_record_missing:
			'no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one',
		timestamps_missing: 'the snapshot lacks a start or finish time',
		timestamps_inverted: 'the finish time precedes the start time'
	};
	for (const [token, sentence] of Object.entries(sentences)) {
		expect(reasonText(token)).toBe(sentence);
	}
	expect(reasonText('from_a_later_build')).toBe('from_a_later_build');

	// cmd/orkctl/testdata/run-diff.golden's last two lines, from the same shape.
	const resp = create(DiffRunsResponseSchema, {
		from: create(RunSchema, { id: 'ork-mon' }),
		to: create(RunSchema, { id: 'ork-tue' }),
		tasksCompared: 4,
		tasksIdentical: 1,
		evidenceUnavailable: 1,
		differences: [
			create(RunFieldDiffSchema, { taskId: 'extract', field: 'pool', fromValue: 'main', toValue: 'gpu', fromAvailable: true, toAvailable: true }),
			create(RunFieldDiffSchema, {
				taskId: 'load',
				field: 'platform_digest',
				toValue: 'sha256:c0ffee',
				fromAvailable: false,
				toAvailable: true,
				fromReason: 'attempt_record_missing'
			})
		]
	});
	expect(diffSummary(resp)).toBe('3 of 4 tasks differ; 1 identical; 1 field unavailable.');
	expect(unavailableLines(resp)).toEqual([
		'load platform_digest in ork-mon: attempt_record_missing (no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one)'
	]);

	const identical = create(DiffRunsResponseSchema, { tasksCompared: 4, tasksIdentical: 4 });
	expect(diffSummary(identical)).toBe('0 of 4 tasks differ; 4 identical.');
	expect(unavailableLines(identical)).toEqual([]);
	const two = create(DiffRunsResponseSchema, {
		from: create(RunSchema, { id: 'a' }),
		to: create(RunSchema, { id: 'b' }),
		tasksCompared: 1,
		evidenceUnavailable: 2,
		differences: [
			create(RunFieldDiffSchema, { taskId: 'w', field: 'image', fromReason: 'x_new', toReason: 'no_attempt_store' })
		]
	});
	expect(diffSummary(two)).toBe('1 of 1 tasks differ; 0 identical; 2 fields unavailable.');
	expect(unavailableLines(two)).toEqual([
		'w image in a: x_new',
		'w image in b: no_attempt_store (this engine has no task attempt store configured)'
	]);
});
