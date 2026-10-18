import { expect, test } from 'bun:test';
import { plainFailure } from '../src/lib/failureText';

// The text a run's RUN_FAILED and a task's error carry, as the engine and the
// Orker recorded them (proof/ui-finish-2-feature, run-sheet and each-fan-out).
const WRAPPED =
	'activity error (type: ExecuteTask, scheduledEventID: 86, startedEventID: 87, identity: sauroposeidon-50427179): ' +
	'CONTAINER_EXIT_NONZERO: task "charge": the container exited 3 (type: ActivityFailed, retryable: true)';

test('a wrapped task failure leads with its reason code and its sentence, and names no mechanism', () => {
	const plain = plainFailure(WRAPPED);
	expect(plain.code).toBe('CONTAINER_EXIT_NONZERO');
	expect(plain.text).toBe('task "charge": the container exited 3');
	for (const word of ['activity error', 'scheduledEventID', 'startedEventID', 'identity', 'ExecuteTask', 'retryable']) {
		expect(plain.text).not.toContain(word);
	}
});

test('the reason code is split off the sentence, so a panel showing the code does not repeat it', () => {
	expect(plainFailure(WRAPPED)).toEqual({
		code: 'CONTAINER_EXIT_NONZERO',
		text: 'task "charge": the container exited 3'
	});
});

test('a child run failure drops its wrapper too', () => {
	const plain = plainFailure(
		'child workflow execution error (type: RunWorkflow, workflowID: ork-1-2, runID: abc, initiatedEventID: 5, startedEventID: 6): ' +
			'CHILD_RUN_FAILED: child run ork-1-2 (failing-block@0.1.0) failed (type: CHILD_RUN_FAILED, retryable: false)'
	);
	expect(plain).toEqual({ code: 'CHILD_RUN_FAILED', text: 'child run ork-1-2 (failing-block@0.1.0) failed' });
});

test("a fan-out item's failure keeps its item sentence, loses the wrapper inside it, and leads with the item's reason", () => {
	const plain = plainFailure(
		'item 3 of 6 failed: activity error (type: ExecuteTask, scheduledEventID: 66, startedEventID: 74, identity: sauroposeidon-475e1813): ' +
			'CONTAINER_EXIT_NONZERO: task "convert": the container exited 1 (type: ActivityFailed, retryable: true)'
	);
	expect(plain).toEqual({ code: 'CONTAINER_EXIT_NONZERO', text: 'item 3 of 6 failed: task "convert": the container exited 1' });
});

test('a plain sentence is left alone', () => {
	expect(plainFailure('item 3 of 6 failed: the container exited 1')).toEqual({
		code: undefined,
		text: 'item 3 of 6 failed: the container exited 1'
	});
	expect(plainFailure('run canceled by operator: run-sheet: drawn, not run')).toEqual({
		code: undefined,
		text: 'run canceled by operator: run-sheet: drawn, not run'
	});
});

test('a cause chain keeps every sentence and loses only the decoration', () => {
	expect(
		plainFailure('IMAGE_PULL_FAILED: pulling x (type: InfrastructureError, retryable: true): dial tcp: refused')
	).toEqual({ code: 'IMAGE_PULL_FAILED', text: 'pulling x: dial tcp: refused' });
});

test('an empty message stays empty', () => {
	expect(plainFailure('')).toEqual({ code: undefined, text: '' });
});
