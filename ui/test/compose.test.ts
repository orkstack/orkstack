import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { EventSchema, EventType, RunSchema, TaskChildSchema, TaskRunSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	applyChildEvent,
	childLabel,
	childrenListHref,
	childRollup,
	childRunHref,
	rollupSummary
} from '../src/lib/compose';

test('childLabel is name@version', () => {
	expect(childLabel(create(TaskChildSchema, { orkflowName: 'release-block', orkflowVersion: '2.1.0' }))).toBe(
		'release-block@2.1.0'
	);
	expect(childLabel(undefined)).toBe('');
});

test('childRunHref and childrenListHref keep the namespace and escape the id', () => {
	expect(childRunHref('team-a', 'ork-1/2')).toBe('/runs/ork-1%2F2?ns=team-a');
	expect(childrenListHref('team-a', 'ork-1')).toBe(
		'/runs?origin=parent-run&originId=ork-1&ns=team-a'
	);
	expect(childrenListHref('', 'ork 1')).toContain('originId=ork+1');
	expect(childrenListHref('', 'ork-1')).toContain('ns=default');
});

const childRun = () =>
	create(RunSchema, {
		id: 'ork-child',
		taskRuns: [
			create(TaskRunSchema, { taskId: 'sign', status: Status.SUCCEEDED }),
			create(TaskRunSchema, { taskId: 'publish', status: Status.SUCCEEDED }),
			create(TaskRunSchema, { taskId: 'attest', status: Status.SUCCEEDED }),
			create(TaskRunSchema, {
				taskId: 'notarize',
				status: Status.RUNNING,
				child: create(TaskChildSchema, { orkflowName: 'notarize', orkflowVersion: '1.0', runId: 'ork-g' })
			})
		]
	});

test('childRollup counts a mixed task list by status', () => {
	const rollup = childRollup(childRun());
	expect(rollup.tasks.map((t) => t.id)).toEqual(['sign', 'publish', 'attest', 'notarize']);
	expect(rollup.tasks[3].child?.runId).toBe('ork-g');
	expect(rollupSummary(rollup)).toBe('4 tasks: 3 succeeded, 1 running');
	expect(rollupSummary(childRollup(undefined))).toBe('0 tasks');
});

test('applyChildEvent moves one row on a TASK_* event', () => {
	const rollup = childRollup(childRun());
	const next = applyChildEvent(
		rollup,
		create(EventSchema, { type: EventType.TASK_SUCCEEDED, taskId: 'notarize', status: Status.SUCCEEDED })
	);
	expect(next).not.toBe(rollup);
	expect(next.tasks[3].status).toBe(Status.SUCCEEDED);
	expect(rollup.tasks[3].status).toBe(Status.RUNNING);
	expect(rollupSummary(next)).toBe('4 tasks: 4 succeeded');
	// A status-less TASK_FAILED still fails its row.
	const failed = applyChildEvent(rollup, create(EventSchema, { type: EventType.TASK_FAILED, taskId: 'sign' }));
	expect(failed.tasks[0].status).toBe(Status.FAILED);
});

test('applyChildEvent ignores a TASK_LOG without a terminal status, items and run events', () => {
	const rollup = childRollup(childRun());
	expect(
		applyChildEvent(rollup, create(EventSchema, { type: EventType.TASK_LOG, taskId: 'sign', status: Status.RUNNING }))
	).toBe(rollup);
	const skipped = applyChildEvent(
		rollup,
		create(EventSchema, { type: EventType.TASK_LOG, taskId: 'sign', status: Status.SKIPPED })
	);
	expect(skipped.tasks[0].status).toBe(Status.SKIPPED);
	expect(
		applyChildEvent(rollup, create(EventSchema, { type: EventType.RUN_SUCCEEDED, status: Status.SUCCEEDED }))
	).toBe(rollup);
	expect(
		applyChildEvent(
			rollup,
			create(EventSchema, { type: EventType.TASK_SUCCEEDED, taskId: 'sign', item: 0, status: Status.SUCCEEDED })
		)
	).toBe(rollup);
	expect(
		applyChildEvent(rollup, create(EventSchema, { type: EventType.TASK_STARTED, taskId: 'ghost' }))
	).toBe(rollup);
});
