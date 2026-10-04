import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { EventSchema, EventType, TaskAttemptSchema, TaskRunSchema, TaskEachSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { EachSpecSchema } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { PlanOutcome, PlanRunResponseSchema, PlanTaskSchema } from '../src/lib/gen/orkstack/v1/service_pb';
import {
	applyItemEvent,
	eachPlanDetail,
	eachProgress,
	eachSummary,
	itemAriaLabel,
	itemCli,
	itemFromUrl,
	itemLabel,
	itemsFromTaskRun,
	mergeItems,
	retriedItemsLabel,
	type ItemView
} from '../src/lib/each';
import { attemptsKey, groupAttemptsByTask, runRetryCount } from '../src/lib/attempts';
import { planDetail } from '../src/lib/plan';

function item(index: number, status: Status, attempt = 1): ItemView {
	return { index, status, attempt, message: '' };
}

function itemEvent(index: number | undefined, status: Status, message = ''): ReturnType<typeof create<typeof EventSchema>> {
	return create(EventSchema, { runId: 'ork-1', taskId: 'convert', type: EventType.TASK_LOG, status, message, item: index });
}

test('itemLabel spells one item convert[97]', () => {
	expect(itemLabel('convert', 97)).toBe('convert[97]');
	expect(itemLabel('convert', 0)).toBe('convert[0]');
});

test('eachProgress tallies a mixed item list', () => {
	const items = [
		item(0, Status.SUCCEEDED),
		item(1, Status.SUCCEEDED, 2),
		item(2, Status.FAILED, 3),
		item(3, Status.CANCELED, 0),
		item(4, Status.RUNNING),
		item(5, Status.PENDING, 0)
	];
	expect(eachProgress(items)).toEqual({
		count: 6,
		settled: 4,
		succeeded: 2,
		failed: 1,
		canceled: 1,
		running: 1,
		pending: 1,
		retried: 2
	});
	expect(retriedItemsLabel(items)).toBe('2 items retried');
	expect(retriedItemsLabel([item(0, Status.SUCCEEDED, 2)])).toBe('1 item retried');
	expect(retriedItemsLabel([item(0, Status.SUCCEEDED)])).toBeNull();
});

test('eachSummary says settled, empty, or what the fan-out will do', () => {
	const items = [item(0, Status.SUCCEEDED), item(1, Status.FAILED, 2), item(2, Status.RUNNING)];
	expect(eachSummary(true, items)).toBe('2 of 3 settled — 1 succeeded, 1 failed · 1 retried');
	expect(eachSummary(true, [])).toBe('fan-out over 0 items');
	const spec = create(EachSpecSchema, { over: '{{ .tasks.list.output.parts }}', as: 'part', maxItems: 50 });
	expect(eachSummary(false, [], spec)).toBe('Will fan out over {{ .tasks.list.output.parts }} (at most 50 items)');
});

test('an item TASK_LOG moves one cell and never the task', () => {
	const items = [item(0, Status.RUNNING), item(1, Status.RUNNING)];
	const next = applyItemEvent(items, itemEvent(1, Status.SUCCEEDED, 'item 1 succeeded'));
	expect(next[1].status).toBe(Status.SUCCEEDED);
	expect(next[1].message).toBe('item 1 succeeded');
	expect(next[0]).toBe(items[0]);
	// The input is not mutated: the page swaps the array, cell by cell.
	expect(items[1].status).toBe(Status.RUNNING);
});

test('an event without an item is ignored', () => {
	const items = [item(0, Status.RUNNING)];
	expect(applyItemEvent(items, itemEvent(undefined, Status.CANCELED))).toBe(items);
});

test('an item event that outruns the expansion snapshot grows the grid', () => {
	const next = applyItemEvent([], itemEvent(2, Status.SUCCEEDED));
	expect(next.map((cell) => [cell.index, cell.status])).toEqual([
		[0, Status.PENDING],
		[1, Status.PENDING],
		[2, Status.SUCCEEDED]
	]);
});

test('a snapshot never moves an item an event already settled back', () => {
	const held = [item(0, Status.SUCCEEDED), item(1, Status.RUNNING)];
	const snapshot = itemsFromTaskRun(
		create(TaskRunSchema, {
			taskId: 'convert',
			each: create(TaskEachSchema, { expanded: true, count: 3 }),
			items: [
				create(TaskRunSchema, { taskId: 'convert', item: 0, status: Status.RUNNING }),
				create(TaskRunSchema, { taskId: 'convert', item: 1, status: Status.FAILED, attempt: 2 }),
				create(TaskRunSchema, { taskId: 'convert', item: 2, status: Status.PENDING })
			]
		})
	);
	const merged = mergeItems(held, snapshot);
	expect(merged.map((cell) => cell.status)).toEqual([Status.SUCCEEDED, Status.FAILED, Status.PENDING]);
	expect(merged[1].attempt).toBe(2);
});

test('a never-started item says so in its accessible name', () => {
	expect(itemAriaLabel(item(97, Status.SUCCEEDED, 2))).toBe('item 97, SUCCEEDED, attempt 2');
	expect(itemAriaLabel(item(4, Status.CANCELED, 0))).toBe('item 4, CANCELED (never started), attempt 0');
	const ran: ItemView = {
		...item(5, Status.CANCELED),
		run: create(TaskRunSchema, { taskId: 'convert', item: 5, scheduledAt: timestampFromMs(1) })
	};
	expect(itemAriaLabel(ran)).toBe('item 5, CANCELED, attempt 1');
});

test('itemFromUrl reads a well-formed ?item= only', () => {
	expect(itemFromUrl(new URL('http://x/runs/r?task=convert&item=97'))).toBe(97);
	expect(itemFromUrl(new URL('http://x/runs/r?item=0'))).toBe(0);
	expect(itemFromUrl(new URL('http://x/runs/r'))).toBeUndefined();
	expect(itemFromUrl(new URL('http://x/runs/r?item=-1'))).toBeUndefined();
	expect(itemFromUrl(new URL('http://x/runs/r?item=1e3'))).toBeUndefined();
	expect(itemFromUrl(new URL('http://x/runs/r?item='))).toBeUndefined();
});

test('itemCli quotes every argument: a quote in a task id cannot become shell syntax', () => {
	expect(itemCli({ namespace: '', runId: 'ork-1', taskId: 'convert', item: 97, command: 'logs' })).toBe(
		"orkctl --namespace 'default' 'run' 'logs' 'ork-1' '--task' 'convert' '--item' '97'"
	);
	const hostile = itemCli({ namespace: 'data', runId: 'ork-1', taskId: "x'; rm -rf / #", item: 1, command: 'attempts' });
	expect(hostile).toContain(`'x'"'"'; rm -rf / #'`);
	expect(hostile).not.toContain("'x'; rm");
});

test('item attempts are their own timelines, so first tries are not retries', () => {
	const rows = [0, 1, 2].map((index) =>
		create(TaskAttemptSchema, { runId: 'ork-1', taskId: 'convert', item: index, attempt: 1 })
	);
	rows.push(create(TaskAttemptSchema, { runId: 'ork-1', taskId: 'convert', item: 2, attempt: 2 }));
	const grouped = groupAttemptsByTask(rows);
	expect([...grouped.keys()]).toEqual(['convert[0]', 'convert[1]', 'convert[2]']);
	expect(attemptsKey('convert', 2)).toBe('convert[2]');
	expect(attemptsKey('convert')).toBe('convert');
	expect(runRetryCount(rows)).toBe(1);
});

test('the plan states a fan-out the way the CLI does', () => {
	const spec = create(EachSpecSchema, { over: 'input.files', as: 'item', maxItems: 50, maxParallel: 2 });
	const counted = create(PlanTaskSchema, { taskId: 'convert', outcome: PlanOutcome.RUNS, each: spec, eachCount: 3 });
	expect(eachPlanDetail(counted)).toBe('each over input.files: 3 items (≤ 50, 2 at a time)');
	expect(planDetail(counted, create(PlanRunResponseSchema, { tasks: [counted] }))).toBe('each over input.files: 3 items (≤ 50, 2 at a time)');
	const later = create(PlanTaskSchema, {
		taskId: 'convert',
		outcome: PlanOutcome.RUNS,
		each: create(EachSpecSchema, { over: '{{ .tasks.list.output.parts }}', as: 'part', maxItems: 500 })
	});
	expect(eachPlanDetail(later)).toBe('each over {{ .tasks.list.output.parts }} (≤ 500 items; counted when the run reaches it)');
});
