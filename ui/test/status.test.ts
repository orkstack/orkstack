import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { TimestampSchema } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { isTerminal, taskDispatched } from '../src/lib/status';

// A real Timestamp message, not a shaped literal: taskDispatched's whole point
// is reading the field the wire actually carries.
const AT = create(TimestampSchema, { seconds: 1_700_000_000n, nanos: 0 });

test('a task that never reached an Orker is not dispatched, whatever its status says', () => {
	// The two the run page used to draw a misleading badge for.
	expect(taskDispatched(Status.PENDING, undefined)).toBe(false);
	expect(taskDispatched(Status.CANCELED, undefined)).toBe(false);

	// CANCELED is ALSO the status of a task that ran and was killed.
	// scheduled_at is the only thing separating the two, which is why the
	// engine stamps it exclusively at real dispatch.
	expect(taskDispatched(Status.CANCELED, AT)).toBe(true);

	// A skip is a DAG decision, not an execution: not dispatched, and the card
	// keeps its own pill to say so.
	expect(taskDispatched(Status.SKIPPED, undefined)).toBe(false);
	expect(taskDispatched(Status.SKIPPED, AT)).toBe(false);

	// Nothing known yet is not evidence of a start.
	expect(taskDispatched(undefined, undefined)).toBe(false);
	expect(taskDispatched(Status.UNSPECIFIED, undefined)).toBe(false);

	// Everything that can only be reached by running.
	expect(taskDispatched(Status.RUNNING, undefined)).toBe(true);
	expect(taskDispatched(Status.SUCCEEDED, AT)).toBe(true);
	expect(taskDispatched(Status.FAILED, AT)).toBe(true);
});

test('SKIPPED is terminal: nothing moves a task the DAG decided not to run', () => {
	expect(isTerminal(Status.SKIPPED)).toBe(true);
	expect(isTerminal(Status.SUCCEEDED)).toBe(true);
	expect(isTerminal(Status.FAILED)).toBe(true);
	expect(isTerminal(Status.CANCELED)).toBe(true);
	expect(isTerminal(Status.RUNNING)).toBe(false);
	expect(isTerminal(Status.PENDING)).toBe(false);
	expect(isTerminal(undefined)).toBe(false);
});

// U9 (task-timing-breakdown): the dispatch stamp is scheduled_at. A task
// canceled while it ran has scheduled_at and NO started_at — no Orker reported
// a start — and it did run.
test('taskDispatched keys on scheduledAt, not on the Orker-reported start', () => {
	expect(taskDispatched(Status.CANCELED, AT)).toBe(true);
	expect(taskDispatched(Status.CANCELED, undefined)).toBe(false);
});

test('AWAITING has its own label and tone, is not terminal, and counts as dispatched', async () => {
	const { STATUS_LABEL, STATUS_CLASSES, EVENT_TYPE_LABEL, isTerminal, taskDispatched } = await import(
		'../src/lib/status'
	);
	const { Status } = await import('../src/lib/gen/orkstack/v1/common_pb');
	const { EventType } = await import('../src/lib/gen/orkstack/v1/run_pb');
	expect(STATUS_LABEL[Status.AWAITING]).toBe('AWAITING');
	expect(STATUS_CLASSES[Status.AWAITING]).toBe('border-awaiting-border bg-awaiting-bg text-awaiting-fg');
	expect(EVENT_TYPE_LABEL[EventType.TASK_AWAITING]).toBe('TASK_AWAITING');
	expect(EVENT_TYPE_LABEL[EventType.TASK_RESOLVED]).toBe('TASK_RESOLVED');
	expect(isTerminal(Status.AWAITING)).toBe(false);
	expect(taskDispatched(Status.AWAITING, undefined)).toBe(true);
});
