import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { RunTimeoutSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { runTimeoutFact, TASK_STOPPED_BY_RUN_TIMEOUT, taskStoppedByRunTimeout } from '../src/lib/runTimeout';

const NOW = Date.UTC(2026, 10, 12, 14, 48, 0);
const at = (offsetMs: number) => timestampFromMs(NOW + offsetMs);
const clock = (offsetMs: number) => new Date(NOW + offsetMs).toLocaleTimeString('en-GB', { hour12: false });
const MIN = 60_000;

test('no fact for a run whose Orkflow declares no timeout', () => {
	expect(runTimeoutFact(undefined, Status.RUNNING, undefined, at(0))).toBeNull();
	expect(runTimeoutFact(create(RunTimeoutSchema, { seconds: 0n }), Status.RUNNING, undefined, at(0))).toBeNull();
});

test('a run that has not started says when its clock starts, and one that never will says so', () => {
	const timeout = create(RunTimeoutSchema, { seconds: 21600n });
	expect(runTimeoutFact(timeout, Status.PENDING, undefined, at(0))).toEqual({ text: '6h · starts when the run starts', tip: '', state: 'waiting' });
	expect(runTimeoutFact(timeout, Status.CANCELED, undefined, at(0))?.text).toBe('6h · the run never started');
});

test('a running run counts down to its deadline, named in full in the tip', () => {
	const timeout = create(RunTimeoutSchema, { seconds: 21600n, deadline: at(72 * MIN) });
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(0))).toEqual({
		text: '6h · 1h12m left',
		tip: 'Deadline 2026-11-12T16:00:00.000Z',
		state: 'counting'
	});
	expect(runTimeoutFact(timeout, Status.PAUSED, undefined, at(72 * MIN - 42_300))?.text).toBe('6h · 43s left');
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(72 * MIN - 300))?.text).toBe('6h · 1s left');
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(72 * MIN))?.text).toBe('6h · deadline passed');
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(72 * MIN + 1000))?.text).toBe('6h · deadline passed');
});

test('the time left is spelled as the CLI spells it', () => {
	const timeout = create(RunTimeoutSchema, { seconds: 172800n, deadline: at(2 * 3600_000) });
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(0))?.text).toBe('48h · 2h left');
	expect(runTimeoutFact(timeout, Status.RUNNING, undefined, at(3600_000 + 30 * MIN))?.text).toBe('48h · 30m left');
});

test('a deadline no date holds is none, and a timed-out run with no clock still says it timed out', () => {
	const huge = { seconds: BigInt(Number.MAX_SAFE_INTEGER), nanos: 0 };
	const timeout = create(RunTimeoutSchema, { seconds: 60n });
	expect(runTimeoutFact(timeout, Status.RUNNING, huge, at(0))?.text).toBe('1m · starts when the run starts');
	expect(runTimeoutFact(create(RunTimeoutSchema, { seconds: 60n, timedOut: true }), Status.FAILED, undefined, at(0))?.text).toBe('1m · timed out, deadline unknown');
});

test('a run seen ending past its deadline before its snapshot says it timed out reads passed', () => {
	const timeout = create(RunTimeoutSchema, { seconds: 60n, deadline: at(-MIN) });
	expect(runTimeoutFact(timeout, Status.FAILED, undefined, at(0), at(-MIN))?.text).toBe(`1m · deadline ${clock(-MIN)}, passed`);
	expect(runTimeoutFact(timeout, Status.SUCCEEDED, undefined, at(0), at(-2 * MIN))?.text).toBe(`1m · deadline ${clock(-MIN)}`);
});

test('the live RUN_STARTED instant stands in for a deadline the page has not read yet', () => {
	const timeout = create(RunTimeoutSchema, { seconds: 3600n });
	expect(runTimeoutFact(timeout, Status.RUNNING, at(-10 * MIN), at(0))?.text).toBe('1h · 50m left');
});

test('a finished run names its deadline; a timed-out one says when, and when its handlers stop', () => {
	expect(runTimeoutFact(create(RunTimeoutSchema, { seconds: 21600n, deadline: at(60 * MIN) }), Status.SUCCEEDED, undefined, at(0))?.text).toBe(
		`6h · deadline ${clock(60 * MIN)}`
	);
	const failing = create(RunTimeoutSchema, { seconds: 21600n, deadline: at(-MIN), hardEnd: at(9 * MIN), timedOut: true });
	expect(runTimeoutFact(failing, Status.RUNNING, undefined, at(0))).toEqual({
		text: `6h · timed out at ${clock(-MIN)} · failure handlers stop at ${clock(9 * MIN)}`,
		tip: 'Deadline 2026-11-12T14:47:00.000Z',
		state: 'timed-out'
	});
	const finished = create(RunTimeoutSchema, { seconds: 21600n, deadline: at(-60 * MIN), hardEnd: at(-50 * MIN), timedOut: true });
	expect(runTimeoutFact(finished, Status.FAILED, undefined, at(0))?.text).toBe(`6h · timed out at ${clock(-60 * MIN)}`);
	const stopped = create(RunTimeoutSchema, { ...finished, hardEndReached: true });
	expect(runTimeoutFact(stopped, Status.FAILED, undefined, at(0))?.text).toBe(
		`6h · timed out at ${clock(-60 * MIN)} · failure handlers stopped at ${clock(-50 * MIN)}`
	);
});

test('a row the run timeout stopped is said in words; any other failure is not', () => {
	expect(taskStoppedByRunTimeout({ status: Status.FAILED, error: { code: TASK_STOPPED_BY_RUN_TIMEOUT } })).toBe(true);
	expect(taskStoppedByRunTimeout({ status: Status.FAILED, error: { code: 'CONTAINER_TIMEOUT' } })).toBe(false);
	expect(taskStoppedByRunTimeout({ status: Status.CANCELED, error: { code: TASK_STOPPED_BY_RUN_TIMEOUT } })).toBe(false);
	expect(taskStoppedByRunTimeout({ status: Status.FAILED })).toBe(false);
});
