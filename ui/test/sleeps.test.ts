import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { TaskSleepSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { sleepLengthWords, sleepRemaining, sleepStateLabel, sleepWakeAt, wakesWords } from '../src/lib/sleeps';

test('sleepRemaining counts down, says waking once the time has passed, and has no wake-up as -', () => {
	const now = timestampFromMs(Date.UTC(2026, 0, 2, 3, 4, 5));
	const wake = timestampFromMs(Date.UTC(2026, 0, 2, 3, 4, 5) + (1 * 3600 + 30 * 60) * 1000);
	expect(sleepRemaining(wake, now)).toBe('in 1h30m');
	expect(sleepRemaining(now, wake)).toBe('waking');
	expect(sleepRemaining(undefined, now)).toBe('-');
	expect(sleepRemaining(timestampFromMs(Date.UTC(2026, 0, 2, 3, 4, 50)), now)).toBe('in 45s');
});

test('the event-derived wake-up equals the snapshot wake-up for the same instant', () => {
	const sleepingAt = timestampFromMs(Date.UTC(2026, 8, 24, 14, 2, 0, 250));
	const snapshot = create(TaskSleepSchema, {
		seconds: 900,
		wakeAt: timestampFromMs(Date.UTC(2026, 8, 24, 14, 2, 0, 250) + 900 * 1000)
	});
	const fromEvent = sleepWakeAt({}, 900, sleepingAt);
	const fromSnapshot = sleepWakeAt({ sleep: snapshot }, 900, undefined);
	expect(fromEvent).toEqual({ seconds: snapshot.wakeAt!.seconds, nanos: snapshot.wakeAt!.nanos });
	expect(fromSnapshot).toEqual(fromEvent);
	expect(sleepWakeAt({}, 900, undefined)).toBeUndefined();
	expect(sleepWakeAt({}, 0, sleepingAt)).toBeUndefined();
});

test('sleepStateLabel names every state', () => {
	expect(sleepStateLabel({ status: Status.SLEEPING, seconds: 900 })).toBe('Sleeping for 15m');
	expect(sleepStateLabel({ status: Status.SUCCEEDED, seconds: 5400 })).toBe('Woke after 1h30m');
	expect(sleepStateLabel({ status: Status.CANCELED, seconds: 60, opened: true })).toBe('Canceled while sleeping');
	expect(sleepStateLabel({ status: Status.CANCELED, seconds: 60 })).toBe('Canceled before it started sleeping');
	expect(sleepStateLabel({ status: Status.SKIPPED, seconds: 60 })).toBe('Skipped: it never started sleeping');
	expect(sleepStateLabel({ status: Status.PENDING, seconds: 90 })).toBe('Will sleep 1m30s once its needs are complete');
	expect(sleepStateLabel({ status: Status.FAILED, seconds: 60 })).toBe('Failed');
});

test('the panel names a sleep length its state does not: a failed, canceled or skipped sleep', () => {
	// The state line already says it while sleeping, once woken and before it starts.
	expect(sleepLengthWords({ status: Status.SLEEPING, seconds: 900 })).toBe('');
	expect(sleepLengthWords({ status: Status.SUCCEEDED, seconds: 900 })).toBe('');
	expect(sleepLengthWords({ status: Status.PENDING, seconds: 900 })).toBe('');
	for (const status of [Status.FAILED, Status.CANCELED, Status.SKIPPED]) {
		expect(sleepLengthWords({ status, seconds: 900 })).toBe('15m');
		expect(sleepLengthWords({ status, seconds: 0 })).toBe('');
	}
});

test('a wake-up reads on the 24-hour clock', () => {
	const at = new Date(2026, 0, 2, 14, 5, 0);
	expect(wakesWords(timestampFromMs(at.getTime()))).toBe('wakes 14:05:00');
	expect(wakesWords(undefined)).toBe('');
});
