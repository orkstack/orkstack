import { expect, test } from 'bun:test';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { cancelControl } from '../src/lib/cancel';

// The run page's Cancel run, as the pause controls beside it: offered only to
// a caller the server would let cancel (run.cancel at the run's Orkflow), and
// only while the run can still be canceled. The server decides every request;
// this only keeps a control off the page that would be refused.

test('a running run is offered Cancel run to a caller who may cancel it', () => {
	expect(cancelControl(Status.RUNNING, { allowed: true, requested: false })).toEqual({ shown: true, disabled: '' });
});

test('a caller without run.cancel is not offered Cancel run', () => {
	for (const status of [Status.PENDING, Status.RUNNING, Status.PAUSED]) {
		expect(cancelControl(status, { allowed: false, requested: false })).toEqual({ shown: false, disabled: '' });
		expect(cancelControl(status, { allowed: false, requested: true })).toEqual({ shown: false, disabled: '' });
	}
});

test('a finished run offers no Cancel run, whoever asks', () => {
	for (const status of [Status.SUCCEEDED, Status.FAILED, Status.CANCELED]) {
		expect(cancelControl(status, { allowed: true, requested: false }).shown).toBe(false);
	}
});

test('a cancel already requested shows, disabled, and says so', () => {
	expect(cancelControl(Status.RUNNING, { allowed: true, requested: true })).toEqual({
		shown: true,
		disabled: 'Cancellation requested'
	});
});
