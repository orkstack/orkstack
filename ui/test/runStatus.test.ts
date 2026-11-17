import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { ErrorSchema, Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { isLive, isTerminal } from '../src/lib/runStatus';
import { runFailing } from '../src/lib/join';

// Every generated Status value is live or terminal, never both and never
// neither, so a value added to the proto is classified before any page reads
// it; UNSPECIFIED, the absence of a status, is neither.
test('every Status value is classified', () => {
	const values = Object.values(Status).filter((v): v is Status => typeof v === 'number');
	expect(values.length).toBeGreaterThanOrEqual(10);
	for (const status of values) {
		if (status === Status.UNSPECIFIED) {
			expect([isLive(status), isTerminal(status)]).toEqual([false, false]);
			continue;
		}
		expect(isLive(status) !== isTerminal(status)).toBe(true);
	}
	expect(isLive(undefined)).toBe(false);
	expect(isTerminal(undefined)).toBe(false);
});

test('a paused run is live', () => {
	expect(isLive(Status.PAUSED)).toBe(true);
	expect(isTerminal(Status.PAUSED)).toBe(false);
});

// A paused run that is failing still shows the Failing banner: it is live,
// and its handlers wait for the unpause.
test('runFailing holds for a failing paused run', () => {
	const error = create(ErrorSchema, { code: 'RUN_FAILED', message: 'deploy exploded' });
	expect(runFailing(Status.PAUSED, error, '')).toBe(true);
	// A live run with no error and no failing note is not failing, whatever
	// its status.
	for (const status of [Status.PENDING, Status.RUNNING, Status.PAUSED]) {
		expect(runFailing(status, undefined, '')).toBe(false);
	}
});
