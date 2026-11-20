import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { RunSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { endedRuns, noLongerListed, runEnded, runGoing, sidebarCounts } from '../src/lib/sidebarCounts';

// The sidebar's "N running, M paused" comes from GetStats, polled every 5 s
// and served from a cache of the same age, so a run the page watched end can
// stay counted for seconds. A run seen ended is not counted, whatever an older
// answer says: an ended run never runs again.

const run = (id: string, status: Status) => create(RunSchema, { id, status });
const ended = (entries: [string, number][]) => new Map(entries);

test('every run the answer lists counts, the paused ones as paused', () => {
	const listed = [run('a', Status.RUNNING), run('b', Status.PAUSED), run('c', Status.PENDING)];
	expect(sidebarCounts(listed, ended([]))).toEqual({ running: 3, paused: 1 });
});

test('a paused run the page saw end is counted neither running nor paused', () => {
	const listed = [run('a', Status.RUNNING), run('b', Status.PAUSED)];
	expect(sidebarCounts(listed, ended([['b', 1_000]]))).toEqual({ running: 1, paused: 0 });
});

test('a running run the page saw end is not counted', () => {
	expect(sidebarCounts([run('a', Status.RUNNING)], ended([['a', 1_000]]))).toEqual({ running: 0, paused: 0 });
});

test('an ended run is forgotten once an answer measured after its end no longer lists it', () => {
	const seen = ended([
		['a', 1_000],
		['b', 1_000]
	]);
	// Measured after both ended: b is still listed (the cluster is behind), a is not.
	expect(noLongerListed([run('b', Status.PAUSED), run('c', Status.RUNNING)], seen, 2_000)).toEqual(['a']);
	expect(noLongerListed([], seen, 2_000)).toEqual(['a', 'b']);
	// Measured before they ended: an answer that does not list them yet says nothing.
	expect(noLongerListed([], seen, 500)).toEqual([]);
});

test('a run seen ended and then going again (resumed, same id) is counted again', () => {
	runEnded('ork-resumed', 1_000);
	expect(sidebarCounts([run('ork-resumed', Status.RUNNING)], endedRuns)).toEqual({ running: 0, paused: 0 });
	runGoing('ork-resumed');
	expect(sidebarCounts([run('ork-resumed', Status.RUNNING)], endedRuns)).toEqual({ running: 1, paused: 0 });
	runEnded('ork-resumed', 2_000);
	expect(endedRuns.get('ork-resumed')).toBe(2_000);
	runEnded('ork-resumed', 3_000); // seen ended again: the first sighting stands
	expect(endedRuns.get('ork-resumed')).toBe(2_000);
	endedRuns.delete('ork-resumed');
});
