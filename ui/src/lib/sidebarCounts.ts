import { SvelteMap } from 'svelte/reactivity';
import { Status } from './gen/orkstack/v1/common_pb';
import type { Run } from './gen/orkstack/v1/run_pb';

// The sidebar's "N running, M paused" is GetStats' running list, polled every
// 5 s and answered from a cache of the same age, so a run a page watched end
// could stay counted for up to twice that. A page that sees its run end says
// so here, and the count leaves the run out at once: an ended run never runs
// again, whatever an older answer says.

/** The runs a page saw end, with when (ms since the epoch) it saw it. */
export const endedRuns = new SvelteMap<string, number>();

/** runEnded records that a page saw run `id` end. */
export function runEnded(id: string, at: number = Date.now()): void {
	if (!endedRuns.has(id)) endedRuns.set(id, at);
}

/** runGoing records that a page saw run `id` going: a resumed run keeps its id. */
export function runGoing(id: string): void {
	if (endedRuns.has(id)) endedRuns.delete(id);
}

/** sidebarCounts counts the answer's runs, leaving out the ones seen ended. */
export function sidebarCounts(listed: Run[], ended: ReadonlyMap<string, number>): { running: number; paused: number } {
	const going = listed.filter((r) => !ended.has(r.id));
	return { running: going.length, paused: going.filter((r) => r.status === Status.PAUSED).length };
}

/**
 * noLongerListed is the ended runs an answer measured at `computedAt` (ms)
 * makes it safe to forget: measured after the page saw each end, and not
 * listing it. An answer measured before an end says nothing of it.
 */
export function noLongerListed(listed: Run[], ended: ReadonlyMap<string, number>, computedAt: number): string[] {
	const ids = new Set(listed.map((r) => r.id));
	return [...ended].filter(([id, at]) => computedAt > at && !ids.has(id)).map(([id]) => id);
}
