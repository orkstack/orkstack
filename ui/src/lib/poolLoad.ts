import type { Orker } from './gen/orkstack/v1/activity_pb';
import type { Pool } from './gen/orkstack/v1/namespace_pb';
import { operatorCordoned, profileState } from './orkers';

// A pool card's load and state on Admin → Pools. Nothing unavailable is ever
// drawn as zero: a pool the cluster could not answer for has NO number at
// all, only the reason. Whether an Orker's cordon keeps its pool live mirrors
// the engine (internal/engine/pool_census.go, keepsPoolLive): a store outage,
// stopping and an unavailable profile do, so runs sent there are accepted
// and wait; an operator's cordon, a drifted profile and a profile cause this
// release cannot read do not, whatever else holds the Orker. The pool's counts
// follow that rule, not a row's one state (orkerState), which names an Orker's
// most pressing cause for its own row.

/** What a pool's live Orkers reserved of what they declared. */
export interface PoolLoad {
	orkers: number;
	cordoned: number;
	/** Orkers an operator cordoned. */
	operatorCordons: number;
	/** Orkers cordoned because object storage does not answer them. */
	storeOutages: number;
	/** Orkers stopping: they take no new work and finish what they run. */
	stopping: number;
	/** Orkers an unavailable profile alone holds: their pool stays live. */
	profileUnavailable: number;
	/** Orkers no operator cordoned that a drifted or unreadable profile takes out of their pool. */
	profileOut: number;
	tasks: number;
	pins: number;
	cpu: { reserved: number; capacity: number };
	memory: { reserved: number; capacity: number };
	/** gpu units; capacity 0 when no Orker of the pool offers a gpu profile. */
	gpu: { reserved: number; capacity: number };
}

export function loadOf(pool: string, orkers: Orker[]): PoolLoad {
	const mine = orkers.filter((o) => o.pool === pool && !o.unavailableReason);
	const sum = (f: (o: Orker) => number) => mine.reduce((a, o) => a + f(o), 0);
	const profile = (o: Orker) => (operatorCordoned(o) ? null : profileState(o));
	return {
		orkers: mine.length,
		cordoned: mine.filter((o) => o.cordoned).length,
		operatorCordons: mine.filter(operatorCordoned).length,
		storeOutages: mine.filter((o) => o.storeOutage).length,
		stopping: mine.filter((o) => o.stopping).length,
		profileUnavailable: mine.filter((o) => !o.storeOutage && !o.stopping && profile(o) === 'profile-unavailable').length,
		profileOut: mine.filter((o) => profile(o) === 'profile-drift' || profile(o) === 'profile-out').length,
		tasks: sum((o) => o.runningTasks),
		pins: sum((o) => o.openPins),
		cpu: { reserved: sum((o) => o.reserved?.cpu ?? 0), capacity: sum((o) => o.capacity?.cpu ?? 0) },
		memory: { reserved: sum((o) => Number(o.reserved?.memoryBytes ?? 0n)), capacity: sum((o) => Number(o.capacity?.memoryBytes ?? 0n)) },
		gpu: { reserved: sum((o) => Number(o.reserved?.gpu ?? 0n)), capacity: sum((o) => Number(o.capacity?.gpu ?? 0n)) }
	};
}

const at = (d: { reserved: number; capacity: number }) => d.capacity > 0 && d.reserved >= d.capacity;

/** Every Orker has reserved all it declared, in a dimension their capacity bounds. */
export function isFull(load: PoolLoad): boolean {
	return fullDimension(load) !== null;
}

/** The first dimension the pool's Orkers have reserved in full: cpu, then memory, then gpu units. */
export function fullDimension(load: PoolLoad): 'cpu' | 'memory' | 'gpu' | null {
	if (load.orkers === 0) return null;
	if (at(load.cpu)) return 'cpu';
	if (at(load.memory)) return 'memory';
	if (at(load.gpu)) return 'gpu';
	return null;
}

export type PoolTone = 'ok' | 'wait' | 'fail' | 'plain';

/**
 * Ready, Full or Cordoned, in words; colour only beside the word. A pool whose
 * every Orker is held only by an unavailable profile is never `Cordoned`: it
 * stays live, and its runs wait for the profile to come back.
 */
export function stateOf(pool: Pool, load: PoolLoad | null): { word: string; tone: PoolTone } {
	if (pool.unavailableReason) return { word: 'Unknown', tone: 'plain' };
	if (load && load.orkers > 0 && load.storeOutages === load.orkers) return { word: 'Object storage unreachable', tone: 'fail' };
	if (pool.liveOrkerCount === 0) return { word: 'No Orkers', tone: 'fail' };
	if (load && load.storeOutages > 0) return { word: `${load.storeOutages} cannot reach object storage`, tone: 'fail' };
	if (load && load.orkers > 0 && load.stopping === load.orkers && load.profileOut === 0) return { word: 'Stopping', tone: 'wait' };
	if (load && load.orkers > 0 && load.profileUnavailable === load.orkers) return { word: 'Profile unavailable', tone: 'wait' };
	if (load && load.orkers > 0 && load.cordoned === load.orkers && load.profileUnavailable === 0) return { word: 'Cordoned', tone: 'wait' };
	if (load && isFull(load)) return { word: 'Full', tone: 'wait' };
	if (load && load.operatorCordons > 0) return { word: `${load.operatorCordons} cordoned`, tone: 'wait' };
	if (load && load.profileOut > 0) return { word: `${load.profileOut} profile out of service`, tone: 'wait' };
	if (load && load.stopping > 0) return { word: `${load.stopping} stopping`, tone: 'wait' };
	if (load && load.profileUnavailable > 0) return { word: `${load.profileUnavailable} profile unavailable`, tone: 'wait' };
	return { word: 'Ready', tone: 'ok' };
}
