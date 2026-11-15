import type { Timestamp } from '@bufbuild/protobuf/wkt';
import type { Orker } from './gen/orkstack/v1/activity_pb';
import { shellQuote } from './runDetail';

// The Orker inventory's rendering rules, shared by Admin → Pools and its
// tests (docs/DEPLOY.md "Removing an Orker safely: cordon and drain"). They mirror `orkctl orker list`
// (cmd/orkctl/orker.go): an Orker that could not be asked is unreachable,
// never idle, and none of its numbers are drawn.

export type OrkerState = 'ready' | 'cordoned' | 'store-outage' | 'stopping' | 'unreachable';

/**
 * An operator's cordon holds: it always carries a reason. An Orker of a release
 * with no store check and no stop grace only ever reports that cause, so a row
 * with neither reads as it always has.
 */
export function operatorCordoned(orker: Orker): boolean {
	if (!orker.cordoned) return false;
	return orker.cordonReason !== '' || (!orker.storeOutage && !orker.stopping);
}

/**
 * The row's one state: a stopping Orker never takes work again, so it wins;
 * then an operator's cordon, and a store outage beside it has its own stamp.
 */
export function orkerState(orker: Orker): OrkerState {
	if (orker.unavailableReason) return 'unreachable';
	if (orker.stopping) return 'stopping';
	if (operatorCordoned(orker)) return 'cordoned';
	return orker.storeOutage ? 'store-outage' : 'ready';
}

/** The state in words, for the row's stamp. */
export function orkerStateLabel(state: OrkerState): string {
	return state === 'store-outage' ? STORE_OUTAGE_LABEL : state;
}

/** What a store outage's stamp says, beside an operator's `cordoned`. */
export const STORE_OUTAGE_LABEL = 'object storage unreachable';

/** PINS: open/max for a pin host, `-` for an Orker that hosts none or was not measured. */
export function orkerPins(orker: Orker): string {
	if (orker.unavailableReason || orker.maxConcurrentPins === 0) return '-';
	return `${orker.openPins}/${orker.maxConcurrentPins}`;
}

/** TASKS: the running count, or `-` when nothing was measured. */
export function orkerTasks(orker: Orker): string {
	return orker.unavailableReason ? '-' : String(orker.runningTasks);
}

function plural(n: number, one: string, many: string): string {
	return n === 1 ? `1 ${one}` : `${n} ${many}`;
}

/** "is running 2 tasks and hosts 1 pin" — the cordon dialog's live counts. */
export function orkerLoad(orker: Orker): string {
	return `is running ${plural(orker.runningTasks, 'task', 'tasks')} and hosts ${plural(orker.openPins, 'pin', 'pins')}`;
}

/**
 * The CLI equivalent of the row's state: the cordon that produced a cordoned
 * row, with its reason shell-quoted so a quote in it cannot become shell
 * syntax, the uncordon that produced a ready one, and, for an Orker a store
 * outage or its own stop alone holds, which no command produced or can clear,
 * the read of it.
 */
export function orkerCommand(orker: Orker): string {
	if (operatorCordoned(orker)) {
		return `orkctl orker cordon ${shellQuote(orker.orkerId)} --reason ${shellQuote(orker.cordonReason)}`;
	}
	if (orker.storeOutage || orker.stopping) return `orkctl orker get ${shellQuote(orker.orkerId)}`;
	return `orkctl orker uncordon ${shellQuote(orker.orkerId)}`;
}

/** The command the cordon dialog is about to run, previewed as the reason is typed. */
export function cordonCommand(orkerId: string, reason: string): string {
	return `orkctl orker cordon ${shellQuote(orkerId)} --reason ${shellQuote(reason)}`;
}

/**
 * The sentence a stopping Orker's row carries: when its stop began and when
 * its grace ends, as `orkctl orker get` prints them. An Orker of a release that
 * reports neither keeps the plain sentence.
 */
export function stoppingNote(orker: Orker, fmt: (ts: Timestamp | undefined) => string): string {
	const stop = orker.stopping;
	const since = stop?.since ? ` since ${fmt(stop.since)}` : '';
	const ends = stop?.graceEndsAt ? `; its grace ends ${fmt(stop.graceEndsAt)}` : '';
	return `Stopping${since}: it takes no new work and finishes what it runs, then exits${ends}. A task it hands back runs again on another Orker of its release.`;
}
