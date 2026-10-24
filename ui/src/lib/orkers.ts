import type { Orker } from './gen/orkstack/v1/activity_pb';
import { shellQuote } from './runDetail';

// The Orker inventory's rendering rules, shared by Admin → Pools and its
// tests (docs/DEPLOY.md "Removing an Orker safely: cordon and drain"). They mirror `orkctl orker list`
// (cmd/orkctl/orker.go): an Orker that could not be asked is unreachable,
// never idle, and none of its numbers are drawn.

export type OrkerState = 'ready' | 'cordoned' | 'unreachable';

export function orkerState(orker: Orker): OrkerState {
	if (orker.unavailableReason) return 'unreachable';
	return orker.cordoned ? 'cordoned' : 'ready';
}

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
 * syntax, and the uncordon that produced a ready one.
 */
export function orkerCommand(orker: Orker): string {
	if (orker.cordoned) {
		return `orkctl orker cordon ${shellQuote(orker.orkerId)} --reason ${shellQuote(orker.cordonReason)}`;
	}
	return `orkctl orker uncordon ${shellQuote(orker.orkerId)}`;
}

/** The command the cordon dialog is about to run, previewed as the reason is typed. */
export function cordonCommand(orkerId: string, reason: string): string {
	return `orkctl orker cordon ${shellQuote(orkerId)} --reason ${shellQuote(reason)}`;
}
