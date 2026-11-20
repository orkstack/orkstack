import type { Status } from './gen/orkstack/v1/common_pb';
import { isTerminal } from './runStatus';

/** What the run page shows of Cancel run. */
export interface CancelControl {
	/** Whether the page offers it at all. */
	shown: boolean;
	/** Why it cannot be pressed, or '' when it can. */
	disabled: string;
}

/**
 * cancelControl is the run page's Cancel run, as pauseControls is its pause:
 * hidden from a caller without run.cancel at the run's Orkflow (`allowed`,
 * from permissions.can), whom the server would refuse, and from a run that
 * has finished; shown disabled once a cancel was requested.
 */
export function cancelControl(status: Status, ctx: { allowed: boolean; requested: boolean }): CancelControl {
	if (isTerminal(status) || !ctx.allowed) return { shown: false, disabled: '' };
	return { shown: true, disabled: ctx.requested ? 'Cancellation requested' : '' };
}
