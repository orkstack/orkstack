import { Status } from './gen/orkstack/v1/common_pb';

/**
 * The one classifier of a status, the UI's twin of internal/runstatus: live
 * (it may still change, so a page keeps following it) or terminal (it has
 * ended). The Record makes every generated value a compile-time case, so a
 * status added to the proto fails the type check until it is classified;
 * UNSPECIFIED, the absence of a status, is neither.
 */
const CLASS: Record<Status, 'live' | 'terminal' | 'none'> = {
	[Status.UNSPECIFIED]: 'none',
	[Status.PENDING]: 'live',
	[Status.RUNNING]: 'live',
	[Status.AWAITING]: 'live',
	[Status.SLEEPING]: 'live',
	[Status.PAUSED]: 'live',
	[Status.SUCCEEDED]: 'terminal',
	[Status.FAILED]: 'terminal',
	[Status.CANCELED]: 'terminal',
	// SKIPPED is terminal too: the DAG decided this task will not run, and
	// nothing after that decision moves it.
	[Status.SKIPPED]: 'terminal'
};

/** isLive reports a status that has not ended: PAUSED among them. */
export function isLive(status: Status | undefined): boolean {
	return status !== undefined && CLASS[status] === 'live';
}

/** isTerminal reports an ended status: SUCCEEDED, FAILED, CANCELED or SKIPPED. */
export function isTerminal(status: Status | undefined): boolean {
	return status !== undefined && CLASS[status] === 'terminal';
}
