// The run page's force cancel rules, pure and unit-tested: whether it is
// offered, the grace choices, and the words of the Force cancel card. The
// server decides every request (ForceCancelRun refuses with a reason,
// docs/REASONS.md "Force cancel"); how the run ended is the run's, never this.
import {
	ForceCancelCascadeState,
	ForceCancelState,
	type ForceCancelCascade,
	type ForceCancelRunBelow,
	type ForceCancelRunResponse,
	type ForceCancelSubtree,
	type GetForceCancelResponse
} from './gen/orkstack/v1/run_service_pb';
import { Status } from './gen/orkstack/v1/common_pb';
import { isTerminal } from './runStatus';

/** The longest reason a force cancel takes, in bytes of UTF-8. */
export const REASON_MAX_BYTES = 512;

/** How often the page asks where a force cancel stands. */
export const POLL_MS = 5_000;

/** What the page knows beyond the run. */
export interface ForceCancelContext {
	/** The caller holds run.cancel.force and run.read where the run lives. */
	allowed: boolean;
	/** The run's newest force cancel, when there is one. */
	current?: GetForceCancelResponse;
}

/** Whether Force cancel is offered, and why it is disabled when it is. */
export interface ForceCancelOffer {
	shown: boolean;
	disabled: string;
}

/**
 * forceCancelOffer: on a run that has not ended, to a caller who may. While
 * one is in progress a request would only join it.
 */
export function forceCancelOffer(status: Status, ctx: ForceCancelContext): ForceCancelOffer {
	if (isTerminal(status) || !ctx.allowed) return { shown: false, disabled: '' };
	return { shown: true, disabled: inProgress(ctx.current) ? 'Force cancel in progress' : '' };
}

/** inProgress reports a force cancel that still runs, its cascade included. */
export function inProgress(r: GetForceCancelResponse | undefined): boolean {
	if (!r) return false;
	return r.state === ForceCancelState.REQUESTED || r.cascade?.state === ForceCancelCascadeState.RUNNING;
}

/**
 * awaited reports whether where a force cancel stands is to be read again: it
 * runs, or it is the one this page asked for and has not been read yet.
 */
export function awaited(r: GetForceCancelResponse | undefined, requestId: string): boolean {
	return inProgress(r) || (requestId !== '' && r?.requestId !== requestId);
}

/** cascadeInProgress reports a cascade that may be stopped. */
export function cascadeInProgress(r: GetForceCancelResponse | undefined): boolean {
	return r?.cascade?.state === ForceCancelCascadeState.RUNNING;
}

/** The graces the dialog offers: the server's least, or a longer one. */
export const graceChoices: { label: string; seconds: number | null }[] = [
	{ label: 'The least this run needs', seconds: null },
	{ label: '2 minutes', seconds: 120 },
	{ label: '5 minutes', seconds: 300 },
	{ label: '10 minutes', seconds: 600 }
];

/** reasonProblem is why a reason cannot be sent, '' when it can. */
export function reasonProblem(reason: string): string {
	const text = reason.trim();
	if (!text) return 'Give a reason: it is recorded with the run’s end.';
	const bytes = new TextEncoder().encode(text).length;
	if (bytes > REASON_MAX_BYTES) return `The reason is ${bytes} bytes; at most ${REASON_MAX_BYTES} fit.`;
	if (/\p{Cc}|\p{Cf}|\p{Zl}|\p{Zp}/u.test(text)) return 'The reason holds a line break or an invisible character.';
	return '';
}

/** belowListed is what the dialog says of the search for open runs below. */
export function belowListed(below: ForceCancelSubtree | undefined, error: string): string {
	if (error) return `The open runs below it could not be listed: ${error}`;
	if (!below) return 'Looking for open runs below it…';
	const n = below.runs.length;
	const found = n === 0 ? 'No open run below it' : `${n} open ${n === 1 ? 'run' : 'runs'} below it`;
	const more = below.truncatedBy ? [`not every one could be listed (${below.truncatedBy})`] : [];
	if (below.exclusions.length > 0) more.push(`${below.exclusions.length} left out (${[...new Set(below.exclusions.map((e) => e.cause))].join(', ')})`);
	return `${[found, ...more].join('; ')}.`;
}

/** requestNotes are what a request did beside starting one, for the page's note. */
export function requestNotes(r: ForceCancelRunResponse): string[] {
	const out: string[] = [];
	if (r.state === ForceCancelState.ALREADY_ENDED) out.push('The run had already ended.');
	else if (r.attached) out.push('A force cancel of this run was already in progress; its reason and grace apply.');
	if (r.direct) out.push('The run was ended at once: two force cancels had left it open.');
	if (r.attachUnknown) out.push('Whether another is in progress could not be told; this one may run beside it.');
	return out;
}

/** needsMore reports a force cancel that the same request made again finishes. */
export function needsMore(r: GetForceCancelResponse): boolean {
	if (inProgress(r)) return false;
	return (
		r.incomplete ||
		r.state === ForceCancelState.FAILED ||
		(r.cascade !== undefined && r.cascade.state !== ForceCancelCascadeState.COMPLETE)
	);
}

/** stateWords is where a force cancel stands, in the CLI's and the docs' words. */
export function stateWords(r: GetForceCancelResponse, runEnded = false): string {
	switch (r.state) {
		case ForceCancelState.REQUESTED:
			if (runEnded) return 'The run has ended; how is checked once its grace is over';
			return 'In progress: the run is asked to cancel, and ended only if it makes no progress';
		case ForceCancelState.FORCE_CANCELED:
			return r.incomplete ? 'Ended the run; something of its end is missing' : 'Ended the run';
		case ForceCancelState.CANCELED:
			return 'The run ended on its own cancel';
		case ForceCancelState.ENDED:
			return 'The run ended on its own';
		case ForceCancelState.ALREADY_ENDED:
			return r.incomplete ? 'The run had ended; something of its end is missing' : 'The run had already ended';
		case ForceCancelState.FAILED:
			return 'Failed; the run may still be open';
		default:
			return 'Unknown';
	}
}

const rowWords: Partial<Record<ForceCancelState, string>> = {
	[ForceCancelState.REQUESTED]: 'Waiting',
	[ForceCancelState.WAITING]: 'Waiting, for another’s force cancel',
	[ForceCancelState.FORCE_CANCELED]: 'Force-canceled',
	[ForceCancelState.CANCELED]: 'Canceled',
	[ForceCancelState.ENDED]: 'Ended',
	[ForceCancelState.ALREADY_ENDED]: 'Had ended',
	[ForceCancelState.FAILED]: 'Failed',
	[ForceCancelState.PENDING]: 'Not ended yet; it goes on'
};

/** belowWords is what became of a run below. */
export function belowWords(r: ForceCancelRunBelow): string {
	const words = rowWords[r.state] ?? 'Unknown';
	return r.cause ? `${words} (${r.cause})` : words;
}

/** cascadeWords is a cascade in one line. */
export function cascadeWords(c: ForceCancelCascade): string {
	if (c.unread) return 'Where it stands could not be asked';
	const total = c.runs.length;
	const ended = [ForceCancelState.FORCE_CANCELED, ForceCancelState.CANCELED, ForceCancelState.ENDED, ForceCancelState.ALREADY_ENDED];
	const over = c.runs.filter((r) => ended.includes(r.state)).length;
	switch (c.state) {
		case ForceCancelCascadeState.RUNNING:
			return `${over} of ${total} runs below ended`;
		case ForceCancelCascadeState.COMPLETE:
			return total === 0 ? 'Nothing below was open' : `${total} ${total === 1 ? 'run' : 'runs'} below ended, and none is open`;
		case ForceCancelCascadeState.PARTIAL:
			return 'Some runs below may still be open';
		case ForceCancelCascadeState.STOPPED:
			return 'Stopped before its end; some runs below may still be open';
		default:
			return 'Unknown';
	}
}

/** cascadeNotes are what a cascade left, one sentence each. */
export function cascadeNotes(c: ForceCancelCascade): string[] {
	const out: string[] = [];
	const after = c.openAfter;
	if (after && after.count > 0) out.push(`${after.count} open after it`);
	if (after?.unconfirmedCause) out.push(`Some runs couldn’t be checked (${after.unconfirmedCause})`);
	if (c.cause) out.push(`The runs below could not be looked for (${c.cause})`);
	if (c.leftToParents > 0) out.push(`${c.leftToParents} deeper ${c.leftToParents === 1 ? 'run is' : 'runs are'} left to the cancel of the run above`);
	if (c.notStarted > 0) out.push(`${c.notStarted} not force-canceled, and no run above is open to cancel them`);
	for (const e of c.exclusions) out.push(`Left out: ${e.runId || 'a run'} (${e.cause})`);
	return out;
}
