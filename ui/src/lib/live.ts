import {
	HeartbeatUnavailableReason,
	LiveAttemptState,
	type LiveAttempt
} from './gen/orkstack/v1/service_pb';

// The run page's live line: what an in-flight attempt is doing right now,
// derived from ONE InspectRun answer and the engine's clock.
//
// There is deliberately no copy of the heartbeat constants here. Whether a
// beat is stale, and when Temporal will act on it, are the server's answers
// (heartbeatStale, heartbeatDeadlineAt); the page only renders them. Every
// age is measured against `nowMs`, which the page derives from the response's
// computed_at plus the wall time since it arrived — never from the browser's
// clock alone — so a browser whose clock is off cannot make a healthy Orker
// look dead. The sentences match `orkctl run inspect` (cmd/orkctl/inspect.go).

/** The kind of live line a task card shows. */
export type LiveKind = 'started' | 'waiting' | 'retry' | 'cancelling' | 'unavailable';

export interface LiveLine {
	kind: LiveKind;
	text: string;
	title: string;
	stale: boolean;
}

type Timestamp = { seconds: bigint; nanos: number };

function ms(ts: Timestamp | undefined): number | undefined {
	if (!ts) return undefined;
	return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
}

/**
 * formatBytes renders a byte count exactly as the CLI does: whole bytes below
 * 1 KiB, then KiB, MiB and GiB with one decimal. Zero prints `0 B` — a live
 * container that has written nothing is a fact, not an absence.
 */
export function formatBytes(n: bigint | number): string {
	const value = typeof n === 'bigint' ? Number(n) : n;
	if (value < 1024) return `${value} B`;
	let scaled = value / 1024;
	for (const suffix of ['KiB', 'MiB']) {
		if (scaled < 1024) return `${scaled.toFixed(1)} ${suffix}`;
		scaled /= 1024;
	}
	return `${scaled.toFixed(1)} GiB`;
}

/** A span of whole seconds, in the run page's own style (`42s`, `2m 10s`). */
function span(seconds: number): string {
	if (seconds < 60) return `${seconds}s`;
	return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

/** A wall-clock time of day, 24-hour, in the viewer's zone. */
function clock(atMs: number): string {
	return new Date(atMs).toLocaleTimeString('en-GB', { hour12: false });
}

/**
 * heartbeatAge is how long ago the attempt's last beat arrived, against the
 * engine's clock. A non-positive age reads `just now`.
 */
export function heartbeatAge(attempt: LiveAttempt, nowMs: number): { seconds: number; label: string } {
	const beat = ms(attempt.lastHeartbeatAt);
	const seconds = beat === undefined ? 0 : Math.round((nowMs - beat) / 1000);
	return { seconds, label: seconds <= 0 ? 'just now' : `${span(seconds)} ago` };
}

/** The fixed sentence for each reason a heartbeat is unavailable. */
export function unavailableReason(reason: HeartbeatUnavailableReason): string {
	switch (reason) {
		case HeartbeatUnavailableReason.NOT_STARTED:
			return 'not started';
		case HeartbeatUnavailableReason.NO_HEARTBEAT_YET:
			return 'no heartbeat yet';
		case HeartbeatUnavailableReason.UNDECODABLE:
			return 'heartbeat detail undecodable';
		default:
			return 'heartbeat unavailable';
	}
}

/**
 * liveLine is the one sentence a RUNNING task's card shows about its pending
 * attempt. A heartbeat that is not there is `unavailable` with its reason,
 * never a zero.
 */
export function liveLine(attempt: LiveAttempt | undefined, nowMs: number): LiveLine {
	if (!attempt) {
		return {
			kind: 'unavailable',
			text: 'Live detail unavailable',
			title: 'No attempt of this task is pending in the cluster right now.',
			stale: false
		};
	}
	switch (attempt.state) {
		case LiveAttemptState.SCHEDULED: {
			const next = ms(attempt.nextAttemptAt);
			if (next !== undefined) {
				const until = Math.round((next - nowMs) / 1000);
				return {
					kind: 'retry',
					text: `Attempt ${attempt.attempt} scheduled for ${clock(next)} · ${until <= 0 ? 'due now' : `in ${span(until)}`}`,
					title: `Attempt ${attempt.attempt - 1} ended; its error is in the attempt timeline.`,
					stale: false
				};
			}
			const scheduled = ms(attempt.attemptScheduledAt);
			const waited = scheduled === undefined ? 0 : Math.max(0, Math.round((nowMs - scheduled) / 1000));
			return {
				kind: 'waiting',
				text: `Waiting for an orker · ${span(waited)}`,
				title: 'No orker has picked this attempt up yet: the pool has no free orker.',
				stale: false
			};
		}
		case LiveAttemptState.CANCEL_REQUESTED: {
			const phase = attempt.heartbeatAvailable ? `phase ${attempt.phase || '—'}` : 'phase unavailable';
			return {
				kind: 'cancelling',
				text: `Cancelling · ${phase}`,
				title: `Cancellation requested: the orker sends SIGTERM, waits the ${attempt.cancellationGracePeriodSeconds}s grace, then SIGKILL.`,
				stale: false
			};
		}
		case LiveAttemptState.STARTED: {
			if (!attempt.heartbeatAvailable) {
				const reason = unavailableReason(attempt.heartbeatUnavailableReason);
				return {
					kind: 'unavailable',
					text: `Phase unavailable · ${reason}`,
					title: `An orker has this attempt; ${reason}.`,
					stale: false
				};
			}
			const age = heartbeatAge(attempt, nowMs);
			const deadline = ms(attempt.heartbeatDeadlineAt);
			const deadlineClock = deadline === undefined ? '—' : clock(deadline);
			const text = `Phase ${attempt.phase} · ${formatBytes(attempt.bytesLogged)} logged · heartbeat ${age.label}`;
			if (attempt.heartbeatStale) {
				return {
					kind: 'started',
					text: `${text} — STALE`,
					title: `No heartbeat for ${span(Math.max(0, age.seconds))}. The orker is gone or partitioned; Temporal times this attempt out at ${deadlineClock} unless a beat arrives.`,
					stale: true
				};
			}
			return {
				kind: 'started',
				text,
				title: `Last heartbeat ${age.label}; Temporal times this attempt out at ${deadlineClock} unless another beat arrives.`,
				stale: false
			};
		}
		default:
			return {
				kind: 'unavailable',
				text: `Phase unavailable · ${unavailableReason(attempt.heartbeatUnavailableReason)}`,
				title: 'The cluster reports this attempt in a state OrkStack does not name.',
				stale: false
			};
	}
}
