// One task on its run's timeline, the axis every row of the Tasks card shares:
// where it waited, the attempts that failed, and what ran or is running, as
// fractions of the run's span. Pure, and the only place those bars are decided.
import { formatDuration } from './runDetail';

export type BarKind = 'wait' | 'ran' | 'running' | 'failed';

export interface TimelineBar {
	kind: BarKind;
	left: number;
	width: number;
}

export interface TimelineAttempt {
	attempt: number;
	failed: boolean;
	started: number | null;
	finished: number | null;
}

export interface TimelineInput {
	run: { start: number | null; end: number | null };
	scheduled: number | null;
	started: number | null;
	finished: number | null;
	running: boolean;
	now: number;
	attempts: readonly TimelineAttempt[];
	/** The last attempt failed: its bar says so. */
	failed?: boolean;
	/** A running fan-out: when the latest of its items finished. */
	doneUntil?: number | null;
}

export interface Timeline {
	bars: TimelineBar[];
	/** The same bars in words, for the row's accessible name. */
	words: string;
}

/** The share of a run's timeline a wait must fill to be drawn. */
const MIN_WAIT = 0.01;

const seconds = (ms: number) => {
	const stamp = (t: number) => ({ seconds: BigInt(Math.floor(t / 1000)), nanos: (t % 1000) * 1e6 });
	return formatDuration(stamp(0), stamp(Math.max(ms, 0)));
};

export function taskTimelineBars(input: TimelineInput): Timeline {
	const { run, scheduled, started, finished, running, now, attempts, failed = false, doneUntil = null } = input;
	const none = { bars: [], words: '' };
	if (run.start === null || run.end === null || run.end <= run.start) return none;
	const span = run.end - run.start;
	const at = (t: number) => Math.min(Math.max((t - run.start!) / span, 0), 1);
	const bars: TimelineBar[] = [];
	const words: string[] = [];
	const push = (kind: BarKind, from: number, to: number) => {
		if (to < from) return;
		const width = Math.max(at(to) - at(from), 0);
		// A wait too short to see on this scale is no bar: the words still say it.
		if (kind === 'wait' && width < MIN_WAIT) return;
		bars.push({ kind, left: at(from), width });
	};
	const from = scheduled ?? started;
	if (from === null) return none;
	const end = finished ?? (running ? now : null);

	const tried = attempts.filter((a) => a.started !== null);
	if (tried.length > 1) {
		let cursor = from;
		tried.forEach((a, i) => {
			const last = i === tried.length - 1;
			const stop = a.finished ?? (last ? end : null) ?? a.started!;
			if (a.started! > cursor) {
				push('wait', cursor, a.started!);
				words.push(`waited ${seconds(a.started! - cursor)}`);
			}
			if (a.failed) {
				push('failed', a.started!, stop);
				words.push(`attempt ${a.attempt} failed after ${seconds(stop - a.started!)}`);
			} else if (last && running && a.finished === null) {
				push('running', a.started!, stop);
				words.push(`attempt ${a.attempt} has run ${seconds(stop - a.started!)}`);
			} else {
				push('ran', a.started!, stop);
				words.push(`attempt ${a.attempt} ran ${seconds(stop - a.started!)}`);
			}
			cursor = stop;
		});
		return { bars, words: words.join(', ') };
	}

	const begun = started ?? (running ? null : from);
	if (begun === null) {
		// Dispatched, and no Orker has reported a start: all of it is waiting.
		if (end !== null) {
			push('wait', from, end);
			words.push(`has waited ${seconds(end - from)}`);
		}
		return { bars, words: words.join(', ') };
	}
	if (begun > from) {
		push('wait', from, begun);
		words.push(`waited ${seconds(begun - from)}`);
	}
	if (end !== null) {
		const kind: BarKind = running ? 'running' : failed ? 'failed' : 'ran';
		// A running fan-out: what its items have finished, then what is still going.
		if (running && doneUntil !== null && doneUntil > begun && doneUntil < end) {
			push('ran', begun, doneUntil);
			push('running', doneUntil, end);
		} else {
			push(kind, begun, end);
		}
		words.push(`${running ? 'has run' : failed ? 'failed after' : 'ran'} ${seconds(end - begun)}`);
	}
	return { bars, words: words.join(', ') };
}

/** The Tasks card's axis: round marks that fit its span, as fractions of it. */
export function timelineAxis(spanMs: number): { label: string; at: number }[] {
	if (!(spanMs > 0)) return [];
	const steps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400];
	const wanted = spanMs / 1000 / 5;
	const step = steps.find((s) => s >= wanted) ?? steps[steps.length - 1];
	const marks: { label: string; at: number }[] = [];
	for (let t = 0; t * 1000 < spanMs; t += step) {
		const label = t < 60 ? `${t}s` : t < 3600 ? `${Math.floor(t / 60)}m` : `${Math.floor(t / 3600)}h`;
		marks.push({ label, at: (t * 1000) / spanMs });
	}
	return marks;
}
