// Pure helpers for the run comparison page and its Compare dialog.
//
// The comparison itself is computed SERVER-side (RunService.DiffRuns): every
// value in a row is a rendered string, and `orkctl run diff` prints the same
// strings. What lives here is only what a client has to decide — how a side
// with no value looks, which run the dialog offers first, and the two
// sentences the CLI prints under its table — each mirroring cmd/orkctl/
// run_diff.go so the page and the terminal say the same thing.
import { Status } from './gen/orkstack/v1/common_pb';
import type { Run } from './gen/orkstack/v1/run_pb';
import type { DiffRunsResponse, RunFieldDiff } from './gen/orkstack/v1/service_pb';

/** The DiffRuns vocabulary, in canonical order. */
export const DIFF_FIELDS = [
	'definition',
	'input',
	'presence',
	'status',
	'attempt',
	'pool',
	'pin',
	'orker',
	'exit_code',
	'error',
	'duration',
	'image',
	'platform_digest',
	// A cached task's memo decision (docs/DSL.md "Caching"): hit <source run>, miss, bypassed.
	'cached'
] as const;

// docs/UI_CLI_PARITY.md "Run diff"; cmd/orkctl/run_diff.go carries the same table.
const REASON_TEXT: Record<string, string> = {
	definition_unavailable: 'the pinned definition is no longer stored',
	definition_digest_mismatch: 'the stored definition does not hash to the digest this run pinned',
	task_not_in_definition: 'the pinned definition has no task of this id',
	no_attempt_store: 'this engine has no task attempt store configured',
	no_outcome_attempt: 'the snapshot names no outcome attempt',
	attempt_record_missing:
		'no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one',
	timestamps_missing: 'the snapshot lacks a start or finish time',
	timestamps_inverted: 'the finish time precedes the start time'
};

/** `?only=image, pool` → ['image', 'pool']. Unknown names are kept: the server refuses them, naming the valid ones. */
export function parseOnly(param: string | null): string[] {
	if (!param) return [];
	return param
		.split(',')
		.map((field) => field.trim())
		.filter((field) => field !== '');
}

/**
 * The run the Compare dialog offers first: the newest SUCCEEDED run that is
 * not this one ("last successful" is the comparison asked for most), else the
 * newest other run, else none. `runs` is in ListRuns order, newest first, and
 * is never re-sorted here.
 */
export function defaultCompareTarget(runs: Run[], currentId: string): string {
	const others = runs.filter((run) => run.id !== currentId);
	return (others.find((run) => run.status === Status.SUCCEEDED) ?? others[0])?.id ?? '';
}

/** One side of a row: `-` means "does not apply", `unavailable` means the evidence is gone. */
export function renderDiffValue(value: string, available: boolean): string {
	if (!available) return 'unavailable';
	return value === '' ? '-' : value;
}

/** A reason token's sentence; a token from a later build renders as itself. */
export function reasonText(token: string): string {
	return REASON_TEXT[token] ?? token;
}

/** The CLI's summary sentence, verbatim. */
export function diffSummary(resp: DiffRunsResponse): string {
	const compared = resp.tasksCompared;
	const identical = resp.tasksIdentical;
	let line = `${compared - identical} of ${compared} tasks differ; ${identical} identical`;
	const unavailable = resp.evidenceUnavailable;
	if (unavailable === 1) line += '; 1 field unavailable';
	else if (unavailable > 1) line += `; ${unavailable} fields unavailable`;
	return `${line}.`;
}

/** The CLI's footnotes: one line per unavailable side, in row order. */
export function unavailableLines(resp: DiffRunsResponse): string[] {
	const lines: string[] = [];
	const footnote = (row: RunFieldDiff, runId: string, reason: string) => {
		const subject = row.taskId === '' ? 'run' : row.taskId;
		let line = `${subject} ${row.field} in ${runId}: ${reason}`;
		const sentence = reasonText(reason);
		if (sentence !== reason) line += ` (${sentence})`;
		lines.push(line);
	};
	for (const row of resp.differences) {
		if (!row.fromAvailable) footnote(row, resp.from?.id ?? '', row.fromReason);
		if (!row.toAvailable) footnote(row, resp.to?.id ?? '', row.toReason);
	}
	return lines;
}

/** Whether `fields_compared` is narrower than the whole vocabulary. */
export function isNarrowed(resp: DiffRunsResponse): boolean {
	return resp.fieldsCompared.length < DIFF_FIELDS.length;
}
