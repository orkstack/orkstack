// The run page's awaited-task logic, pure and unit-tested. An awaited task
// starts no container: it waits until something outside the run resolves it
// (`orkctl run resolve`, the run page's dialogs, or the callback door), or
// its deadline passes. Every sentence here mirrors cmd/orkctl/resolve.go.
import { Status, type ArtifactRef, type Payload } from './gen/orkstack/v1/common_pb';
import { AwaitResolvedVia, type TaskAwait } from './gen/orkstack/v1/run_pb';
import { emptyPayload, inlinePayload, payloadJson, runCommand, shellQuote } from './runDetail';

/** A protobuf Timestamp, or the page's own `now` in the same shape. */
export type Timestamp = { seconds: bigint; nanos: number };

/** The callback door's address for one await, on the origin the page was opened at. */
export function awaitCallbackUrl(origin: string, runId: string, taskId: string): string {
	return `${origin.replace(/\/+$/, '')}/v1/runs/${encodeURIComponent(runId)}/awaits/${taskId}`;
}

/**
 * awaitCurl renders the callback. A real token is single-quoted, which keeps
 * whatever it contains from being read as shell syntax; with no token the
 * placeholder is DOUBLE quoted so $ORKSTACK_CALLBACK_TOKEN expands — single
 * quotes would copy cleanly and then 401, exactly the Triggers tab's rule.
 */
export function awaitCurl({ url, token, reject }: { url: string; token?: string; reject?: boolean }): string {
	const authorization =
		token !== undefined
			? shellQuote(`Authorization: Bearer ${token}`)
			: '"Authorization: Bearer $ORKSTACK_CALLBACK_TOKEN"';
	return [
		`curl -X POST ${shellQuote(reject ? `${url}/reject` : url)} \\`,
		`  -H ${authorization} \\`,
		`  -H 'Content-Type: application/json' \\`,
		reject ? `  -d '{"reason":"…"}'` : `  -d '{}'`
	].join('\n');
}

/** The `orkctl run resolve` command that does what a dialog is about to do. */
export function resolveCli({
	namespace,
	runId,
	taskId,
	valuesJson,
	reject,
	reason
}: {
	namespace: string;
	runId: string;
	taskId: string;
	valuesJson?: string;
	reject?: boolean;
	reason?: string;
}): string {
	const args = ['run', 'resolve', runId, '--task', taskId];
	if (reject) args.push('--reject', '--reason', reason ?? '');
	else if (valuesJson !== undefined && valuesJson.trim() !== '' && valuesJson.trim() !== '{}') {
		args.push('--values', valuesJson);
	}
	return runCommand(namespace, args);
}

function toMs(ts: Timestamp): number {
	return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
}

function fromMs(ms: number): Timestamp {
	const seconds = Math.floor(ms / 1000);
	return { seconds: BigInt(seconds), nanos: (ms - seconds * 1000) * 1_000_000 };
}

/**
 * awaitDeadline is the snapshot's deadline when the page has read one, and
 * otherwise TASK_AWAITING's own timestamp plus the pinned definition's
 * timeout — equal by construction: the engine stamps both from one
 * workflow.Now.
 */
export function awaitDeadline(
	task: { await?: TaskAwait },
	timeoutSeconds: number,
	awaitingAt: Timestamp | undefined
): Timestamp | undefined {
	if (task.await?.deadline) return { seconds: task.await.deadline.seconds, nanos: task.await.deadline.nanos };
	if (!awaitingAt || timeoutSeconds <= 0) return undefined;
	return fromMs(toMs(awaitingAt) + timeoutSeconds * 1000);
}

/** A coarse countdown — hours and minutes, minutes, or seconds — as `run get` prints it. */
export function formatRemaining(ms: number): string {
	const totalSeconds = Math.floor(ms / 1000);
	const h = Math.floor(totalSeconds / 3600);
	const m = Math.floor((totalSeconds % 3600) / 60);
	if (h > 0) return `${h}h${m}m`;
	if (m > 0) return `${m}m`;
	return `${totalSeconds}s`;
}

/** The live REMAINING: `in 5h41m`, `expired`, or `-` when there is no deadline. */
export function awaitRemaining(deadline: Timestamp | undefined, now: Timestamp): string {
	if (!deadline) return '-';
	const left = toMs(deadline) - toMs(now);
	if (left < 0) return 'expired';
	return `in ${formatRemaining(left)}`;
}

/** A timeout in the words a person reads: 6h, 1h30m, 45m, 30s. */
export function humanTimeout(seconds: number): string {
	const h = Math.floor(seconds / 3600);
	const m = Math.floor((seconds % 3600) / 60);
	const s = seconds % 60;
	if (h > 0) return m > 0 ? `${h}h${m}m` : `${h}h`;
	if (m > 0) return s > 0 ? `${m}m${s}s` : `${m}m`;
	return `${s}s`;
}

/** The door a resolution came through, as every client names it. */
export function resolvedViaWord(via: AwaitResolvedVia | undefined): string {
	switch (via) {
		case AwaitResolvedVia.API:
			return 'api';
		case AwaitResolvedVia.CALLBACK:
			return 'callback';
		default:
			return '';
	}
}

function resolver(task: { await?: TaskAwait }): string {
	const via = resolvedViaWord(task.await?.resolvedVia);
	const by = task.await?.resolvedBy ? ` by ${task.await.resolvedBy}` : '';
	return via ? ` via ${via}${by}` : '';
}

/** The await panel's one-line state. */
export function awaitStateLabel(task: {
	status: Status;
	await?: TaskAwait;
	error?: { code: string };
	timeoutSeconds?: number;
	opened?: boolean;
}): string {
	switch (task.status) {
		case Status.AWAITING:
			return 'Waiting for external completion';
		case Status.SUCCEEDED:
			return `Completed${resolver(task)}`;
		case Status.FAILED:
			if (task.error?.code === 'AWAIT_TIMED_OUT') return 'Timed out';
			if (task.error?.code === 'AWAIT_REJECTED') return `Rejected${resolver(task)}`;
			return 'Failed';
		case Status.CANCELED:
			return task.opened ? 'Canceled while waiting' : 'Canceled before it started waiting';
		case Status.SKIPPED:
			return 'Skipped: it never started waiting';
		default:
			return task.timeoutSeconds
				? `Will wait up to ${humanTimeout(task.timeoutSeconds)} once its needs are complete`
				: 'Will wait once its needs are complete';
	}
}

/** The most of a resolution's values a panel shows: a value can be as large as the deployment allows. */
export const VALUES_SHOW_MAX = 64 * 1024;

/** The size at or below which a resolution's values travel inline in the run (awaitInlineValuesLimit). */
export const RESOLUTION_INLINE_MAX = 4096n;

// Control characters and the marks that reorder text on screen: they are what
// they are in the value, but they must not act on the page.
const UNPRINTABLE = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f‎‏‪-‮⁦-⁩]/g;

/** Shows control characters as their escapes, leaving lines and tabs. */
export function printable(text: string): string {
	return text.replace(UNPRINTABLE, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`);
}

// Indenting deep JSON multiplies it: 4 KiB of nested arrays indents to megabytes,
// more than a box should lay out. Past the bound it stays as it was written.
function indentedOr(indented: string, compact: string): string {
	return indented.length <= VALUES_SHOW_MAX ? indented : compact;
}

export type ResolvedValuesView =
	| { kind: 'none' }
	| { kind: 'inline'; text: string }
	| { kind: 'long'; text: string; bytes: number }
	| { kind: 'reference'; bytes: number };

/**
 * How a panel draws what an await was resolved with: one line in the flow when
 * it is short; closed until asked when it is long; and read by reference when
 * the run carries none of it inline.
 */
export function resolvedValuesView(output: Payload | undefined, valuesRef: Pick<ArtifactRef, 'sizeBytes'> | undefined): ResolvedValuesView {
	// A value past what a run carries inline leaves an empty payload beside its
	// reference: that empty payload is not what it was resolved with. Values of
	// RESOLUTION_INLINE_MAX bytes or fewer always travel inline, so an empty
	// payload beside one that small is what they were, `{}` with a newline or
	// spaces included.
	if (valuesRef && valuesRef.sizeBytes > RESOLUTION_INLINE_MAX && emptyPayload(output)) return { kind: 'reference', bytes: Number(valuesRef.sizeBytes) };
	if (output) {
		const line = inlinePayload(output);
		if (line !== null) return { kind: 'inline', text: printable(line) };
		const compact = JSON.stringify(output.data ?? {});
		return { kind: 'long', text: printable(indentedOr(payloadJson(output), compact)), bytes: new TextEncoder().encode(compact).length };
	}
	return { kind: 'none' };
}

export interface ValuesRead {
	text: string;
	/** The part of a value too long to show whole. */
	cut: boolean;
}

/** Stored values as a reader sees them: indented when whole and valid, as they are otherwise. */
export function readableValues(raw: string, cut: boolean): ValuesRead {
	if (!cut) {
		try {
			return { text: printable(indentedOr(JSON.stringify(JSON.parse(raw), null, 2), raw)), cut };
		} catch {
			// Not ours to judge here: what was stored is what is shown.
		}
	}
	return { text: printable(raw), cut };
}
