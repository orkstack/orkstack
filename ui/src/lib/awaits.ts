// The run page's awaited-task logic, pure and unit-tested. An awaited task
// starts no container: it waits until something outside the run resolves it
// (`orkctl run resolve`, the run page's dialogs, or the callback door), or
// its deadline passes. Every sentence here mirrors cmd/orkctl/resolve.go.
import { Status } from './gen/orkstack/v1/common_pb';
import { AwaitResolvedVia, type TaskAwait } from './gen/orkstack/v1/run_pb';
import { runCommand, shellQuote } from './runDetail';

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
