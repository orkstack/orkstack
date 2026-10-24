import type { ArtifactRef, Payload } from './gen/orkstack/v1/common_pb';

// The characters a POSIX shell never reads as syntax: a token made only of
// them is passed as written. Everything else — a space, a quote, `$`, `*`,
// `~`, a backslash, a newline — is single-quoted.
const SHELL_SAFE = /^[A-Za-z0-9_@%+=:,./-]+$/;

/**
 * Quote one CLI argument for a POSIX shell without trusting page data as
 * code: a token that holds only safe characters is written bare, the way a
 * person types it, and any other is single-quoted (an embedded quote closes,
 * escapes and reopens). The empty string is `''`.
 */
export function shellQuote(value: string): string {
	if (SHELL_SAFE.test(value)) return value;
	return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

/**
 * A run id drawn short where a whole one does not fit: its first four and
 * last six characters ("ork-…873153"). Characters are counted, never parsed:
 * nothing is read from an id's parts (AGENTS.md §7).
 */
export function shortRunId(id: string): string {
	return id.length > 13 ? `${id.slice(0, 4)}…${id.slice(-6)}` : id;
}

/** A copyable command always states the namespace, including `default`. */
export function runCommand(namespace: string, args: string[]): string {
	return ['orkctl', '--namespace', shellQuote(namespace || 'default'), ...args.map(shellQuote)].join(' ');
}

/**
 * The command that runs one task of this run on the reader's own machine,
 * against this run's recorded bytes (`orkctl task run`). A local tool: the
 * browser never runs it (docs/UI_CLI_PARITY.md, local-tool exception).
 */
export function taskRunCommand(namespace: string, runId: string, taskId: string): string {
	return runCommand(namespace, ['task', 'run', '--from-run', runId, '--task', taskId]);
}

/** Payloads are compact JSON values; absent means the API has no inline value. */
export function payloadJson(payload: Payload | undefined): string {
	return JSON.stringify(payload?.data ?? {}, null, 2);
}

/** One line of JSON, spaced the way the pretty form is: `{"exitCode": 0, "ok": true}`. */
function oneLine(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(oneLine).join(', ')}]`;
	if (value !== null && typeof value === 'object') {
		return `{${Object.entries(value as Record<string, unknown>)
			.map(([k, v]) => `${JSON.stringify(k)}: ${oneLine(v)}`)
			.join(', ')}}`;
	}
	return JSON.stringify(value) ?? 'null';
}

/** The longest value a task row states inline, in characters; anything longer keeps its own block. */
export const INLINE_VALUES_MAX = 64;

/**
 * A payload short enough to read on its row — one line of JSON of at most
 * INLINE_VALUES_MAX characters — or null when it needs its own block.
 */
export function inlinePayload(payload: Payload | undefined): string | null {
	if (!payload) return null;
	const line = oneLine(payload.data ?? {});
	return line.length <= INLINE_VALUES_MAX ? line : null;
}

/** A payload with nothing in it: absent, or an empty object. */
export function emptyPayload(payload: Payload | undefined): boolean {
	const data = payload?.data;
	return data === undefined || data === null || (typeof data === 'object' && !Array.isArray(data) && Object.keys(data).length === 0);
}

/** Artifact references are metadata only. Their storage URI is never a browser download URL. */
export function artifactMetadata(artifact: ArtifactRef): Record<string, unknown> {
	return {
		name: artifact.name,
		contentType: artifact.contentType,
		sizeBytes: artifact.sizeBytes.toString(),
		sha256: artifact.sha256,
		metadata: artifact.metadata
	};
}

export function formatDuration(start: { seconds: bigint; nanos: number } | undefined, end: { seconds: bigint; nanos: number } | undefined): string {
	if (!start || !end) return '—';
	const milliseconds = Number(end.seconds - start.seconds) * 1000 + Math.floor((end.nanos - start.nanos) / 1e6);
	if (!Number.isFinite(milliseconds) || milliseconds < 0) return '—';
	if (milliseconds < 1000) return `${milliseconds}ms`;
	const seconds = milliseconds / 1000;
	if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
	const minutes = Math.floor(seconds / 60);
	return `${minutes}m ${Math.floor(seconds % 60)}s`;
}

/** Serialize protobuf timestamps before JSON export; JSON.stringify cannot encode bigint seconds. */
export function timestampJson(timestamp: { seconds: bigint; nanos: number } | undefined): string | undefined {
	if (!timestamp) return undefined;
	const milliseconds = Number(timestamp.seconds) * 1000 + Math.floor(timestamp.nanos / 1e6);
	return Number.isFinite(milliseconds) ? new Date(milliseconds).toISOString() : undefined;
}

// pinAccents are the identity colours a pin's member cards are tinted with.
//
// They are their OWN token family and deliberately not the status palette:
// `--color-succeeded-*`, `--color-failed-*`, `--color-canceled-*` and
// `--color-running-*` already mean an outcome on the very card being tinted,
// so borrowing green or red for "these ran together" would say something about
// how the task ended. A pin is an identity, not a state.
export const PIN_ACCENTS = ['pin-a', 'pin-b', 'pin-c', 'pin-d', 'pin-e', 'pin-f'] as const;

/**
 * pinAccent maps a pin name onto one of a small set of accent tokens.
 *
 * The mapping is a hash of the NAME and not the pin's position in the task
 * list, so a pin keeps its colour while a run streams and tasks arrive in
 * whatever order the engine reports them — a positional palette would recolour
 * every card each time a new pin appeared. The colour is redundant with the
 * `Pin <name>` label it sits beside: with six accents a run with seven pins
 * has a collision, which costs a reader nothing because the name is written
 * out. It is never the only way to tell two pins apart.
 *
 * An empty pin has no accent: an unpinned task is not a group of one.
 */
export function pinAccent(pin: string): string | undefined {
	if (!pin) return undefined;
	// FNV-1a, 32-bit, kept in a Math.imul so the multiply stays integral.
	let hash = 0x811c9dc5;
	for (let i = 0; i < pin.length; i++) {
		hash = Math.imul(hash ^ pin.charCodeAt(i), 0x01000193);
	}
	return PIN_ACCENTS[(hash >>> 0) % PIN_ACCENTS.length];
}

/** Storage names are data, not trusted response headers. Keep a download in the user's directory. */
export function downloadFilename(name: string): string {
	const safe = name.replace(/[\\/\u0000-\u001f\u007f]/g, '_').trim();
	return safe || 'artifact';
}
