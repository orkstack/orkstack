import type { ArtifactRef, Payload } from './gen/orkstack/v1/common_pb';

/** Quote one CLI argument for a POSIX shell without trusting page data as code. */
export function shellQuote(value: string): string {
	return `'${value.replaceAll("'", "'\"'\"'")}'`;
}

/** A copyable command always states the namespace, including `default`. */
export function runCommand(namespace: string, args: string[]): string {
	return ['orkctl', '--namespace', shellQuote(namespace || 'default'), ...args.map(shellQuote)].join(' ');
}

/** Payloads are compact JSON values; absent means the API has no inline value. */
export function payloadJson(payload: Payload | undefined): string {
	return JSON.stringify(payload?.data ?? {}, null, 2);
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
const pinAccents = ['pin-a', 'pin-b', 'pin-c', 'pin-d', 'pin-e', 'pin-f'] as const;

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
	return pinAccents[(hash >>> 0) % pinAccents.length];
}

/** Storage names are data, not trusted response headers. Keep a download in the user's directory. */
export function downloadFilename(name: string): string {
	const safe = name.replace(/[\\/\u0000-\u001f\u007f]/g, '_').trim();
	return safe || 'artifact';
}
