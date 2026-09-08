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

/** Storage names are data, not trusted response headers. Keep a download in the user's directory. */
export function downloadFilename(name: string): string {
	const safe = name.replace(/[\\/\u0000-\u001f\u007f]/g, '_').trim();
	return safe || 'artifact';
}
