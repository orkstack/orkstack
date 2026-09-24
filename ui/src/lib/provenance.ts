// Artifact provenance: the wordings and command the run page's Provenance
// disclosure renders. They are the CLI's (`orkctl run artifact provenance`,
// cmd/orkctl/provenance.go) word for word, so a screenshot and a golden file
// describe one answer.
import type { Timestamp } from '@bufbuild/protobuf/wkt';
import { ArtifactKind, ReferenceState } from './gen/orkstack/v1/service_pb';
import { runCommand } from './runDetail';

export const BYTES_PRESENT = 'Bytes present in the object store';
export const BYTES_MISSING = 'Bytes MISSING from the object store';
export const NO_REFERENCES = 'No archived run of this namespace references this object.';

/** The --kind value the CLI takes for a recorded artifact kind. */
export function artifactKindFlag(kind: ArtifactKind): string {
	switch (kind) {
		case ArtifactKind.OUTPUT:
			return 'output';
		case ArtifactKind.VALUES:
			return 'values';
		case ArtifactKind.STDOUT:
			return 'stdout';
		case ArtifactKind.STDERR:
			return 'stderr';
		default:
			return 'unspecified';
	}
}

/** RFC 3339 in UTC to the second — the CLI's rendering of the same instant. */
export function rfc3339(ts: Timestamp | undefined): string {
	if (!ts) return '—';
	const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6);
	return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z');
}

/**
 * The addressed run's own standing in the object index, in the CLI's three
 * wordings. ABSENT is "no reference recorded" — never rendered as released or
 * held, because the server does not know which of its causes applies.
 */
export function referenceStateText(state: ReferenceState, releasedAt: Timestamp | undefined): string {
	switch (state) {
		case ReferenceState.HELD:
			return "held — this run's archive holds its marker on the object";
		case ReferenceState.RELEASED:
			return `released — cleanup released this run's object claims at ${rfc3339(releasedAt)}`;
		default:
			return 'absent — no marker and no cleanup record for this run (not archived yet, or archived before the object index existed)';
	}
}

/** The footer for markers whose run body is not in the archive; empty for none. */
export function unreadableText(count: bigint | number): string {
	const n = Number(count);
	if (n <= 0) return '';
	if (n === 1) return '1 marker could not be resolved: its run body is not in the archive';
	return `${n} markers could not be resolved: their run bodies are not in the archive`;
}

/** The copyable command that asks the same question from a terminal. */
export function provenanceCommand(namespace: string, runId: string, taskId: string, kind: ArtifactKind, name: string): string {
	return runCommand(namespace, ['run', 'artifact', 'provenance', runId, '--task', taskId, '--kind', artifactKindFlag(kind), '--name', name]);
}
