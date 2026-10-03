// The run evidence bundle, as the browser reads it (docs/RUN_EVIDENCE.md).
//
// The engine streams MANIFEST.json alone for `manifestOnly`, which is what the
// Evidence dialog shows before any bundle byte is fetched; these helpers are
// the pure half of that dialog. They mirror internal/evidence and
// `orkctl run evidence`: the same manifest, the same reason sentences, the
// same command.
import { formatBytes } from './live';
import { downloadFilename, runCommand } from './runDetail';

/**
 * A browser assembles a download in memory before saving it, so a bundle
 * larger than this is handed to the CLI instead. A client constant, on the
 * same terms as LIVE_POLL_MS: no server setting decides it.
 */
export const EVIDENCE_BROWSER_MAX_BYTES = 512 * 1024 * 1024;

export type EvidenceState = 'present' | 'reclaimed' | 'unrecorded' | 'rejected';

export interface EvidenceMember {
	path: string;
	kind: string;
	task_id?: string;
	attempt?: number;
	name?: string;
	origin: string;
	size_bytes: number;
	sha256: string;
	state: EvidenceState | string;
	reason?: string;
}

export interface EvidenceAttempt {
	attempt: number;
	state: EvidenceState | string;
	outcome?: string;
	commit_adopted?: boolean;
}

export interface EvidenceTask {
	task_id: string;
	status: string;
	outcome_attempt: number;
	exit_code: number;
	attempts: EvidenceAttempt[];
}

/** MANIFEST.json, snake_case as the wire carries it. */
export interface Manifest {
	schema_version: number;
	run_id: string;
	namespace: string;
	orkflow_name: string;
	orkflow_version: string;
	definition_digest: string;
	run_status: string;
	run_created_at: string;
	run_finished_at: string;
	exported_at: string;
	engine_version: string;
	build_id: string;
	scope: { task_id: string; logs: boolean; artifacts: boolean };
	definition: { state: EvidenceState | string; reason: string; stored_definition_digest: string };
	tasks: EvidenceTask[];
	members: EvidenceMember[];
	counts: { present: number; reclaimed: number; unrecorded: number; rejected: number; bytes_present: number };
}

/** The only manifest schema this client reads. */
export const EVIDENCE_SCHEMA_VERSION = 1;

/** Parses MANIFEST.json bytes; a schema this client does not know is refused. */
export function parseManifest(bytes: Uint8Array): Manifest {
	const manifest = JSON.parse(new TextDecoder().decode(bytes)) as Manifest;
	if (manifest?.schema_version !== EVIDENCE_SCHEMA_VERSION) {
		throw new Error(
			`MANIFEST.json schema_version ${String(manifest?.schema_version)} is not supported (this client reads ${EVIDENCE_SCHEMA_VERSION})`
		);
	}
	return manifest;
}

/** "41 present · 2 reclaimed · 1 unrecorded · 44.1 MiB to download"; zero counts are omitted. */
export function evidenceSummary(m: Manifest): string {
	const parts = [`${m.counts.present} present`];
	if (m.counts.reclaimed) parts.push(`${m.counts.reclaimed} reclaimed`);
	if (m.counts.unrecorded) parts.push(`${m.counts.unrecorded} unrecorded`);
	if (m.counts.rejected) parts.push(`${m.counts.rejected} rejected`);
	parts.push(`${formatBytes(m.counts.bytes_present)} to download`);
	return parts.join(' · ');
}

const REASONS: Record<string, string> = {
	definition_deleted:
		"the pinned definition was deleted from the orkflow store; the run's definition digest still attests it",
	definition_digest_mismatch:
		'a document exists at the pinned coordinates but does not hash to the digest this run pinned',
	commit_missing: 'the commit record was reclaimed by cleanup; its recorded digest is preserved',
	object_missing:
		'the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved',
	attempt_record_missing: 'no record: the orker died before filing one, or it was reclaimed'
};

/** The documented sentence for a reason token; an unknown token is itself. */
export function reasonText(token: string): string {
	return REASONS[token] ?? token;
}

/** Every member the bundle will not carry, with why. */
export function absentMembers(m: Manifest): { path: string; state: string; reason: string }[] {
	return m.members
		.filter((member) => member.state !== 'present')
		.map((member) => ({
			path: member.path,
			state: member.state,
			reason: reasonText(member.reason ?? '')
		}));
}

/** The CLI that writes the same bundle the dialog downloads. */
export function evidenceCommand(
	namespace: string,
	runId: string,
	o: { logs: boolean; artifacts: boolean }
): string {
	return runCommand(namespace, [
		'run',
		'evidence',
		'export',
		runId,
		'--output',
		`${runId}.evidence.tar.gz`,
		...(o.logs ? [] : ['--no-logs']),
		...(o.artifacts ? [] : ['--no-artifacts'])
	]);
}

/** The saved file's name, sanitised as every browser download is. */
export function evidenceFilename(runId: string): string {
	return downloadFilename(`${runId}.evidence.tar.gz`);
}

/** The definition line: "Definition sha256:… · source.yaml present". */
export function definitionLine(m: Manifest): string {
	const digest = m.definition_digest || 'no digest recorded';
	switch (m.definition.state) {
		case 'present':
			return `Definition ${digest} · source.yaml present`;
		case 'reclaimed':
			return `Definition ${digest} · source.yaml reclaimed: definition deleted`;
		case 'rejected':
			return `Definition ${digest} · source.yaml rejected: stored document hashes to ${m.definition.stored_definition_digest}`;
		default:
			return `Definition ${digest} · source.yaml ${m.definition.state}`;
	}
}
