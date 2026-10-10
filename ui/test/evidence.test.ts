import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import {
	EVIDENCE_BROWSER_MAX_BYTES,
	absentMembers,
	collectEvidence,
	evidenceBlob,
	evidenceTooLarge,
	definitionLine,
	evidenceCommand,
	evidenceFilename,
	evidenceSummary,
	parseManifest,
	reasonText,
	type Manifest
} from '../src/lib/evidence';
import { runCommand } from '../src/lib/runDetail';

// The golden manifest from internal/evidence: the exact document the engine writes.
const golden = readFileSync(`${import.meta.dir}/../../internal/evidence/testdata/manifest.golden.json`, 'utf8');
const goldenBytes = new TextEncoder().encode(golden);

function withCounts(m: Manifest, counts: Partial<Manifest['counts']>): Manifest {
	return { ...m, counts: { ...m.counts, ...counts } };
}

test('U1 parseManifest accepts the engine golden and refuses another schema', () => {
	const m = parseManifest(goldenBytes);
	expect(m.schema_version).toBe(1);
	expect(m.run_id).toBe('ork-20260910-1');
	expect(m.members.length).toBe(13);
	const next = new TextEncoder().encode(golden.replace('"schema_version": 1', '"schema_version": 2'));
	let refusal = '';
	try {
		parseManifest(next);
	} catch (err) {
		refusal = String(err);
	}
	expect(refusal).toContain('schema_version 2');
});

test('U1 evidenceSummary counts, sizes and omits zeros', () => {
	const m = parseManifest(goldenBytes);
	expect(
		evidenceSummary(withCounts(m, { present: 41, reclaimed: 2, unrecorded: 1, rejected: 0, bytes_present: 46_242_201 }))
	).toBe('41 present · 2 reclaimed · 1 unrecorded · 44.1 MiB to download');
	expect(evidenceSummary(withCounts(m, { present: 3, reclaimed: 0, unrecorded: 0, rejected: 0, bytes_present: 900 }))).toBe(
		'3 present · 900 B to download'
	);
	expect(EVIDENCE_BROWSER_MAX_BYTES).toBe(256 * 1024 * 1024);
});

test('U2 absentMembers lists every non-present member with its sentence', () => {
	const m = parseManifest(goldenBytes);
	const extra: Manifest = {
		...m,
		members: [
			...m.members,
			{ path: 'orkflow/source.yaml', kind: 'source', origin: 'x', size_bytes: 0, sha256: '', state: 'rejected', reason: 'definition_digest_mismatch' },
			{ path: 'tasks/x/commit.json', kind: 'commit', origin: 'y', size_bytes: 0, sha256: 'ab', state: 'reclaimed', reason: 'brand_new_token' }
		]
	};
	expect(absentMembers(extra)).toEqual([
		{ path: 'tasks/reconcile/attempts/1/stderr.log', state: 'reclaimed', reason: reasonText('object_missing') },
		{ path: 'tasks/extract/attempts/2.json', state: 'unrecorded', reason: reasonText('attempt_record_missing') },
		{ path: 'orkflow/source.yaml', state: 'rejected', reason: reasonText('definition_digest_mismatch') },
		{ path: 'tasks/x/commit.json', state: 'reclaimed', reason: 'brand_new_token' }
	]);
	expect(reasonText('object_missing')).toBe(
		'the object was removed from the store by a lifecycle rule or cleanup; its recorded digest is preserved'
	);
	expect(reasonText('unknown_token')).toBe('unknown_token');
	expect(definitionLine(m)).toBe(`Definition ${m.definition_digest} · source.yaml present`);
});

test('U3 evidenceCommand is the CLI that writes the same bundle', () => {
	expect(evidenceCommand('default', 'ork-1', { logs: false, artifacts: true })).toBe(
		runCommand('default', ['run', 'evidence', 'export', 'ork-1', '--output', 'ork-1.evidence.tar.gz', '--no-logs'])
	);
	expect(evidenceCommand('', 'ork-1', { logs: true, artifacts: false })).toBe(
		"orkctl --namespace 'default' 'run' 'evidence' 'export' 'ork-1' '--output' 'ork-1.evidence.tar.gz' '--no-artifacts'"
	);
});

test('U4 evidenceFilename sanitises through downloadFilename', () => {
	expect(evidenceFilename('ork-1')).toBe('ork-1.evidence.tar.gz');
	expect(evidenceFilename('a/b\\c')).toBe('a_b_c.evidence.tar.gz');
});

async function* stream(chunks: { data: Uint8Array; last: boolean }[]) {
	for (const chunk of chunks) yield chunk;
}

// F-ui-ux-5: the download is assembled with no per-chunk copy, and the Blob
// is the one copy left once it exists.
test('U9 the bundle is assembled without copying a chunk, byte for byte', async () => {
	const chunks = [
		{ data: new Uint8Array([1, 2, 3]), last: false },
		{ data: new Uint8Array(0), last: false },
		{ data: new Uint8Array([4, 5]), last: false },
		{ data: new Uint8Array([6]), last: true }
	];
	const original = Uint8Array.prototype.slice;
	let slices = 0;
	Uint8Array.prototype.slice = function (this: Uint8Array, ...args: [number?, number?]) {
		slices++;
		return original.apply(this, args);
	} as typeof original;
	let parts: Uint8Array[];
	let blob: Blob;
	try {
		parts = await collectEvidence(stream(chunks), false);
		// Kept as delivered: the same buffers, not copies of them.
		expect(parts.length).toBe(3);
		expect(parts[0] === chunks[0].data).toBe(true);
		expect(parts[2] === chunks[3].data).toBe(true);
		blob = evidenceBlob(parts);
	} finally {
		Uint8Array.prototype.slice = original;
	}
	expect(slices).toBe(0);
	// The parts are released once the Blob holds the bytes.
	expect(parts.length).toBe(0);
	expect(blob.type).toBe('application/gzip');
	expect(Array.from(new Uint8Array(await blob.arrayBuffer()))).toEqual([1, 2, 3, 4, 5, 6]);
});

test('U9 a stream that never says last is refused, never assembled', async () => {
	let refusal = '';
	try {
		await collectEvidence(stream([{ data: new Uint8Array([1]), last: false }]), false);
	} catch (err) {
		refusal = String(err);
	}
	expect(refusal).toContain('before the engine confirmed a complete bundle');
});

test('U10 an over-limit bundle names its size, the limit and the CLI that exports it', () => {
	const m = parseManifest(goldenBytes);
	expect(evidenceTooLarge(withCounts(m, { bytes_present: EVIDENCE_BROWSER_MAX_BYTES }), 'payments', 'ork-1', { logs: true, artifacts: true })).toBeNull();
	const over = evidenceTooLarge(withCounts(m, { bytes_present: 300 * 1024 * 1024 }), 'payments', 'ork-1', {
		logs: false,
		artifacts: true
	});
	expect(over?.message).toBe(
		'This bundle is 300.0 MiB, more than the 256.0 MiB a browser download assembles in memory. Export it with the CLI:'
	);
	expect(over?.command).toBe(evidenceCommand('payments', 'ork-1', { logs: false, artifacts: true }));
	expect(over?.command).toBe("orkctl --namespace 'payments' 'run' 'evidence' 'export' 'ork-1' '--output' 'ork-1.evidence.tar.gz' '--no-logs'");
});
