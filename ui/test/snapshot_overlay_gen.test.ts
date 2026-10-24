import { expect, test } from 'bun:test';
import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { RunSnapshotOverlaySchema } from '../src/lib/gen/orkstack/v1/continuation_pb';
import { RunSchema, TaskRunSchema } from '../src/lib/gen/orkstack/v1/run_pb';

// The generated client of orkstack.v1.RunSnapshotOverlay (Stage 6) keeps the
// proto's field numbers: no RPC carries the message, but the TypeScript
// client is generated from the same proto as the engine, and a drift between
// them would surface here first.
test('RunSnapshotOverlay keeps its field numbers and round-trips', () => {
	const fields = Object.fromEntries(RunSnapshotOverlaySchema.fields.map((f) => [f.name, f.number]));
	expect(fields).toEqual({ base_uri: 1, header: 2, task_runs: 3, items: 4, base_sha256: 5 });
	const overlay = create(RunSnapshotOverlaySchema, {
		baseUri: 's3://bucket/metadata/snapshots/default/ork-1/12.pb',
		baseSha256: 'a'.repeat(64),
		header: create(RunSchema, { id: 'ork-1', namespace: 'default' }),
		taskRuns: [create(TaskRunSchema, { taskId: 'fan' })],
		items: [create(TaskRunSchema, { taskId: 'fan', item: 3 })]
	});
	const back = fromBinary(RunSnapshotOverlaySchema, toBinary(RunSnapshotOverlaySchema, overlay));
	expect(back.baseUri).toBe(overlay.baseUri);
	expect(back.baseSha256).toBe(overlay.baseSha256);
	expect(back.header?.id).toBe('ork-1');
	expect(back.items[0].item).toBe(3);
});
