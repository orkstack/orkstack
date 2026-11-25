import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import {
	UNRECORDED,
	originKindSlug,
	originLabel,
	originLinkPath,
	originParentTask,
	originReferenceURL,
	originRoot,
	originRootRunPath
} from '../src/lib/origin';
import { FilterOperator, specFor } from '../src/lib/listFilter';
import { ListRunsRequestSchema } from '../src/lib/gen/orkstack/v1/run_service_pb';
import { RunOriginKind, RunOriginKindSchema, RunOriginSchema } from '../src/lib/gen/orkstack/v1/run_pb';

// The descriptor walk: a kind added to the proto without a slug fails here,
// exactly as it fails in pkg/artifacts and cmd/orkctl.
test('every declared origin kind has a slug that round-trips', () => {
	for (const value of RunOriginKindSchema.values) {
		const slug = originKindSlug(value.number);
		if (value.number === RunOriginKind.UNSPECIFIED) {
			expect(slug).toBe('');
			continue;
		}
		const want = value.name.replace(/^RUN_ORIGIN_KIND_/, '').toLowerCase().replaceAll('_', '-');
		expect(slug).toBe(want);
	}
	expect(originKindSlug(RunOriginKind.PARENT_RUN)).toBe('parent-run');
	expect(originKindSlug(42 as RunOriginKind)).toBe('');
});

test('a run with no origin is unrecorded, never submit', () => {
	expect(originLabel(undefined)).toBe(UNRECORDED);
	expect(originLabel(create(RunOriginSchema, {}))).toBe(UNRECORDED);
	expect(originLabel(create(RunOriginSchema, { kind: RunOriginKind.SUBMIT }))).toBe('submit');
	expect(originLabel(create(RunOriginSchema, { kind: RunOriginKind.TRIGGER, id: 'trg-1' }))).toBe('trigger trg-1');
});

test('a reference is a link only when it provably is an http(s) URL', () => {
	const of = (reference: string, kind = RunOriginKind.SUBMIT) => originReferenceURL(create(RunOriginSchema, { kind, reference }));
	expect(of('https://ci.example/jobs/1')).toBe('https://ci.example/jobs/1');
	expect(of('http://ci.example/x')).toBe('http://ci.example/x');
	expect(of('https://ci.example/x', RunOriginKind.TRIGGER)).toBe('https://ci.example/x');
	expect(of('https://ci.example/x', RunOriginKind.RUN_AGAIN)).toBe('https://ci.example/x');
	expect(of('INC-4412')).toBeNull();
	expect(of('javascript:alert(1)')).toBeNull();
	expect(of('ftp://files.example/x')).toBeNull();
	expect(of('')).toBeNull();
	expect(originReferenceURL(undefined)).toBeNull();
});

test('a publisher’s text is never a link, whatever it looks like', () => {
	const url = 'https://ci.example/jobs/1';
	for (const kind of [RunOriginKind.EVENT_SOURCE, RunOriginKind.SCHEDULE, RunOriginKind.PARENT_RUN, RunOriginKind.UNSPECIFIED, 42 as RunOriginKind]) {
		expect(originReferenceURL(create(RunOriginSchema, { kind, reference: url }))).toBeNull();
	}
});

test('an origin links to the Triggers or Schedules tab or to the source run', () => {
	expect(originLinkPath(create(RunOriginSchema, { kind: RunOriginKind.TRIGGER, id: 'trg-1' }), 'deploy')).toBe(
		'/orkflows/deploy?tab=triggers'
	);
	expect(originLinkPath(create(RunOriginSchema, { kind: RunOriginKind.RUN_AGAIN, id: 'ork-1' }), 'deploy')).toBe('/runs/ork-1');
	expect(originLinkPath(create(RunOriginSchema, { kind: RunOriginKind.PARENT_RUN, id: 'ork-p' }), 'deploy')).toBe('/runs/ork-p');
	expect(originLinkPath(create(RunOriginSchema, { kind: RunOriginKind.SCHEDULE, id: 'sch-1' }), 'deploy')).toBe(
		'/orkflows/deploy?tab=schedules'
	);
	expect(originLinkPath(create(RunOriginSchema, { kind: RunOriginKind.SUBMIT }), 'deploy')).toBeNull();
});

test('the root run links only when it is not the parent the row already links', () => {
	const child = (id: string, rootRunId: string) =>
		create(RunOriginSchema, { kind: RunOriginKind.PARENT_RUN, id, rootRunId });
	expect(originRootRunPath(child('ork-mid', 'ork-root'))).toBe('/runs/ork-root');
	expect(originRootRunPath(child('ork-root', 'ork-root'))).toBeNull();
	// A child recorded before the field, a top-level run, and no origin at all.
	expect(originRootRunPath(child('ork-mid', ''))).toBeNull();
	expect(originRootRunPath(create(RunOriginSchema, { kind: RunOriginKind.SUBMIT }))).toBeNull();
	expect(originRootRunPath(undefined)).toBeNull();
	expect(originRootRunPath(child('ork-mid', 'ork/a b'))).toBe('/runs/ork%2Fa%20b');
});

test('a child run names its parent task, and its root where that adds something', () => {
	const child = create(RunOriginSchema, {
		kind: RunOriginKind.PARENT_RUN, id: 'ork-mid', rootRunId: 'ork-root', parentTaskId: 'ship',
		parentOrkflow: { name: 'deploy', version: '2.1.0', namespace: 'platform' },
		rootOrkflow: { name: 'release', version: '1.0.0' },
		rootOrigin: { kind: RunOriginKind.TRIGGER, id: 'trg-1' }
	});
	expect(originParentTask(child)).toEqual({ task: 'ship', orkflow: 'platform/deploy@2.1.0' });
	expect(originRoot(child)).toEqual({ orkflow: 'release@1.0.0', startedBy: 'trigger trg-1' });
	// Its parent is its root: the Orkflow is already named once.
	child.rootRunId = 'ork-mid';
	expect(originRoot(child)).toEqual({ orkflow: '', startedBy: 'trigger trg-1' });
	for (const none of [undefined, create(RunOriginSchema, { kind: RunOriginKind.SUBMIT })]) {
		expect(originParentTask(none)).toBeNull();
		expect(originRoot(none)).toEqual({ orkflow: '', startedBy: '' });
	}
});

test('the runs list filters by origin kind, and by origin id only within a kind', () => {
	const fields = specFor(ListRunsRequestSchema).fields;
	const kind = fields.find((f) => f.path === 'origin.kind');
	const id = fields.find((f) => f.path === 'origin.id');
	expect(kind?.kind).toBe('enum');
	expect(kind?.operators).toEqual([FilterOperator.EQUALS]);
	expect(kind?.values).toEqual(['SUBMIT', 'TRIGGER', 'SCHEDULE', 'PARENT_RUN', 'RUN_AGAIN', 'EVENT_SOURCE']);
	expect(id?.operators).toEqual([FilterOperator.EQUALS]);
	expect(id?.requires).toBe('origin.kind');
});
