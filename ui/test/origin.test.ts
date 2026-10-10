import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import {
	UNRECORDED,
	originKindSlug,
	originLabel,
	originLinkPath,
	originReferenceURL
} from '../src/lib/origin';
import { FilterOperator, specFor } from '../src/lib/listFilter';
import { ListRunsRequestSchema } from '../src/lib/gen/orkstack/v1/service_pb';
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
	expect(originReferenceURL('https://ci.example/jobs/1')).toBe('https://ci.example/jobs/1');
	expect(originReferenceURL('http://ci.example/x')).toBe('http://ci.example/x');
	expect(originReferenceURL('INC-4412')).toBeNull();
	expect(originReferenceURL('javascript:alert(1)')).toBeNull();
	expect(originReferenceURL('ftp://files.example/x')).toBeNull();
	expect(originReferenceURL('')).toBeNull();
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

test('the runs list filters by origin kind, and by origin id only within a kind', () => {
	const fields = specFor(ListRunsRequestSchema).fields;
	const kind = fields.find((f) => f.path === 'origin.kind');
	const id = fields.find((f) => f.path === 'origin.id');
	expect(kind?.kind).toBe('enum');
	expect(kind?.operators).toEqual([FilterOperator.EQUALS]);
	expect(kind?.values).toEqual(['SUBMIT', 'TRIGGER', 'SCHEDULE', 'PARENT_RUN', 'RUN_AGAIN']);
	expect(id?.operators).toEqual([FilterOperator.EQUALS]);
	expect(id?.requires).toBe('origin.kind');
});
