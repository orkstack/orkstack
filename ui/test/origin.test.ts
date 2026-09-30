import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import {
	ORIGIN_KIND_SLUGS,
	UNRECORDED,
	originKindFromSlug,
	originKindSlug,
	originLabel,
	originLinkPath,
	originReferenceURL
} from '../src/lib/origin';
import { runFilterError } from '../src/lib/browsing';
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
		expect(originKindFromSlug(slug)).toBe(value.number);
	}
	expect(originKindSlug(RunOriginKind.PARENT_RUN)).toBe('parent-run');
	expect(ORIGIN_KIND_SLUGS).toEqual(['submit', 'trigger', 'schedule', 'parent-run', 'run-again']);
	for (const bad of ['', 'unspecified', 'Trigger', 'parent_run', 'bogus']) {
		expect(originKindFromSlug(bad)).toBe(RunOriginKind.UNSPECIFIED);
	}
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

test('the runs filter refuses an unknown origin kind and an id without a kind', () => {
	expect(runFilterError('', '', '', 'bogus', '')).toContain('Unknown origin kind');
	expect(runFilterError('', '', '', '', 'trg-1')).toBe('An origin id needs an origin kind.');
	expect(runFilterError('', '', '', 'trigger', 'trg-1')).toBeNull();
});
