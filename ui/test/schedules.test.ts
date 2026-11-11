import { expect, test } from 'bun:test';
import { PayloadSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import { create } from '@bufbuild/protobuf';
import { DurationSchema, TimestampSchema, timestampFromDate } from '@bufbuild/protobuf/wkt';
import {
	EMPTY_SCHEDULE_FORM,
	OVERLAP_POLICY_SLUGS,
	type ScheduleForm,
	durationLabel,
	fireOutcome,
	isoUTC,
	overlapPolicyFromSlug,
	overlapPolicySlug,
	parseDuration,
	scheduleCreateCli,
	scheduleDeleteCli,
	scheduleFireCli,
	scheduleFormOf,
	schedulePauseCli,
	scheduleResumeCli,
	scheduleState,
	scheduleUpdateCli,
	scheduleWrite,
	shellArg,
	whenLabel
} from '../src/lib/schedules';
import {
	OverlapPolicy,
	OverlapPolicySchema,
	ScheduleFireSchema,
	ScheduleSchema,
	ScheduleSpecSchema,
	type Schedule
} from '../src/lib/gen/orkstack/v1/schedule_pb';

// The descriptor walk: a policy added to the proto without a slug fails here,
// exactly as it fails in cmd/orkctl.
test('every declared overlap policy has a slug that round-trips', () => {
	for (const value of OverlapPolicySchema.values) {
		const slug = overlapPolicySlug(value.number);
		if (value.number === OverlapPolicy.UNSPECIFIED) {
			expect(slug).toBe('');
			continue;
		}
		const want = value.name.replace(/^OVERLAP_POLICY_/, '').toLowerCase().replaceAll('_', '-');
		expect(slug).toBe(want);
		expect(overlapPolicyFromSlug(slug)).toBe(value.number);
		expect(value.name).not.toContain('TERMINATE');
	}
	expect(OVERLAP_POLICY_SLUGS).toEqual(['skip', 'buffer-one', 'buffer-all', 'cancel-other', 'allow-all']);
	expect(overlapPolicySlug(5 as OverlapPolicy)).toBe('');
	for (const bad of ['', 'unspecified', 'terminate-other', 'Skip', 'buffer_one']) {
		expect(overlapPolicyFromSlug(bad)).toBe(OverlapPolicy.UNSPECIFIED);
	}
	expect(OverlapPolicy.ALLOW_ALL as number).toBe(6);
});

test('durations parse and print the way an operator types them', () => {
	for (const [text, seconds] of [
		['30s', 30],
		['15m', 900],
		['1h', 3600],
		['1h30m', 5400]
	] as const) {
		const parsed = parseDuration('spec.interval', text);
		expect('value' in parsed && Number(parsed.value?.seconds)).toBe(seconds);
		expect(durationLabel(create(DurationSchema, { seconds: BigInt(seconds) }))).toBe(text);
	}
	expect(parseDuration('spec.jitter', '')).toEqual({ value: undefined });
	const bad = parseDuration('spec.jitter', 'soon');
	expect('error' in bad && bad.error).toContain('spec.jitter');
	expect('error' in parseDuration('x', '5 minutes')).toBe(true);
});

test('WHEN reads as the CLI spells it', () => {
	expect(whenLabel(create(ScheduleSpecSchema, { cron: ['0 2 * * *'], timezone: 'Europe/Lisbon' }))).toBe(
		'0 2 * * * (Europe/Lisbon)'
	);
	expect(whenLabel(create(ScheduleSpecSchema, { interval: create(DurationSchema, { seconds: 3600n }) }))).toBe('every 1h');
	expect(
		whenLabel(create(ScheduleSpecSchema, { cron: ['@daily', '@hourly'], interval: create(DurationSchema, { seconds: 900n }) }))
	).toBe('@daily | @hourly | every 15m');
});

test('the create command recreates the row, quoting only what needs it', () => {
	const schedule = create(ScheduleSchema, {
		name: 'nightly',
		namespace: 'payments',
		orkflowName: 'hello',
		paused: true,
		spec: create(ScheduleSpecSchema, { cron: ['0 2 * * *'], timezone: 'Europe/Lisbon', overlap: OverlapPolicy.SKIP })
	});
	expect(scheduleCreateCli(schedule)).toBe(
		"orkctl schedule create nightly --orkflow hello --cron '0 2 * * *' --timezone Europe/Lisbon --overlap skip --paused -n payments"
	);
	schedule.description = "Bob's nightly";
	schedule.orkflowVersion = '3';
	expect(scheduleCreateCli(schedule)).toContain("--version 3 --cron '0 2 * * *'");
	expect(scheduleCreateCli(schedule)).toContain(`--description 'Bob'"'"'s nightly'`);
	expect(shellArg('$(rm -rf /)')).toBe("'$(rm -rf /)'");
});

test('one-liners: fire sends no override, so its command carries no --overlap', () => {
	expect(scheduleFireCli('nightly', '')).toBe('orkctl schedule fire nightly -n default');
	expect(scheduleFireCli('nightly', 'payments')).not.toContain('--overlap');
	expect(schedulePauseCli('nightly', 'payments', 'Q3 close')).toBe(
		"orkctl schedule pause nightly --reason 'Q3 close' -n payments"
	);
	expect(scheduleResumeCli('nightly', 'payments')).toBe('orkctl schedule resume nightly -n payments');
	expect(scheduleDeleteCli('nightly', 'payments')).toBe('orkctl schedule delete nightly -n payments');
});

test('a fire reads as a run, refused, preparing, or unavailable — never blank', () => {
	const at = timestampFromDate(new Date('2026-09-24T01:00:00Z'));
	expect(fireOutcome(create(ScheduleFireSchema, { scheduledAt: at, runId: 'ork-1', detailsAvailable: true }))).toEqual({
		kind: 'run',
		runId: 'ork-1'
	});
	expect(fireOutcome(create(ScheduleFireSchema, { refusal: 'no live Orker', detailsAvailable: true }))).toEqual({
		kind: 'refused',
		refusal: 'no live Orker'
	});
	expect(fireOutcome(create(ScheduleFireSchema, { detailsAvailable: true }))).toEqual({ kind: 'preparing' });
	expect(fireOutcome(create(ScheduleFireSchema, { runId: 'ork-1', detailsAvailable: false }))).toEqual({ kind: 'unavailable' });
	expect(scheduleState(create(ScheduleSchema, { unavailableReason: 'x', paused: true }))).toBe('unavailable');
	expect(scheduleState(create(ScheduleSchema, { paused: true }))).toBe('paused');
	expect(scheduleState(create(ScheduleSchema, {}))).toBe('active');
});

function stored(): Schedule {
	return create(ScheduleSchema, {
		name: 'nightly',
		namespace: 'team-a',
		orkflowName: 'flow',
		orkflowVersion: 'v1',
		description: 'the nightly one',
		input: create(PayloadSchema, { data: { mode: 'full' } }),
		spec: create(ScheduleSpecSchema, {
			cron: ['0 2 * * *', '0 3 * * *'],
			interval: create(DurationSchema, { seconds: 1n, nanos: 500_000_000 }),
			timezone: 'Europe/Lisbon',
			jitter: create(DurationSchema, { seconds: 30n }),
			catchupWindow: create(DurationSchema, { seconds: 3600n }),
			startAt: timestampFromDate(new Date('2030-01-01T00:00:00.250Z')),
			endAt: timestampFromDate(new Date('2031-01-01T00:00:00Z')),
			overlap: OverlapPolicy.BUFFER_ONE,
			pauseOnFailure: true
		})
	});
}

test('the edit form reads every field a schedule carries', () => {
	expect(scheduleFormOf(stored())).toEqual({
		version: 'v1',
		cron: '0 2 * * *\n0 3 * * *',
		interval: '1s500ms',
		timezone: 'Europe/Lisbon',
		overlap: 'buffer-one',
		jitter: '30s',
		catchupWindow: '1h',
		startAt: '2030-01-01T00:00:00Z',
		endAt: '2031-01-01T00:00:00Z',
		description: 'the nightly one',
		input: '{\n  "mode": "full"\n}',
		pauseOnFailure: true
	});
});

test('saving an untouched form writes back exactly what was stored, sub-second values included', () => {
	const schedule = stored();
	const written = scheduleWrite(scheduleFormOf(schedule), schedule);
	if ('error' in written) throw new Error(written.error);
	expect(written.value.orkflowVersion).toBe('v1');
	expect(written.value.description).toBe('the nightly one');
	expect(written.value.input).toEqual(schedule.input);
	expect(written.value.spec).toEqual(schedule.spec!);
});

test('a changed field is read from the form, and the others stay as stored', () => {
	const schedule = stored();
	const written = scheduleWrite({ ...scheduleFormOf(schedule), cron: '@daily\n\n', jitter: '1m' }, schedule);
	if ('error' in written) throw new Error(written.error);
	expect(written.value.spec.cron).toEqual(['@daily']);
	expect(written.value.spec.jitter?.seconds).toBe(60n);
	expect(written.value.spec.interval).toEqual(schedule.spec!.interval);
	expect(written.value.spec.startAt).toEqual(schedule.spec!.startAt);
});

test('an emptied field clears: a pin, the input, a description, a time, a duration', () => {
	const schedule = stored();
	const written = scheduleWrite(
		{ ...scheduleFormOf(schedule), version: '', input: ' ', description: '', endAt: '', jitter: '' },
		schedule
	);
	if ('error' in written) throw new Error(written.error);
	expect(written.value.orkflowVersion).toBe('');
	expect(written.value.input).toBeUndefined();
	expect(written.value.description).toBe('');
	expect(written.value.spec.endAt).toBeUndefined();
	expect(written.value.spec.jitter).toBeUndefined();
});

test('a bad field is refused by name, and an input must be an object', () => {
	const schedule = stored();
	const refusal = (patch: Partial<ScheduleForm>) => {
		const written = scheduleWrite({ ...scheduleFormOf(schedule), ...patch }, schedule);
		return 'error' in written ? written.error : '';
	};
	expect(refusal({ interval: 'soon' })).toContain('Interval');
	expect(refusal({ catchupWindow: 'soon' })).toContain('Catch-up window');
	expect(refusal({ startAt: 'soon' })).toContain('Start at');
	expect(refusal({ input: '[1]' })).toContain('JSON object');
});

test('a create has nothing stored: an empty form asks for no value of its own', () => {
	const written = scheduleWrite({ ...EMPTY_SCHEDULE_FORM, cron: '0 2 * * *', overlap: 'skip' });
	if ('error' in written) throw new Error(written.error);
	expect(written.value.spec.cron).toEqual(['0 2 * * *']);
	expect(written.value.spec.overlap).toBe(OverlapPolicy.SKIP);
	expect(written.value.input).toBeUndefined();
});

test('the update command names only what changed, as the CLI takes it', () => {
	const schedule = stored();
	const form = scheduleFormOf(schedule);
	expect(scheduleUpdateCli(schedule, form)).toBe('orkctl schedule update nightly -n team-a');
	expect(
		scheduleUpdateCli(schedule, {
			...form,
			version: '',
			cron: '@daily',
			interval: '',
			description: '',
			input: '{"a": 1}',
			pauseOnFailure: false
		})
	).toBe(
		"orkctl schedule update nightly --follow-current --cron @daily --interval '' --pause-on-failure=false --description '' --input-json '{\"a\":1}' -n team-a"
	);
	expect(scheduleUpdateCli(schedule, { ...form, input: '' })).toBe('orkctl schedule update nightly --clear-input -n team-a');
	expect(scheduleUpdateCli(schedule, { ...form, version: 'v2', overlap: 'skip' })).toBe(
		'orkctl schedule update nightly --version v2 --overlap skip -n team-a'
	);
});

test('the CLI cannot empty a list of cron expressions, so that edit has no command to show', () => {
	const schedule = stored();
	expect(scheduleUpdateCli(schedule, { ...scheduleFormOf(schedule), cron: '' })).toBe('');
});

test('isoUTC shows the second an instant falls in, never the next one', () => {
	expect(isoUTC(create(TimestampSchema, { seconds: 1794168803n, nanos: 999_600_000 }))).toBe('2026-11-08T20:13:23Z');
	expect(isoUTC(create(TimestampSchema, { seconds: 1794168803n, nanos: 0 }))).toBe('2026-11-08T20:13:23Z');
	expect(isoUTC(undefined)).toBe('—');
});
