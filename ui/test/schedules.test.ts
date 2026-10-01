import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { DurationSchema, timestampFromDate } from '@bufbuild/protobuf/wkt';
import {
	OVERLAP_POLICY_SLUGS,
	durationLabel,
	fireOutcome,
	overlapPolicyFromSlug,
	overlapPolicySlug,
	parseDuration,
	scheduleCreateCli,
	scheduleDeleteCli,
	scheduleFireCli,
	schedulePauseCli,
	scheduleResumeCli,
	scheduleState,
	shellArg,
	whenLabel
} from '../src/lib/schedules';
import {
	OverlapPolicy,
	OverlapPolicySchema,
	ScheduleFireSchema,
	ScheduleSchema,
	ScheduleSpecSchema
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
		"orkctl schedule create nightly --orkflow hello --cron '0 2 * * *' --timezone Europe/Lisbon --overlap skip --paused --namespace payments"
	);
	schedule.description = "Bob's nightly";
	schedule.orkflowVersion = '3';
	expect(scheduleCreateCli(schedule)).toContain("--version 3 --cron '0 2 * * *'");
	expect(scheduleCreateCli(schedule)).toContain(`--description 'Bob'"'"'s nightly'`);
	expect(shellArg('$(rm -rf /)')).toBe("'$(rm -rf /)'");
});

test('one-liners: fire sends no override, so its command carries no --overlap', () => {
	expect(scheduleFireCli('nightly', '')).toBe('orkctl schedule fire nightly --namespace default');
	expect(scheduleFireCli('nightly', 'payments')).not.toContain('--overlap');
	expect(schedulePauseCli('nightly', 'payments', 'Q3 close')).toBe(
		"orkctl schedule pause nightly --reason 'Q3 close' --namespace payments"
	);
	expect(scheduleResumeCli('nightly', 'payments')).toBe('orkctl schedule resume nightly --namespace payments');
	expect(scheduleDeleteCli('nightly', 'payments')).toBe('orkctl schedule delete nightly --namespace payments');
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
