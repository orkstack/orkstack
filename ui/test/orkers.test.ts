import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { MetadataProbeOutcome, OrkerProfileSchema, OrkerSchema, ProfileCordonReason, ProfileCordonSchema, ProfileCordon_Cause, ProfileKind } from '../src/lib/gen/orkstack/v1/activity_pb';
import { cordonCommand, metadataConcerns, metadataProbeText, metadataReachable, operatorCordoned, orkerCommand, orkerGpu, orkerLoad, orkerPins, orkerState, orkerStateLabel, orkerTasks, MAX_UNIT_CELLS, profileCause, profileCauseChip, profileCauseNote, profileState, profileCauses, profileKindLabel, profileReasonCode, profileUnits, profileUnitsText, stoppingNote } from '../src/lib/orkers';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';

test('an Orker that could not be asked is unreachable and has no numbers, never zero', () => {
	const o = create(OrkerSchema, { orkerId: 'orker-c001', unavailableReason: 'describe orker: deadline' });
	expect(orkerState(o)).toBe('unreachable');
	expect(orkerTasks(o)).toBe('-');
	expect(orkerPins(o)).toBe('-');
});

test('state, tasks and pins read as orkctl orker list prints them', () => {
	const cordoned = create(OrkerSchema, {
		orkerId: 'orker-7f3a', cordoned: true, cordonReason: 'cve', runningTasks: 2, openPins: 1, maxConcurrentPins: 2
	});
	expect(orkerState(cordoned)).toBe('cordoned');
	expect(orkerTasks(cordoned)).toBe('2');
	expect(orkerPins(cordoned)).toBe('1/2');
	expect(orkerLoad(cordoned)).toBe('is running 2 tasks and hosts 1 pin');
	const plain = create(OrkerSchema, { orkerId: 'orker-b45e', runningTasks: 1 });
	expect(orkerState(plain)).toBe('ready');
	expect(orkerPins(plain)).toBe('-');
	expect(orkerLoad(plain)).toBe('is running 1 task and hosts 0 pins');
});

test('the CLI equivalent quotes a reason so it cannot become shell syntax', () => {
	const o = create(OrkerSchema, { orkerId: 'orker-7f3a', cordoned: true, cordonReason: "it's $(rm -rf /)" });
	expect(orkerCommand(o)).toBe(`orkctl orker cordon orker-7f3a --reason 'it'"'"'s $(rm -rf /)'`);
	expect(orkerCommand(create(OrkerSchema, { orkerId: 'orker-7f3a' }))).toBe(`orkctl orker uncordon orker-7f3a`);
	expect(cordonCommand('orker-7f3a', "a'b")).toBe(`orkctl orker cordon orker-7f3a --reason 'a'"'"'b'`);
});

test('a store outage reads differently from an operator\'s cordon, in plain words', () => {
	const storeOutage = { reason: 'object storage unreachable: the last check failed' };
	const outage = create(OrkerSchema, { orkerId: 'o', cordoned: true, storeOutage });
	expect(orkerState(outage)).toBe('store-outage');
	expect(orkerStateLabel(orkerState(outage))).toBe('object storage unreachable');
	expect(operatorCordoned(outage)).toBe(false);

	const both = create(OrkerSchema, { orkerId: 'o', cordoned: true, cordonReason: 'cve', storeOutage });
	expect(orkerState(both)).toBe('cordoned');
	expect(operatorCordoned(both)).toBe(true);
});

test('an Orker that sends no store outage reads as it always has', () => {
	const older = create(OrkerSchema, { orkerId: 'o', cordoned: true, cordonReason: 'cve' });
	expect(orkerState(older)).toBe('cordoned');
	expect(orkerStateLabel('cordoned')).toBe('cordoned');
	expect(operatorCordoned(older)).toBe(true);
	expect(operatorCordoned(create(OrkerSchema, { orkerId: 'o' }))).toBe(false);
});

test('the CLI equivalent of a store outage reads the Orker, because no command clears it', () => {
	const outage = create(OrkerSchema, { orkerId: 'orker-7f3a', cordoned: true, storeOutage: { reason: 'x' } });
	expect(orkerCommand(outage)).toBe('orkctl orker get orker-7f3a');
	const both = create(OrkerSchema, { orkerId: 'orker-7f3a', cordoned: true, cordonReason: 'cve', storeOutage: { reason: 'x' } });
	expect(orkerCommand(both)).toBe('orkctl orker cordon orker-7f3a --reason cve');
});

test('a stopping Orker reads as stopping, not as an operator\'s cordon, and offers no cordon', () => {
	const stopping = create(OrkerSchema, { orkerId: 'orker-7f3a', cordoned: true, stopping: {}, runningTasks: 2 });
	expect(orkerState(stopping)).toBe('stopping');
	expect(orkerStateLabel('stopping')).toBe('stopping');
	expect(operatorCordoned(stopping)).toBe(false);
	expect(orkerCommand(stopping)).toBe('orkctl orker get orker-7f3a');

	const both = create(OrkerSchema, { orkerId: 'orker-7f3a', cordoned: true, cordonReason: 'cve', stopping: {} });
	expect(orkerState(both)).toBe('stopping');
	expect(operatorCordoned(both)).toBe(true);
	expect(orkerCommand(both)).toBe('orkctl orker cordon orker-7f3a --reason cve');
});

test('a stopping Orker says when it began and when its grace ends', () => {
	const at = (iso: string) => timestampFromDate(new Date(iso));
	const iso = (ts: { seconds: bigint } | undefined) => (ts ? new Date(Number(ts.seconds) * 1000).toISOString() : '—');
	const o = create(OrkerSchema, {
		orkerId: 'orker-a', cordoned: true,
		stopping: { since: at('2026-11-14T09:00:00Z'), graceEndsAt: at('2026-11-14T10:00:00Z') }
	});
	expect(stoppingNote(o, iso)).toBe(
		'Stopping since 2026-11-14T09:00:00.000Z: it takes no new work and finishes what it runs, then exits; its grace ends 2026-11-14T10:00:00.000Z. A task it hands back runs again on another Orker of its release.'
	);
	// An Orker of a release that reports neither time keeps the plain sentence.
	const bare = create(OrkerSchema, { orkerId: 'orker-b', cordoned: true, stopping: {} });
	expect(stoppingNote(bare, iso)).toBe(
		'Stopping: it takes no new work and finishes what it runs, then exits. A task it hands back runs again on another Orker of its release.'
	);
});

test('the metadata probe names what answered or could not tell, as orkctl orker list does', () => {
	const o = create(OrkerSchema, {
		orkerId: 'o',
		metadataProbes: [
			{ address: '169.254.169.254', outcome: MetadataProbeOutcome.ANSWERED, detail: 'HTTP 200 to PUT /latest/api/token' },
			{ address: '168.63.129.16', outcome: MetadataProbeOutcome.NO_ANSWER },
			{ address: 'metadata.google.internal', outcome: MetadataProbeOutcome.PROBE_FAILED, detail: 'a request of the probe\'s container never reported' }
		]
	});
	expect(metadataConcerns(o)).toEqual([
		'169.254.169.254 answered (HTTP 200 to PUT /latest/api/token)',
		"metadata.google.internal probe failed (a request of the probe's container never reported)"
	]);
	expect(metadataReachable(o)).toBe(true);
	expect(metadataProbeText(o.metadataProbes[1])).toBe('no answer');
	const unchecked = create(OrkerSchema, { orkerId: 'u', metadataProbes: [{ address: 'a', outcome: MetadataProbeOutcome.PROBE_FAILED }] });
	expect(metadataReachable(unchecked)).toBe(false);
	expect(metadataConcerns(unchecked)).toEqual(['a probe failed']);
	const clean = create(OrkerSchema, { orkerId: 'c', metadataProbes: [{ address: 'a', outcome: MetadataProbeOutcome.NO_ANSWER }] });
	expect(metadataConcerns(clean)).toEqual([]);
	expect(metadataConcerns(create(OrkerSchema, { orkerId: 'off' }))).toEqual([]);
	// An outcome this build does not know is a concern, never clean.
	const unknown = create(OrkerSchema, { orkerId: 'n', metadataProbes: [{ address: 'a', outcome: MetadataProbeOutcome.UNSPECIFIED }] });
	expect(metadataConcerns(unknown)).toEqual(['a unknown']);
	expect(metadataReachable(unknown)).toBe(false);
});

// Profiles (docs/DEPLOY.md, "Running a device Orker").
const at = timestampFromDate(new Date('2026-11-17T10:00:00Z'));
const fmt = () => '10:00';
const drift = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.DRIFT, profile: 'sensor', reason: ProfileCordonReason.DEVICE_CHANGED, since: at });
const absent = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'sensor', reason: ProfileCordonReason.DEVICE_ABSENT, since: at });
const probe = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'camera', reason: ProfileCordonReason.PROBE_IMAGE_UNAVAILABLE });

test('a profile cause is never read as an operator’s cordon, and has its own state', () => {
	const o = create(OrkerSchema, { orkerId: 'o', cordoned: true, profileCordons: [probe] });
	expect(operatorCordoned(o)).toBe(false);
	expect(orkerState(o)).toBe('profile-unavailable');
	expect(orkerStateLabel(orkerState(o))).toBe('profile unavailable');
	expect(orkerCommand(o)).toBe('orkctl orker get o');
	const drifted = create(OrkerSchema, { orkerId: 'o', cordoned: true, profileCordons: [probe, drift] });
	expect(orkerState(drifted)).toBe('profile-drift');
	expect(orkerStateLabel('profile-drift')).toBe('profile drifted');
});

test('the state ranks stopping, an operator, drift, a store outage, then an unavailable profile', () => {
	const storeOutage = { reason: 'object storage unreachable' };
	expect(orkerState(create(OrkerSchema, { cordoned: true, cordonReason: 'cve', profileCordons: [drift] }))).toBe('cordoned');
	expect(orkerState(create(OrkerSchema, { cordoned: true, storeOutage, profileCordons: [drift] }))).toBe('profile-drift');
	expect(orkerState(create(OrkerSchema, { cordoned: true, storeOutage, profileCordons: [probe] }))).toBe('store-outage');
	expect(orkerState(create(OrkerSchema, { cordoned: true, stopping: {}, profileCordons: [drift] }))).toBe('stopping');
});

test('causes read drift first, then by reason, and each says how it ends, with its code', () => {
	const o = create(OrkerSchema, { profileCordons: [probe, absent, drift] });
	expect(profileCauses(o).map((c) => c.reason)).toEqual([ProfileCordonReason.DEVICE_CHANGED, ProfileCordonReason.PROBE_IMAGE_UNAVAILABLE, ProfileCordonReason.DEVICE_ABSENT]);
	expect(profileCauseNote(drift, fmt)).toBe('Profile sensor drifted since 10:00: a device was replaced, or changed its numbers or group (DEVICE_CHANGED). It stays out of service on this Orker until the Orker restarts.');
	expect(profileCauseNote(probe, fmt)).toBe('Profile camera unavailable: its probe image cannot be pulled or run (PROBE_IMAGE_UNAVAILABLE). Once that is put right, it returns by itself after consecutive good checks.');
	expect(profileCause(o, 'sensor')?.reason).toBe(ProfileCordonReason.DEVICE_CHANGED);
	expect(profileCause(o, 'other')).toBeUndefined();
});

test('a reason this release does not know is still named, by its number', () => {
	const later = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'gpu', reason: 99 as ProfileCordonReason });
	expect(profileReasonCode(later.reason)).toBe('REASON_99');
	expect(profileCauseNote(later, fmt)).toContain('the Orker found a fault (REASON_99)');
});

test('a gpu profile’s units split into bound, free and unhealthy, clamped; a devices profile has none', () => {
	const gpu = create(OrkerProfileSchema, { name: 'amd', kind: ProfileKind.GPU, units: 4, usableUnits: 3, boundUnits: 1 });
	expect(profileUnits(gpu)).toEqual({ bound: 1, free: 2, unusable: 1, total: 4 });
	expect(profileUnitsText(gpu)).toBe('1 of 3 usable units in use; 1 unhealthy');
	const skewed = create(OrkerProfileSchema, { kind: ProfileKind.GPU, units: 1, usableUnits: 5, boundUnits: 9 });
	expect(profileUnits(skewed)).toEqual({ bound: 1, free: 0, unusable: 0, total: 1 });
	expect(profileUnitsText(skewed)).toBe('1 of 1 usable unit in use');
	expect(profileUnits(create(OrkerProfileSchema, { kind: ProfileKind.GPU, units: 0 }))).toBeNull();
	const huge = create(OrkerProfileSchema, { kind: ProfileKind.GPU, units: 2 ** 31 - 1, usableUnits: 2 ** 31 - 1 });
	expect(profileUnits(huge)?.total).toBe(2 ** 31 - 1);
	expect(MAX_UNIT_CELLS).toBe(64);
	const devices = create(OrkerProfileSchema, { name: 'sensor', kind: ProfileKind.DEVICES });
	expect(profileUnits(devices)).toBeNull();
	expect(profileUnitsText(devices)).toBe('');
	expect(profileKindLabel(ProfileKind.DEVICES)).toBe('devices');
	expect(profileKindLabel(ProfileKind.GPU)).toBe('gpu');
});

test('a cause this release cannot read keeps the Orker out of its pool, never a wait that clears itself', () => {
	const unknown = create(ProfileCordonSchema, { cause: 7 as ProfileCordon_Cause, profile: 'x', reason: ProfileCordonReason.SPEC_ABSENT });
	const o = create(OrkerSchema, { orkerId: 'o', cordoned: true, storeOutage: { reason: 'store' }, profileCordons: [unknown, probe] });
	expect(profileState(o)).toBe('profile-out');
	expect(orkerState(o)).toBe('profile-out');
	expect(orkerStateLabel('profile-out')).toBe('profile out of service');
	expect(profileCauseChip(unknown)).toEqual({ word: 'out of service', failure: true });
	expect(profileCauseChip(probe)).toEqual({ word: 'unavailable', failure: false });
	expect(profileCauseChip(drift)).toEqual({ word: 'drifted', failure: true });
	expect(profileCauseNote(unknown, fmt)).not.toContain('returns by itself');
	expect(profileCauses(create(OrkerSchema, { profileCordons: [probe, unknown, drift] })).map((c) => c.cause)).toEqual([ProfileCordon_Cause.DRIFT, 7, ProfileCordon_Cause.UNAVAILABLE]);
	expect(profileState(create(OrkerSchema, {}))).toBeNull();
	expect(profileState(create(OrkerSchema, { cordoned: true, cordonReason: 'cve', profileCordons: [drift] }))).toBe('profile-drift');
});

test('gpu reserved reads units of capacity, and nothing for an Orker that reports no gpu capacity', () => {
	expect(orkerGpu(create(OrkerSchema, { orkerId: 'g', capacity: { gpu: 4n }, reserved: { gpu: 1n } }))).toBe('1 of 4');
	expect(orkerGpu(create(OrkerSchema, { orkerId: 'g', capacity: { gpu: 2n } }))).toBe('0 of 2');
	expect(orkerGpu(create(OrkerSchema, { orkerId: 'c', capacity: { cpu: 2 } }))).toBe(null);
	expect(orkerGpu(create(OrkerSchema, { orkerId: 'u', unavailableReason: 'deadline', capacity: { gpu: 2n } }))).toBe(null);
});

test('a gpu profile below its per-task ceiling is unavailable, named and said in words', () => {
	const cause = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'gpu', reason: ProfileCordonReason.UNITS_BELOW_CEILING });
	expect(profileReasonCode(cause.reason)).toBe('UNITS_BELOW_CEILING');
	const note = profileCauseNote(cause, () => '');
	expect(note.includes('fewer of its units are usable than the per-task gpu cap')).toBe(true);
	expect(note.includes('(UNITS_BELOW_CEILING)')).toBe(true);
	expect(profileCauseChip(cause)).toEqual({ word: 'unavailable', failure: false });
});

test('a gpu profile whose probes the container engine does not start names the engine, not a unit', () => {
	const cause = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'gpu', reason: ProfileCordonReason.PROBE_NOT_STARTED });
	const note = profileCauseNote(cause, () => '');
	expect(note.includes('the container engine does not start its unit probes')).toBe(true);
	expect(note.includes('(PROBE_NOT_STARTED)')).toBe(true);
	expect(profileCauseChip(cause)).toEqual({ word: 'unavailable', failure: false });
});
