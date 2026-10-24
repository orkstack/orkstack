import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromDate } from '@bufbuild/protobuf/wkt';
import { OrkerSchema } from '../src/lib/gen/orkstack/v1/activity_pb';
import {
	ListReleasesResponseSchema,
	PromoteReleaseResponseSchema,
	RampReleaseResponseSchema,
	ReleaseBlockerSchema,
	ReleaseDrainState,
	ReleaseRole,
	ReleaseSchema
} from '../src/lib/gen/orkstack/v1/system_service_pb';
import { OpenPinSchema } from '../src/lib/gen/orkstack/v1/activity_pb';
import { RunSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	canPromote,
	canRamp,
	canRollBack,
	holdsSummary,
	percentText,
	promoteResult,
	rampResult,
	releaseCommand,
	roleLabel,
	runsLabel,
	stateLabel,
	validRampPercent
} from '../src/lib/releases';

const fmt = () => 'T';

test('roleLabel names every role, the ramping one with its percent', () => {
	expect(roleLabel(create(ReleaseSchema, { role: ReleaseRole.CURRENT }))).toBe('current');
	expect(roleLabel(create(ReleaseSchema, { role: ReleaseRole.RAMPING, rampPercent: 10 }))).toBe('ramping 10%');
	expect(roleLabel(create(ReleaseSchema, { role: ReleaseRole.RAMPING, rampPercent: 12.5 }))).toBe('ramping 12.5%');
	expect(roleLabel(create(ReleaseSchema, { role: ReleaseRole.PREVIOUS }))).toBe('previous');
	expect(roleLabel(create(ReleaseSchema, { role: ReleaseRole.CANDIDATE }))).toBe('candidate');
});

test('stateLabel reads as orkctl release status prints it, for every role and drain state', () => {
	const blocker = create(ReleaseBlockerSchema, { message: 'x' });
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.CURRENT }), fmt)).toBe('serving');
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.RAMPING }), fmt)).toBe('serving');
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.RAMPING, blockers: [blocker] }), fmt)).toBe(
		'at risk (1 blocker)'
	);
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.CANDIDATE }), fmt)).toBe('ready');
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.CANDIDATE, blockers: [blocker, blocker] }), fmt)).toBe(
		'not ready (2 blockers)'
	);
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.PREVIOUS, drained: true }), fmt)).toBe('drained');
	const checked = timestampFromDate(new Date('2026-09-24T09:13:02Z'));
	expect(
		stateLabel(
			create(ReleaseSchema, { role: ReleaseRole.PREVIOUS, drainState: ReleaseDrainState.DRAINING, drainCheckedAt: checked }),
			fmt
		)
	).toBe('draining (checked T)');
	expect(stateLabel(create(ReleaseSchema, { role: ReleaseRole.PREVIOUS }), fmt)).toBe('not yet checked');
	const pinned = create(ReleaseSchema, {
		role: ReleaseRole.PREVIOUS,
		drainState: ReleaseDrainState.DRAINED,
		orkers: [create(OrkerSchema, { orkerId: 'orker-7f3a', openPins: 1, pins: [create(OpenPinSchema, { runId: 'ork-1', pin: 'build' })] })]
	});
	expect(stateLabel(pinned, fmt)).toBe('drained by the cluster, pins still open');
});

test('an unreadable run count is unknown, never zero', () => {
	expect(runsLabel(create(ReleaseSchema, { runsInFlight: 0, runsUnavailableReason: 'the run index could not be asked' }))).toBe(
		'unknown'
	);
	expect(runsLabel(create(ReleaseSchema, { runsInFlight: 3 }))).toBe('3');
});

test('promote, roll back and ramp are offered per role', () => {
	const roles = [ReleaseRole.CURRENT, ReleaseRole.RAMPING, ReleaseRole.PREVIOUS, ReleaseRole.CANDIDATE];
	const offered = roles.map((role) => {
		const r = create(ReleaseSchema, { role });
		return [canPromote(r), canRollBack(r), canRamp(r)];
	});
	expect(offered).toEqual([
		[false, false, false],
		[true, false, true],
		[false, true, true],
		[true, false, true]
	]);
});

test('releaseCommand shell-quotes a build id containing a quote', () => {
	expect(releaseCommand('promote', "v1'; rm -rf /")).toBe(`orkctl release promote 'v1'"'"'; rm -rf /'`);
	expect(releaseCommand('rollback', 'v1')).toBe(`orkctl release rollback v1`);
	expect(releaseCommand('ramp', 'v2', 10)).toBe(`orkctl release ramp v2 --percent 10`);
	expect(releaseCommand('ramp', 'v2', 0)).toBe(`orkctl release ramp v2 --percent 0`);
});

test('holdsSummary names the runs and pins still on a release', () => {
	const r = create(ReleaseSchema, {
		runsInFlight: 3,
		runs: [create(RunSchema, { id: 'ork-1' })],
		orkers: [create(OrkerSchema, { orkerId: 'o', pins: [create(OpenPinSchema, { runId: 'ork-1' })] })]
	});
	expect(holdsSummary(r)).toBe('3 runs, 1 open pin');
	expect(holdsSummary(create(ReleaseSchema, {}))).toBe('');
});

test('result sentences match the CLI, including an unconfirmed routing', () => {
	const read = create(ListReleasesResponseSchema, {
		releases: [
			create(ReleaseSchema, { buildId: 'v1', role: ReleaseRole.CURRENT, runsInFlight: 14 }),
			create(ReleaseSchema, { buildId: 'v2', role: ReleaseRole.CANDIDATE })
		]
	});
	expect(
		promoteResult(read, 'v2', create(PromoteReleaseResponseSchema, { changed: true, previousBuildId: 'v1', routingConfirmed: true }), false)
	).toBe('release v2 is current (was v1). New runs start on v2; the 14 runs executing on v1 finish there. Keep v1 running until "orkctl release wait v1" exits 0.');
	expect(promoteResult(read, 'v2', create(PromoteReleaseResponseSchema, { changed: true, routingConfirmed: false }), false)).toContain(
		'The change is recorded, but the cluster had not applied it to new runs after 10s'
	);
	expect(rampResult(read, 'v2', 10, create(RampReleaseResponseSchema, { changed: true, routingConfirmed: true }))).toBe(
		'release v2 now takes 10% of new runs; v1 (current) takes the rest.'
	);
	expect(rampResult(read, 'v2', 10, create(RampReleaseResponseSchema, { changed: false }))).toBe(
		'release v2 already takes 10% of new runs; nothing changed.'
	);
});

// The proto's ramp_percent is a float32, and the CLI prints it with Go's
// shortest float32 form (strconv.FormatFloat(p, 'g', -1, 32)). The expected
// strings below are what that Go call prints for each value; a JS double read
// off the wire (Math.fround) must print the same.
test('a ramp percent prints as the CLI prints it, not as a double', () => {
	const wire = (p: number) => Math.fround(p);
	const cases: [number, string][] = [
		[0.1, '0.1'],
		[0.3, '0.3'],
		[12.5, '12.5'],
		[100 / 3, '33.333332'],
		[99.99, '99.99'],
		[10, '10'],
		[100, '100'],
		[0, '0'],
		[0.0001, '0.0001'],
		[1e-5, '1e-05'],
		[1.5e-5, '1.5e-05']
	];
	for (const [p, want] of cases) expect(percentText(wire(p))).toBe(want);
});

test('every sentence with a cluster-set percent uses the CLI form', () => {
	const ramping = create(ReleaseSchema, { buildId: 'v2', role: ReleaseRole.RAMPING, rampPercent: Math.fround(0.1) });
	expect(roleLabel(ramping)).toBe('ramping 0.1%');
	const read = create(ListReleasesResponseSchema, {
		releases: [create(ReleaseSchema, { buildId: 'v1', role: ReleaseRole.CURRENT }), ramping]
	});
	expect(promoteResult(read, 'v2', create(PromoteReleaseResponseSchema, { changed: true, routingConfirmed: true }), false)).toBe(
		'release v2 is current (its 0.1% ramp ends). New runs start on v2.'
	);
	expect(
		rampResult(
			read,
			'v3',
			5,
			create(RampReleaseResponseSchema, {
				changed: true,
				routingConfirmed: true,
				previousBuildId: 'v2',
				previousPercent: Math.fround(0.1)
			})
		)
	).toBe('release v3 now takes 5% of new runs; v1 (current) takes the rest (v2 was ramping at 0.1%; its runs finish on it).');
});

test('a ramp percent is a whole number from 1 to 100', () => {
	expect(validRampPercent('10')).toBe(10);
	expect(validRampPercent('100')).toBe(100);
	for (const bad of ['0', '101', '-1', '1.5', '', 'ten']) expect(validRampPercent(bad)).toBeNull();
});
