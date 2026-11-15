import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { OrkerSchema } from '../src/lib/gen/orkstack/v1/activity_pb';
import { cordonCommand, operatorCordoned, orkerCommand, orkerLoad, orkerPins, orkerState, orkerStateLabel, orkerTasks, stoppingNote } from '../src/lib/orkers';
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
