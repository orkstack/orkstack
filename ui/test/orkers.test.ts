import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { OrkerSchema } from '../src/lib/gen/orkstack/v1/activity_pb';
import { cordonCommand, operatorCordoned, orkerCommand, orkerLoad, orkerPins, orkerState, orkerStateLabel, orkerTasks } from '../src/lib/orkers';

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
