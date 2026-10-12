import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { OrkerSchema } from '../src/lib/gen/orkstack/v1/activity_pb';
import { cordonCommand, orkerCommand, orkerLoad, orkerPins, orkerState, orkerTasks } from '../src/lib/orkers';

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
	expect(orkerCommand(o)).toBe(`orkctl orker cordon 'orker-7f3a' --reason 'it'"'"'s $(rm -rf /)'`);
	expect(orkerCommand(create(OrkerSchema, { orkerId: 'orker-7f3a' }))).toBe(`orkctl orker uncordon 'orker-7f3a'`);
	expect(cordonCommand('orker-7f3a', "a'b")).toBe(`orkctl orker cordon 'orker-7f3a' --reason 'a'"'"'b'`);
});
