import { expect, test } from 'bun:test';
import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { OrkerSchema, ProfileCordonSchema, ProfileCordon_Cause, type Orker } from '../src/lib/gen/orkstack/v1/activity_pb';
import { PoolSchema } from '../src/lib/gen/orkstack/v1/namespace_pb';
import { fullDimension, loadOf, stateOf } from '../src/lib/poolLoad';

const unavailable = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.UNAVAILABLE, profile: 'sensor' });
const drift = create(ProfileCordonSchema, { cause: ProfileCordon_Cause.DRIFT, profile: 'sensor' });
const orker = (id: string, extra: MessageInitShape<typeof OrkerSchema> = {}) => create(OrkerSchema, { orkerId: id, pool: 'main', ...extra });
const held = (id: string, cause = unavailable) => orker(id, { cordoned: true, profileCordons: [cause] });
const word = (orkers: Orker[]) => stateOf(create(PoolSchema, { name: 'main', liveOrkerCount: orkers.length }), loadOf('main', orkers));

test('a pool whose every Orker an unavailable profile holds stays live: never Cordoned', () => {
	expect(word([held('a'), held('b')])).toEqual({ word: 'Profile unavailable', tone: 'wait' });
	expect(loadOf('main', [held('a'), held('b')]).profileUnavailable).toBe(2);
});

test('a drifted profile takes its Orker out of the pool: every Orker drifted reads Cordoned', () => {
	expect(word([held('a', drift)])).toEqual({ word: 'Cordoned', tone: 'wait' });
	expect(loadOf('main', [held('a', drift)]).profileOut).toBe(1);
});

test('a profile cause this release cannot read counts as out of the pool', () => {
	const unknown = create(ProfileCordonSchema, { cause: 99 as ProfileCordon_Cause, profile: 'x' });
	expect(loadOf('main', [held('a', unknown), orker('b')]).profileOut).toBe(1);
	expect(word([held('a', unknown), orker('b')]).word).toBe('1 profile out of service');
});

test('an operator cordon beside unavailable profiles is named first, and the pool is not Cordoned while one Orker stays live', () => {
	const op = orker('a', { cordoned: true, cordonReason: 'kernel patch', profileCordons: [unavailable] });
	expect(word([op, held('b')]).word).toBe('1 cordoned');
	expect(word([op]).word).toBe('Cordoned');
});

test('some Orkers held by an unavailable profile beside ready ones are counted', () => {
	expect(word([held('a'), orker('b')])).toEqual({ word: '1 profile unavailable', tone: 'wait' });
	expect(word([orker('a')])).toEqual({ word: 'Ready', tone: 'ok' });
});

test('an unreachable Orker and another pool’s are not counted', () => {
	const load = loadOf('main', [held('a'), orker('b', { unavailableReason: 'deadline' }), orker('c', { pool: 'gpu', cordoned: true, profileCordons: [unavailable] })]);
	expect(load.orkers).toBe(1);
	expect(load.profileUnavailable).toBe(1);
});

test('a stopping Orker beside one an unavailable profile holds is named as stopping', () => {
	expect(word([orker('a', { cordoned: true, stopping: {} }), held('b')]).word).toBe('1 stopping');
});

test('a stopping Orker whose profile drifted takes the pool out of service: never Stopping', () => {
	const both = orker('a', { cordoned: true, stopping: {}, profileCordons: [drift] });
	expect(loadOf('main', [both]).profileOut).toBe(1);
	expect(word([both]).word).toBe('Cordoned');
});

test('a store outage beside an unavailable profile is counted once, as the store outage', () => {
	const both = orker('a', { cordoned: true, storeOutage: { reason: 'x' }, profileCordons: [unavailable] });
	const load = loadOf('main', [both]);
	expect([load.storeOutages, load.profileUnavailable]).toEqual([1, 0]);
	expect(word([both]).word).toBe('Object storage unreachable');
});

test('an operator cordon and a drift on one Orker count once, as the operator cordon', () => {
	const both = orker('a', { cordoned: true, cordonReason: 'kernel patch', profileCordons: [drift] });
	const load = loadOf('main', [both, orker('b')]);
	expect([load.operatorCordons, load.profileOut]).toEqual([1, 0]);
});

test('an unknown pool and an empty one read as before', () => {
	expect(stateOf(create(PoolSchema, { name: 'main', unavailableReason: 'deadline' }), null).word).toBe('Unknown');
	expect(stateOf(create(PoolSchema, { name: 'main', liveOrkerCount: 0 }), loadOf('main', [])).word).toBe('No Orkers');
});

test('gpu units sum across the pool, and a pool that reserved every unit is Full on gpu units', () => {
	const g = (id: string, reserved: bigint) => orker(id, { capacity: { cpu: 8, gpu: 2n }, reserved: { cpu: 1, gpu: reserved } });
	const load = loadOf('main', [g('a', 2n), g('b', 2n)]);
	expect(load.gpu).toEqual({ reserved: 4, capacity: 4 });
	expect(fullDimension(load)).toBe('gpu');
	expect(word([g('a', 2n), g('b', 2n)])).toEqual({ word: 'Full', tone: 'wait' });
	expect(fullDimension(loadOf('main', [g('a', 1n)]))).toBe(null);
	expect(loadOf('main', [orker('c', { capacity: { cpu: 2 } })]).gpu).toEqual({ reserved: 0, capacity: 0 });
});
