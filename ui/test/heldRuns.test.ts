import { expect, test } from 'bun:test';
import {
	HELD_KEY,
	MAX_RUNS_PER_NAMESPACE,
	NEVER_SEEN,
	heldFrom,
	heldRuns,
	loadHeld,
	markAllSeen,
	markSeen,
	observeList,
	saveHeld,
	type HeldStore,
	type ListedRun
} from '../src/lib/heldRuns';

const row = (id: string, status: string, createdMs: number, orkflow = 'deploy'): ListedRun => ({
	id,
	status,
	createdMs,
	orkflow
});

test('rule 1: the first visit to a namespace sets the watermark and holds nothing', () => {
	const { store, held } = observeList({}, 'default', [row('a', 'RUNNING', 10), row('b', 'FAILED', 20)], 1000);
	expect(held.size).toBe(0);
	expect(store.default.watermark).toBe(1000);
	expect(store.default.runs.a.seen).toBe('RUNNING');
	expect(store.default.runs.b.seen).toBe('FAILED');
});

test('rule 2: an unknown run is recorded as seen, unless it was created after the watermark and failed', () => {
	let store: HeldStore = observeList({}, 'default', [], 1000).store;
	({ store } = observeList(store, 'default', [row('old', 'FAILED', 900), row('new-ok', 'SUCCEEDED', 1500), row('new-bad', 'FAILED', 1500)], 2000));
	expect(heldFrom(store, 'default', 'old', 'FAILED')).toBeNull();
	expect(heldFrom(store, 'default', 'new-ok', 'SUCCEEDED')).toBeNull();
	// A run that failed while you were away is held, and stays held.
	expect(heldFrom(store, 'default', 'new-bad', 'FAILED')).toBe(NEVER_SEEN);
	const again = observeList(store, 'default', [row('new-bad', 'FAILED', 1500)], 3000);
	expect(again.held.get('new-bad')).toBe(NEVER_SEEN);
});

test('rule 3: a recorded run whose status moved is held; the status it was seen at is kept', () => {
	let { store } = observeList({}, 'default', [row('a', 'RUNNING', 10)], 1000);
	const second = observeList(store, 'default', [row('a', 'FAILED', 10)], 2000);
	store = second.store;
	expect(second.held.get('a')).toBe('RUNNING');
	expect(store.default.runs.a.seen).toBe('RUNNING');
	expect(store.default.runs.a.now).toBe('FAILED');
	// Still held on the next visit.
	expect(observeList(store, 'default', [row('a', 'FAILED', 10)], 3000).held.get('a')).toBe('RUNNING');
});

test('rule 4: opening the run page records it seen at its status', () => {
	let { store } = observeList({}, 'default', [row('a', 'RUNNING', 10)], 1000);
	({ store } = observeList(store, 'default', [row('a', 'FAILED', 10)], 2000));
	store = markSeen(store, 'default', 'a', 'FAILED', 'deploy', 2500);
	expect(observeList(store, 'default', [row('a', 'FAILED', 10)], 3000).held.size).toBe(0);
	// A run page opened on a run the list never showed records it too.
	store = markSeen(store, 'default', 'z', 'CANCELED', 'deploy', 2600);
	expect(store.default.runs.z.seen).toBe('CANCELED');
});

test('rule 5: Mark all seen records every held run at its current status', () => {
	let { store } = observeList({}, 'default', [row('a', 'RUNNING', 10), row('b', 'RUNNING', 20)], 1000);
	({ store } = observeList(store, 'default', [row('a', 'FAILED', 10), row('b', 'SUCCEEDED', 20)], 2000));
	expect(heldRuns(store, 'default').map((h) => h.id).sort()).toEqual(['a', 'b']);
	store = markAllSeen(store, 'default', 2500);
	expect(heldRuns(store, 'default')).toEqual([]);
});

test('rule 6: the watermark advances to now at the end of each visit', () => {
	const first = observeList({}, 'default', [], 1000);
	const second = observeList(first.store, 'default', [], 5000);
	expect(second.store.default.watermark).toBe(5000);
});

test('a poll patches statuses without moving the watermark', () => {
	const { store } = observeList({}, 'default', [row('a', 'RUNNING', 10)], 1000);
	const polled = observeList(store, 'default', [row('a', 'FAILED', 10)], 1015, { poll: true });
	expect(polled.held.get('a')).toBe('RUNNING');
	expect(polled.store.default.watermark).toBe(1000);
});

test('namespaces are isolated', () => {
	let { store } = observeList({}, 'default', [row('a', 'RUNNING', 10)], 1000);
	({ store } = observeList(store, 'payments', [row('a', 'FAILED', 10)], 1100));
	expect(heldFrom(store, 'payments', 'a', 'FAILED')).toBeNull();
	expect(heldFrom(store, 'default', 'a', 'FAILED')).toBe('RUNNING');
});

test('a namespace keeps at most 500 runs, evicting the least recently touched', () => {
	const rows = Array.from({ length: MAX_RUNS_PER_NAMESPACE + 20 }, (_, i) => row(`r${i}`, 'SUCCEEDED', i));
	let { store } = observeList({}, 'default', rows.slice(0, 20), 1000);
	({ store } = observeList(store, 'default', rows.slice(20), 2000));
	const kept = Object.keys(store.default.runs);
	expect(kept.length).toBe(MAX_RUNS_PER_NAMESPACE);
	expect(kept.includes('r0')).toBe(false);
	expect(kept.includes(`r${MAX_RUNS_PER_NAMESPACE + 19}`)).toBe(true);
});

test('held runs list carries what they were and what they are, and no fetch is needed', () => {
	let { store } = observeList({}, 'default', [row('a', 'RUNNING', 10, 'billing')], 1000);
	({ store } = observeList(store, 'default', [row('a', 'CANCELED', 10, 'billing')], 2000));
	expect(heldRuns(store, 'default')).toEqual([{ id: 'a', seen: 'RUNNING', now: 'CANCELED', orkflow: 'billing' }]);
});

function memoryStorage(initial: Record<string, string> = {}) {
	const data = { ...initial };
	return {
		getItem: (k: string) => (k in data ? data[k] : null),
		setItem: (k: string, v: string) => {
			data[k] = v;
		},
		data
	};
}

test('the store round-trips through storage under its key', () => {
	const storage = memoryStorage();
	const { store } = observeList({}, 'default', [row('a', 'RUNNING', 10)], 1000);
	saveHeld(store, storage);
	expect(Object.keys(storage.data)).toEqual([HELD_KEY]);
	expect(loadHeld(storage)).toEqual(store);
});

test('corrupt JSON, a wrong shape and throwing storage hold nothing and do not throw', () => {
	expect(loadHeld(memoryStorage({ [HELD_KEY]: '{nope' }))).toEqual({});
	expect(loadHeld(memoryStorage({ [HELD_KEY]: '[1,2]' }))).toEqual({});
	expect(loadHeld(memoryStorage({ [HELD_KEY]: '{"default":{"watermark":"x","runs":3}}' }))).toEqual({});
	const throwing = {
		getItem: () => {
			throw new Error('blocked');
		},
		setItem: () => {
			throw new Error('blocked');
		}
	};
	expect(loadHeld(throwing)).toEqual({});
	saveHeld({}, throwing); // must not throw
	expect(loadHeld(undefined)).toEqual({});
});
