import { expect, test } from 'bun:test';
import { pollWhileVisible, type PollEnv } from '../src/lib/visibility';

// F-ui-perf-4: a background tab polls nothing, and a returning reader gets a
// fresh answer at once. A hand-driven clock and document stand in for the
// browser's.

function fakeEnv(initial: 'visible' | 'hidden') {
	let id = 0;
	const timers = new Map<number, { fn: () => void; ms: number }>();
	const listeners = new Set<() => void>();
	const doc = {
		visibilityState: initial as string,
		addEventListener: (_: string, fn: () => void) => listeners.add(fn),
		removeEventListener: (_: string, fn: () => void) => listeners.delete(fn)
	};
	const env: PollEnv = {
		document: doc,
		setInterval: (fn, ms) => {
			timers.set(++id, { fn, ms });
			return id;
		},
		clearInterval: (handle) => timers.delete(handle as number)
	};
	return {
		env,
		/** One interval period elapses: every live timer fires once. */
		tick: () => [...timers.values()].forEach((t) => t.fn()),
		setVisibility: (state: 'visible' | 'hidden') => {
			doc.visibilityState = state;
			listeners.forEach((fn) => fn());
		},
		timers,
		listeners
	};
}

test('a visible page polls on the interval, not at once unless asked', () => {
	const clock = fakeEnv('visible');
	let calls = 0;
	pollWhileVisible(() => calls++, 5000, {}, clock.env);
	expect(calls).toBe(0);
	expect([...clock.timers.values()][0].ms).toBe(5000);
	clock.tick();
	clock.tick();
	expect(calls).toBe(2);

	const eager = fakeEnv('visible');
	let eagerCalls = 0;
	pollWhileVisible(() => eagerCalls++, 5000, { immediate: true }, eager.env);
	expect(eagerCalls).toBe(1);
});

test('nothing runs while hidden, and returning runs once immediately', () => {
	const clock = fakeEnv('visible');
	let calls = 0;
	pollWhileVisible(() => calls++, 5000, {}, clock.env);
	clock.tick();
	expect(calls).toBe(1);
	clock.setVisibility('hidden');
	expect(clock.timers.size).toBe(0);
	for (let i = 0; i < 10; i++) clock.tick();
	expect(calls).toBe(1);
	clock.setVisibility('visible');
	expect(calls).toBe(2);
	clock.tick();
	expect(calls).toBe(3);
});

test('a page opened in a background tab waits for its first look', () => {
	const clock = fakeEnv('hidden');
	let calls = 0;
	pollWhileVisible(() => calls++, 60_000, { immediate: true }, clock.env);
	expect(calls).toBe(0);
	expect(clock.timers.size).toBe(0);
	clock.setVisibility('visible');
	expect(calls).toBe(1);
	// A second "visible" while already polling starts nothing twice.
	clock.setVisibility('visible');
	expect(calls).toBe(1);
	expect(clock.timers.size).toBe(1);
});

test('stopping clears the timer and the listener', () => {
	const clock = fakeEnv('visible');
	const stop = pollWhileVisible(() => {}, 5000, {}, clock.env);
	stop();
	expect(clock.timers.size).toBe(0);
	expect(clock.listeners.size).toBe(0);
});
