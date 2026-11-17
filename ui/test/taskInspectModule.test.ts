import { expect, test } from 'bun:test';
import { loadTaskInspect } from '../src/lib/taskInspectModule';

// The Inspect body loads once: every open after the first awaits the same,
// settled promise, never a fresh import() (which, in the browser, answers a
// task later; the settled promise's microtask answer is the browser's and
// Svelte's, not proved here). A failed load is forgotten, so the next open
// loads again. One sequence: the cache is the module's own state.
test('the Inspect body loads once, every later open shares its promise, and a failed load is retried', async () => {
	const module = { default: (() => {}) as never };
	let loads = 0;

	const failing = () => {
		loads++;
		return Promise.reject(new Error('chunk load failed'));
	};
	let refused = '';
	try {
		await loadTaskInspect(failing);
	} catch (err) {
		refused = (err as Error).message;
	}
	expect(refused).toBe('chunk load failed');

	const loading = () => {
		loads++;
		return Promise.resolve(module);
	};
	const first = loadTaskInspect(loading);
	expect(loads).toBe(2);
	expect(await first).toBe(module);

	const warm = loadTaskInspect(loading);
	expect(warm).toBe(first);
	expect(loads).toBe(2);
});
