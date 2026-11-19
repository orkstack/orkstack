import { expect, test } from 'bun:test';
import { loadTaskInspect, preloadTaskInspect } from '../src/lib/taskInspectModule';

// The Inspect body loads once: every open after the first awaits the same,
// settled promise, never a fresh import() (which, in the browser, answers a
// task later; the settled promise's microtask answer is the browser's and
// Svelte's, not proved here). The run page starts that load when it mounts
// (preloadTaskInspect), so the first press after a page load finds it settled
// rather than waiting a task behind a live page's event applies. A failed
// load, preloaded or pressed, is forgotten, so the next open loads again, and
// a preload never rejects. One sequence: the cache is the module's own state.
test('the Inspect body loads once, a preload is the load every later open shares, and a failed load is retried', async () => {
	const module = { default: (() => {}) as never };
	let loads = 0;

	const failing = () => {
		loads++;
		return Promise.reject(new Error('chunk load failed'));
	};
	expect(await preloadTaskInspect(failing)).toBeUndefined();
	expect(loads).toBe(1);

	let refused = '';
	try {
		await loadTaskInspect(failing);
	} catch (err) {
		refused = (err as Error).message;
	}
	expect(refused).toBe('chunk load failed');
	expect(loads).toBe(2);

	const loading = () => {
		loads++;
		return Promise.resolve(module);
	};
	const preloaded = preloadTaskInspect(loading);
	expect(loads).toBe(3);
	const pressed = loadTaskInspect(loading);
	expect(loads).toBe(3);
	expect(await pressed).toBe(module);
	expect(await preloaded).toBeUndefined();

	const warm = loadTaskInspect(loading);
	expect(warm).toBe(pressed);
	expect(loads).toBe(3);
});
