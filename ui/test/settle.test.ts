import { expect, test } from 'bun:test';
import { afterNavigation } from '../src/lib/settle';

// A controllable navigation: `pending` reports it until it settles.
function navigation() {
	let settle!: (ok: boolean) => void;
	let current: Promise<void> | null = new Promise<void>((resolve, reject) => {
		settle = (ok) => {
			current = null;
			if (ok) resolve();
			else reject(new Error('aborted'));
		};
	});
	return { pending: () => current, settle };
}

test('with no navigation in flight it re-reads at once', async () => {
	let reads = 0;
	const done = await afterNavigation(
		() => null,
		() => true,
		async () => {
			reads++;
		}
	);
	expect(done).toBe(true);
	expect(reads).toBe(1);
});

test('a navigation in flight is waited out before the re-read starts', async () => {
	const nav = navigation();
	const order: string[] = [];
	const result = afterNavigation(
		nav.pending,
		() => true,
		async () => {
			order.push('reread');
		}
	);
	await Promise.resolve();
	expect(order).toEqual([]);
	order.push('navigation done');
	nav.settle(true);
	expect(await result).toBe(true);
	expect(order).toEqual(['navigation done', 'reread']);
});

test('a navigation that left the page cancels the re-read', async () => {
	const nav = navigation();
	let mounted = true;
	let reads = 0;
	const result = afterNavigation(
		nav.pending,
		() => mounted,
		async () => {
			reads++;
		}
	);
	mounted = false;
	nav.settle(true);
	expect(await result).toBe(false);
	expect(reads).toBe(0);
});

test('an aborted navigation that kept the page still gets its re-read', async () => {
	const nav = navigation();
	let reads = 0;
	const result = afterNavigation(
		nav.pending,
		() => true,
		async () => {
			reads++;
		}
	);
	nav.settle(false);
	expect(await result).toBe(true);
	expect(reads).toBe(1);
});

test('a navigation started while waiting is waited out too', async () => {
	const first = navigation();
	const second = navigation();
	let active = first;
	let reads = 0;
	const result = afterNavigation(
		() => active.pending(),
		() => true,
		async () => {
			reads++;
		}
	);
	active = second;
	first.settle(true);
	await Promise.resolve();
	await Promise.resolve();
	expect(reads).toBe(0);
	second.settle(true);
	expect(await result).toBe(true);
	expect(reads).toBe(1);
});

// A model of SvelteKit 3's router, as client.js `_invalidate` behaves: an
// invalidation notes the navigation token and the current page when it
// starts, and when its load returns it gives up, silently, if a navigation
// began or landed in between. A navigation that stays on the page is a
// navigation all the same.
function router() {
	let token = 0;
	let current = 0;
	let landing: Promise<void> | null = null;
	let land = () => {};
	let applied = 0;
	return {
		applied: () => applied,
		navigate() {
			token++;
			landing = new Promise<void>((resolve) => {
				land = () => {
					current++;
					landing = null;
					resolve();
				};
			});
		},
		land: () => land(),
		pending: () => landing,
		async invalidate(load: Promise<void>) {
			const started = { token, current };
			await load;
			if (started.token !== token || started.current !== current) return;
			applied++;
		}
	};
}

test('the router drops a re-read that a navigation staying on the page overlaps', async () => {
	const r = router();
	r.navigate();
	let loaded = () => {};
	const reread = r.invalidate(new Promise<void>((resolve) => (loaded = resolve)));
	r.land();
	loaded();
	await reread;
	expect(r.applied()).toBe(0);
});

test('through afterNavigation that re-read is applied once the navigation lands', async () => {
	const r = router();
	r.navigate();
	const result = afterNavigation(
		r.pending,
		() => true,
		() => r.invalidate(Promise.resolve())
	);
	await Promise.resolve();
	expect(r.applied()).toBe(0);
	r.land();
	expect(await result).toBe(true);
	expect(r.applied()).toBe(1);
});
