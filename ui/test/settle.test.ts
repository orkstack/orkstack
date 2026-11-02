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
