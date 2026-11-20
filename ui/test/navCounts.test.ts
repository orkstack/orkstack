import { expect, test } from 'bun:test';
import { NAV_COUNT_TIMEOUT_MS, readNavCounts, type NavCountReads } from '../src/lib/navCounts';

// The sidebar's counts are three reads the shell makes on every namespace it
// shows. A read the engine does not answer must never outlive the page that
// asked it: each one held a browser connection, and six of them left the
// next page's own reads queued behind them (proof 44642268123).

type Seen = { signal?: AbortSignal; timeoutMs?: number };

function held(seen: Seen[]): NavCountReads {
	const hang = (options?: Seen) =>
		new Promise<never>((_, reject) => {
			seen.push(options ?? {});
			options?.signal?.addEventListener('abort', () => reject(new Error('aborted')));
		});
	return {
		listOrkflows: (_req, options) => hang(options),
		listPools: (_req, options) => hang(options),
		listActivities: (_req, options) => hang(options)
	};
}

test('every count read carries its own timeout and the caller\'s signal', async () => {
	const seen: Seen[] = [];
	const controller = new AbortController();
	const counts = readNavCounts(held(seen), 'payments', controller.signal);
	await Promise.resolve();
	expect(seen).toHaveLength(3);
	for (const s of seen) {
		expect(s.timeoutMs).toBe(NAV_COUNT_TIMEOUT_MS);
		expect(s.signal?.aborted).toBe(false);
	}
	controller.abort();
	expect(seen.every((s) => s.signal?.aborted)).toBe(true);
	// An aborted read draws no count; it never throws.
	expect(await counts).toEqual({ orkflows: undefined, pools: undefined, activities: undefined });
});

test('answered reads are the counts, a failed one draws none', async () => {
	const reads: NavCountReads = {
		listOrkflows: async () => ({ orkflows: [{}, {}], nextPageToken: 'more' }),
		listPools: async () => ({ pools: [{}] }),
		listActivities: async () => {
			throw new Error('unavailable');
		}
	};
	expect(await readNavCounts(reads, 'default', new AbortController().signal)).toEqual({ orkflows: '2+', pools: '1', activities: undefined });
});
