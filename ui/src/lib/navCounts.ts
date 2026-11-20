// The sidebar's counts: how many Orkflows and pools a namespace has, and how
// many images the catalog holds. The shell reads them for every namespace it
// shows, so a read must never outlive the page that asked it: each one holds
// one of the browser's few connections to the engine (six over HTTP/1.1),
// and reads nobody waits for any more left the next page's own reads queued
// behind them (proof 44642268123). So every read carries a timeout of its
// own and the page's signal, which the shell aborts when it moves on. A read
// that fails, times out or is aborted draws no count; it never fails a page.

/** How long one count read may take before it draws no count. */
export const NAV_COUNT_TIMEOUT_MS = 5_000;

type CallOptions = { signal?: AbortSignal; timeoutMs?: number };

/** The three reads, each the Connect client's own call. */
export interface NavCountReads {
	listOrkflows(request: { namespace: string }, options?: CallOptions): Promise<{ orkflows: unknown[]; nextPageToken: string }>;
	listPools(request: { namespace: string }, options?: CallOptions): Promise<{ pools: unknown[] }>;
	listActivities(request: Record<string, never>, options?: CallOptions): Promise<{ activities: unknown[] }>;
}

export interface NavCounts {
	orkflows?: string;
	pools?: string;
	activities?: string;
}

export async function readNavCounts(reads: NavCountReads, namespace: string, signal: AbortSignal): Promise<NavCounts> {
	const options = { signal, timeoutMs: NAV_COUNT_TIMEOUT_MS };
	const [orkflows, pools, activities] = await Promise.all([
		reads.listOrkflows({ namespace }, options).then((res) => `${res.orkflows.length}${res.nextPageToken ? '+' : ''}`, () => undefined),
		reads.listPools({ namespace }, options).then((res) => String(res.pools.length), () => undefined),
		reads.listActivities({}, options).then((res) => String(res.activities.length), () => undefined)
	]);
	return { orkflows, pools, activities };
}
