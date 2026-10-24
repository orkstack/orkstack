import { errorMessage } from '$lib/client';
import { cacheClient } from '$lib/clients/cache';
import { orkflowClient } from '$lib/clients/orkflows';
import { cacheWindowFrom } from '$lib/cache';
import type { GetCacheStatsResponse } from '$lib/gen/orkstack/v1/cache_pb';
import { namespaceFrom } from '$lib/namespace';
import { durationFromMs, timestampFromDate } from '@bufbuild/protobuf/wkt';
import type { PageLoad } from './$types';

// Admin → Cache is `orkctl cache stats` in a browser: one namespace's cached
// tasks over one window — hits, misses, bypasses, the reused time — and the
// memos the store holds now. Both halves are derived on read by the server;
// a window or inventory too large to read whole is refused with its reason,
// shown as the server sent it.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const window = cacheWindowFrom(url);
	const orkflow = url.searchParams.get('orkflow') ?? '';
	const request =
		window.window === 'custom'
			? { namespace, orkflowName: orkflow, since: timestampFromDate(new Date(window.since ?? '')) }
			: { namespace, orkflowName: orkflow, lookback: durationFromMs((window.lookbackSeconds ?? 86400) * 1000) };
	const [stats, orkflows] = await Promise.allSettled([
		cacheClient(fetch).getCacheStats(request),
		orkflowClient(fetch).listOrkflows({ namespace, pageSize: 200 })
	]);
	return {
		namespace,
		window,
		orkflow,
		stats: stats.status === 'fulfilled' ? stats.value : (null as GetCacheStatsResponse | null),
		statsError: stats.status === 'rejected' ? errorMessage(stats.reason) : (null as string | null),
		orkflowNames: orkflows.status === 'fulfilled' ? orkflows.value.orkflows.map((wf) => wf.name) : []
	};
};
