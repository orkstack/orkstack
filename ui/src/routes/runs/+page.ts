import { errorMessage } from '#lib/client.ts';
import { runClient } from '#lib/clients/runs.ts';
import { statsClient } from '#lib/clients/stats.ts';
import { namespaceFrom } from '#lib/namespace.ts';
import { pageNumber, pageSize } from '#lib/browsing.ts';
import { filterOf, filtersFromUrl } from '#lib/listFilter.ts';
import { FilterOperator } from '#lib/gen/orkstack/v1/filter_pb.ts';
import { FailureGroupBy, StatsBucketWidth } from '#lib/gen/orkstack/v1/stats_pb.ts';
import type { PageLoad } from './$types';

const DAY = { seconds: 86_400n, nanos: 0 };
const WEEK = { seconds: 7n * 86_400n, nanos: 0 };

// The list is the page's own read, awaited; everything above it — the 24 h
// tiles, the latest failure, the 7-day charts and the Orkflow names the
// filter chip offers — streams in after, so the list never waits for a
// stats scan. Each part settles on its own and says so when it cannot.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	const coordinates = { namespace, size, cursor, page, applied };

	const stats = statsClient(fetch);
	const day = stats.getStats({ namespace, lookback: DAY });
	// The week's totals and "Runs created each day" are one read: seven days
	// on this browser's wall clock. A zone the engine does not know is no
	// reason to lose the totals, so that one asks again without a series.
	const zone = Intl.DateTimeFormat().resolvedOptions().timeZone;
	const week = stats
		.getStats({ namespace, bucketWidth: StatsBucketWidth.DAY, timezone: zone, bucketCount: 7 })
		.catch(() => stats.getStats({ namespace, lookback: WEEK }));
	const failures = stats.getFailureBreakdown({ namespace, lookback: WEEK, groupBy: [FailureGroupBy.CODE, FailureGroupBy.CATEGORY], limit: 5 });
	const flakes = stats.getFlakeIndex({ namespace, lookback: WEEK, groupBy: [FailureGroupBy.ORKFLOW, FailureGroupBy.TASK], limit: 5 });
	const latestFailure = runClient(fetch)
		.listRuns({ namespace, pageSize: 1, filters: [filterOf('status', FilterOperator.EQUALS, ['FAILED'])] })
		.then((res) => res.runs[0] ?? null);
	// The Orkflow chip's names: read after the page has drawn, with a client of its own.
	const orkflowNames = import('#lib/clients/orkflows.ts')
		.then(({ orkflowClient }) => orkflowClient(fetch).listOrkflows({ namespace, pageSize: 200 }))
		.then((res) => [...new Set(res.orkflows.map((o) => o.name))].sort());
	// Every streamed read is caught where it is made: a refused stats call
	// leaves its card saying so, never an unhandled rejection.
	const settle = <T>(p: Promise<T>) => p.then((value) => ({ value, error: null as string | null }), (err) => ({ value: null as T | null, error: errorMessage(err) }));
	const insights = {
		day: settle(day),
		week: settle(week),
		failures: settle(failures),
		flakes: settle(flakes),
		latestFailure: settle(latestFailure),
		orkflowNames: settle(orkflowNames)
	};

	if (filterError) return { runs: [], nextPageToken: '', ...coordinates, insights, error: filterError as string | null };
	try {
		const res = await runClient(fetch).listRuns({ namespace, pageSize: size, pageToken: cursor, filters: applied });
		return { runs: res.runs, nextPageToken: res.nextPageToken, ...coordinates, insights, error: null as string | null };
	} catch (err) {
		return { runs: [], nextPageToken: '', ...coordinates, insights, error: errorMessage(err) };
	}
};
