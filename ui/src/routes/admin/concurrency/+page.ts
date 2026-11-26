import { Code, ConnectError } from '@connectrpc/connect';
import { errorMessage } from '#lib/client.ts';
import { concurrencyClient } from '#lib/clients/concurrency.ts';
import { pageNumber } from '#lib/browsing.ts';
import type { ConcurrencyCoordinates } from '#lib/concurrencyLimits.ts';
import type { ConcurrencyLimit, ListConcurrencyLimitsResponse } from '#lib/gen/orkstack/v1/concurrency_service_pb.ts';
import { namespaceFrom, namespaceLabel } from '#lib/namespace.ts';
import { filtersFromUrl } from '#lib/listFilter.ts';
import type { PageLoad } from './$types';

// Concurrency is one namespace's limits, read from each limit at the moment
// of the load: `orkctl concurrency list`, or with `?key=` one key, `orkctl
// concurrency get`. It respects the namespace switcher, because a key belongs
// to its namespace. Every coordinate lives in the URL; a failed read is an
// error, never an empty list, and a key nothing holds or waits for is its own
// answer, not a failure.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const key = url.searchParams.get('key') ?? '';
	const cursor = url.searchParams.get('cursor') ?? '';
	const { filters, error: filterError } = filtersFromUrl(url);
	const coordinates: ConcurrencyCoordinates = {
		ns: namespaceLabel(namespace),
		filters,
		cursor,
		// A page number without a cursor names no page the engine could return.
		page: cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1
	};
	const answer = {
		namespace,
		key,
		coordinates,
		limit: null as ConcurrencyLimit | null,
		list: null as ListConcurrencyLimitsResponse | null,
		error: null as string | null
	};
	if (key) {
		try {
			const res = await concurrencyClient(fetch).getConcurrencyLimit({ namespace, key });
			return { ...answer, limit: res.limit ?? null, error: res.limit ? null : `the limit of key ${key} came back empty` };
		} catch (err) {
			if (!(err instanceof ConnectError && err.code === Code.NotFound)) return { ...answer, error: errorMessage(err) };
		}
		// NOT_FOUND is an unused key and a namespace that does not exist alike.
		// The unfiltered list is refused for the second, so only once it
		// answers is the key said to be unused.
		try {
			await concurrencyClient(fetch).listConcurrencyLimits({ namespace, pageSize: 1 });
			return answer;
		} catch (err) {
			return { ...answer, error: errorMessage(err) };
		}
	}
	if (filterError) return { ...answer, error: filterError };
	try {
		return { ...answer, list: await concurrencyClient(fetch).listConcurrencyLimits({ namespace, filters, pageToken: cursor }) };
	} catch (err) {
		return { ...answer, error: errorMessage(err) };
	}
};
