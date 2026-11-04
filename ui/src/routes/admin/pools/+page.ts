import { errorMessage } from '#lib/client.ts';
import { namespaceClient } from '#lib/clients/namespaces.ts';
import { activityClient } from '#lib/clients/activities.ts';
import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
import type { Pool } from '#lib/gen/orkstack/v1/namespace_pb.ts';
import { namespaceFrom } from '#lib/namespace.ts';
import { filtersFromUrl } from '#lib/listFilter.ts';
import type { PageLoad } from './$types';

// Pools is the one Admin view that RESPECTS the namespace switcher: it answers "the pools THIS namespace may dispatch onto", which
// is a namespace-scoped question by construction (NamespaceService.ListPools).
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	// The Orker inventory, streamed: each pool card's cpu and memory meters
	// sum what its Orkers reserved of what they declared. The pools list
	// never waits for it, and a card says so when it could not be read.
	const orkers = activityClient(fetch)
		.listOrkers({})
		.then(
			(res) => ({ orkers: res.orkers, error: null as string | null }),
			(err) => ({ orkers: [] as Orker[], error: errorMessage(err) })
		);
	// The filters are the URL's `filter` parameters; one that does not parse
	// is shown without a request.
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	if (filterError) return { namespace, applied, orkers, pools: [] as Pool[], defaultPool: '', poolsError: filterError as string | null };
	try {
		const res = await namespaceClient(fetch).listPools({ namespace, filters: applied });
		return {
			namespace,
			applied,
			orkers,
			pools: res.pools,
			defaultPool: res.defaultPool,
			poolsError: null as string | null
		};
	} catch (err) {
		return {
			namespace,
			applied,
			orkers,
			pools: [] as Pool[],
			defaultPool: '',
			poolsError: errorMessage(err)
		};
	}
};
