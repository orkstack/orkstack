import { errorMessage } from '$lib/client';
import { namespaceClient } from '$lib/clients/namespaces';
import type { Pool } from '$lib/gen/orkstack/v1/namespace_pb';
import { namespaceFrom } from '$lib/namespace';
import { filtersFromUrl } from '$lib/listFilter';
import type { PageLoad } from './$types';

// Pools is the one Admin view that RESPECTS the namespace switcher: it answers "the pools THIS namespace may dispatch onto", which
// is a namespace-scoped question by construction (NamespaceService.ListPools).
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	// The filters are the URL's `filter` parameters; one that does not parse
	// is shown without a request.
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	if (filterError) return { namespace, applied, pools: [] as Pool[], defaultPool: '', poolsError: filterError as string | null };
	try {
		const res = await namespaceClient(fetch).listPools({ namespace, filters: applied });
		return {
			namespace,
			applied,
			pools: res.pools,
			defaultPool: res.defaultPool,
			poolsError: null as string | null
		};
	} catch (err) {
		return {
			namespace,
			applied,
			pools: [] as Pool[],
			defaultPool: '',
			poolsError: errorMessage(err)
		};
	}
};
