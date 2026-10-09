import { errorMessage, orkstack } from '$lib/client';
import type { Pool } from '$lib/gen/orkstack/v1/namespace_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// Pools is the one Admin view that RESPECTS the namespace switcher: it answers "the pools THIS namespace may dispatch onto", which
// is a namespace-scoped question by construction (NamespaceService.ListPools).
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	try {
		const res = await orkstack(fetch).namespaces.listPools({ namespace });
		return {
			namespace,
			pools: res.pools,
			defaultPool: res.defaultPool,
			poolsError: null as string | null
		};
	} catch (err) {
		return {
			namespace,
			pools: [] as Pool[],
			defaultPool: '',
			poolsError: errorMessage(err)
		};
	}
};
