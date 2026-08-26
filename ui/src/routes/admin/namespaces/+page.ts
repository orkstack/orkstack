import { errorMessage, orkstack } from '$lib/client';
import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// This is the GLOBAL view (design §6.1): every namespace this deployment has,
// not the one the switcher selected. The switcher's value still travels in
// the URL, purely so a link out of this page (e.g. to Pools) can carry it —
// it never filters ListNamespaces.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	try {
		const res = await orkstack(fetch).namespaces.listNamespaces({});
		return {
			namespace,
			namespacesList: res.namespaces,
			namespacesError: null as string | null
		};
	} catch (err) {
		return {
			namespace,
			namespacesList: [] as Namespace[],
			namespacesError: errorMessage(err)
		};
	}
};
