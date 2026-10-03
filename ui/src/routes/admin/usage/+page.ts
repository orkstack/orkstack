import { errorMessage, orkstack } from '$lib/client';
import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
import { namespaceFrom } from '$lib/namespace';
import { USAGE_WINDOWS } from '$lib/usage';
import type { PageLoad } from './$types';

// Admin → Usage is `orkctl stats usage --group-by namespace` in a browser:
// every namespace side by side over one window. It is deployment-wide — the
// switcher's namespace travels in the URL only so links out keep it. The
// loader reads the namespace list; the page asks each namespace's usage
// itself, a few at a time, so one slow or failing namespace never blanks the
// others.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const requested = url.searchParams.get('window') ?? '24h';
	const window = (USAGE_WINDOWS as readonly string[]).includes(requested) ? requested : '24h';
	try {
		const res = await orkstack(fetch).namespaces.listNamespaces({});
		return { namespace, window, namespacesList: res.namespaces, namespacesError: null as string | null };
	} catch (err) {
		return { namespace, window, namespacesList: [] as Namespace[], namespacesError: errorMessage(err) };
	}
};
