import { errorMessage, orkstack } from '$lib/client';
import type { Namespace } from '$lib/gen/orkstack/v1/namespace_pb';
import { pageNumber } from '$lib/browsing';
import { usagePageSize } from '$lib/adminUsage';
import { filtersFromUrl } from '$lib/listFilter';
import { namespaceFrom } from '$lib/namespace';
import { USAGE_WINDOWS } from '$lib/usage';
import type { PageLoad } from './$types';

// Admin → Usage is `orkctl stats usage --group-by namespace` in a browser:
// namespaces side by side over one window. It is deployment-wide — the
// switcher's namespace travels in the URL only so links out keep it. The
// loader reads ONE PAGE of the namespace list, paged and filtered the way
// Admin → Namespaces is (`size`, `cursor`, `page`, repeated `filter`); the
// page asks the usage of those rows only ($lib/adminUsage), a few at a time,
// so one slow or failing namespace never blanks the others.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const requested = url.searchParams.get('window') ?? '24h';
	const window = (USAGE_WINDOWS as readonly string[]).includes(requested) ? requested : '24h';
	const size = usagePageSize(url.searchParams.get('size'));
	const cursor = url.searchParams.get('cursor') ?? '';
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	const base = { namespace, window, size, cursor, page, applied };
	if (filterError) {
		return { ...base, namespacesList: [] as Namespace[], nextPageToken: '', namespacesError: filterError as string | null };
	}
	try {
		const res = await orkstack(fetch).namespaces.listNamespaces({ pageSize: size, pageToken: cursor, filters: applied });
		return { ...base, namespacesList: res.namespaces, nextPageToken: res.nextPageToken, namespacesError: null as string | null };
	} catch (err) {
		return { ...base, namespacesList: [] as Namespace[], nextPageToken: '', namespacesError: errorMessage(err) };
	}
};
