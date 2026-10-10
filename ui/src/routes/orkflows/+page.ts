import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { pageNumber, pageSize } from '$lib/browsing';
import { filtersFromUrl } from '$lib/listFilter';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	// The page number is a LABEL for the cursor beside it (runs/+page.ts says
	// the same thing at more length): without a cursor there is no later page
	// to number, so it is 1.
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	// A `filter` parameter that does not parse is shown without a request.
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	if (filterError) return { orkflows: [], nextPageToken: '', size, cursor, page, namespace, applied, error: filterError as string | null };
	try {
		const res = await orkstack(fetch).orkflows.listOrkflows({ namespace, pageSize: size, pageToken: cursor, filters: applied });
		return { orkflows: res.orkflows, nextPageToken: res.nextPageToken, size, cursor, page, namespace, applied, error: null as string | null };
	} catch (err) {
		return { orkflows: [], nextPageToken: '', size, cursor, page, namespace, applied, error: errorMessage(err) };
	}
};
