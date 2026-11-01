import { errorMessage } from '#lib/client.ts';
import { orkflowClient } from '#lib/clients/orkflows.ts';
import { namespaceFrom } from '#lib/namespace.ts';
import { pageNumber, pageSize } from '#lib/browsing.ts';
import { filtersFromUrl } from '#lib/listFilter.ts';
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
		const res = await orkflowClient(fetch).listOrkflows({ namespace, pageSize: size, pageToken: cursor, filters: applied });
		return { orkflows: res.orkflows, nextPageToken: res.nextPageToken, size, cursor, page, namespace, applied, error: null as string | null };
	} catch (err) {
		return { orkflows: [], nextPageToken: '', size, cursor, page, namespace, applied, error: errorMessage(err) };
	}
};
