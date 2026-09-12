import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { pageNumber, pageSize } from '$lib/browsing';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	// The page number is a LABEL for the cursor beside it (runs/+page.ts says
	// the same thing at more length): without a cursor there is no later page
	// to number, so it is 1.
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	try {
		const res = await orkstack(fetch).orkflows.listOrkflows({ namespace, pageSize: size, pageToken: cursor });
		return { orkflows: res.orkflows, nextPageToken: res.nextPageToken, size, cursor, page, namespace, error: null as string | null };
	} catch (err) {
		return { orkflows: [], nextPageToken: '', size, cursor, page, namespace, error: errorMessage(err) };
	}
};
