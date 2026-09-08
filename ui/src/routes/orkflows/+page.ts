import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { cursorHistory, pageSize } from '$lib/browsing';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	const history = cursorHistory(url.searchParams.get('history') ?? '');
	try {
		const res = await orkstack(fetch).orkflows.listOrkflows({ namespace, pageSize: size, pageToken: cursor });
		return { orkflows: res.orkflows, nextPageToken: res.nextPageToken, size, cursor, history, namespace, error: null as string | null };
	} catch (err) {
		return { orkflows: [], nextPageToken: '', size, cursor, history, namespace, error: errorMessage(err) };
	}
};
