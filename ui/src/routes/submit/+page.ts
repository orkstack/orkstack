import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const preselect = url.searchParams.get('orkflow') ?? '';
	try {
		const res = await orkstack(fetch).orkflows.listOrkflows({});
		return { orkflows: res.orkflows, preselect, error: null as string | null };
	} catch (err) {
		return { orkflows: [], preselect, error: errorMessage(err) };
	}
};
