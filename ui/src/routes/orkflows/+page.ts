import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await orkstack(fetch).orkflows.listOrkflows({});
		return { orkflows: res.orkflows, error: null as string | null };
	} catch (err) {
		return { orkflows: [], error: errorMessage(err) };
	}
};
