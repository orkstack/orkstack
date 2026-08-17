import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params, url }) => {
	const name = params.name;
	const version = url.searchParams.get('version') ?? '';
	try {
		const res = await orkstack(fetch).orkflows.getOrkflow({ name, version });
		return { orkflow: res.orkflow ?? null, error: null as string | null };
	} catch (err) {
		return { orkflow: null, error: errorMessage(err) };
	}
};
