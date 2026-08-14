import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params, url }) => {
	const name = params.name;
	const version = url.searchParams.get('version') ?? '';
	try {
		const res = await orkstack(fetch).workflows.getWorkflow({ name, version });
		return { workflow: res.workflow ?? null, error: null as string | null };
	} catch (err) {
		return { workflow: null, error: errorMessage(err) };
	}
};
