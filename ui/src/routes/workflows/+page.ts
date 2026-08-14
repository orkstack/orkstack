import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await orkstack(fetch).workflows.listWorkflows({});
		return { workflows: res.workflows, error: null as string | null };
	} catch (err) {
		return { workflows: [], error: errorMessage(err) };
	}
};
