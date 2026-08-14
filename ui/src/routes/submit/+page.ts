import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const preselect = url.searchParams.get('workflow') ?? '';
	try {
		const res = await orkstack(fetch).workflows.listWorkflows({});
		return { workflows: res.workflows, preselect, error: null as string | null };
	} catch (err) {
		return { workflows: [], preselect, error: errorMessage(err) };
	}
};
