import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const workflowName = url.searchParams.get('orkflow') ?? '';
	try {
		const res = await orkstack(fetch).runs.listRuns({ workflowName });
		return { runs: res.runs, workflowName, error: null as string | null };
	} catch (err) {
		return { runs: [], workflowName, error: errorMessage(err) };
	}
};
