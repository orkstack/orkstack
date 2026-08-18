import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const orkflowName = url.searchParams.get('orkflow') ?? '';
	try {
		const res = await orkstack(fetch).runs.listRuns({ orkflowName });
		return { runs: res.runs, orkflowName, error: null as string | null };
	} catch (err) {
		return { runs: [], orkflowName, error: errorMessage(err) };
	}
};
