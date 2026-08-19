import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const orkflowName = url.searchParams.get('orkflow') ?? '';
	const namespace = namespaceFrom(url);
	try {
		const res = await orkstack(fetch).runs.listRuns({ orkflowName, namespace });
		return { runs: res.runs, orkflowName, namespace, error: null as string | null };
	} catch (err) {
		return { runs: [], orkflowName, namespace, error: errorMessage(err) };
	}
};
