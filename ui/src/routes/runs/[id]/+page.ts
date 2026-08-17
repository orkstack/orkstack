import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params }) => {
	const client = orkstack(fetch);
	try {
		const res = await client.runs.getRun({ runId: params.id });
		const run = res.run ?? null;
		let orkflow = null;
		if (run) {
			try {
				const wfRes = await client.orkflows.getOrkflow({
					name: run.workflowName,
					version: run.workflowVersion
				});
				orkflow = wfRes.orkflow ?? null;
			} catch {
				orkflow = null; // orkflow may have been superseded; statuses still render
			}
		}
		return { run, orkflow, error: null as string | null };
	} catch (err) {
		return { run: null, orkflow: null, error: errorMessage(err) };
	}
};
