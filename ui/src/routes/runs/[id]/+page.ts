import { errorMessage, orkstack } from '$lib/client';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params }) => {
	const client = orkstack(fetch);
	try {
		const res = await client.runs.getRun({ runId: params.id });
		const run = res.run ?? null;
		let workflow = null;
		if (run) {
			try {
				const wfRes = await client.workflows.getWorkflow({
					name: run.workflowName,
					version: run.workflowVersion
				});
				workflow = wfRes.workflow ?? null;
			} catch {
				workflow = null; // workflow may have been superseded; statuses still render
			}
		}
		return { run, workflow, error: null as string | null };
	} catch (err) {
		return { run: null, workflow: null, error: errorMessage(err) };
	}
};
