import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, params, url }) => {
	const client = orkstack(fetch);
	// A run id addresses a run inside a namespace, not across the deployment:
	// the engine answers NotFound for a run of another one, so the page has to
	// say which it is asking about.
	const namespace = namespaceFrom(url);
	try {
		const res = await client.runs.getRun({ runId: params.id, namespace });
		const run = res.run ?? null;
		let orkflow = null;
		if (run) {
			try {
				const wfRes = await client.orkflows.getOrkflow({
					name: run.orkflowName,
					version: run.orkflowVersion,
					namespace
				});
				orkflow = wfRes.orkflow ?? null;
			} catch {
				orkflow = null; // orkflow may have been superseded; statuses still render
			}
		}
		return { runId: params.id, run, orkflow, namespace, error: null as string | null };
	} catch (err) {
		return {
			runId: params.id,
			run: null,
			orkflow: null,
			namespace,
			error: errorMessage(err)
		};
	}
};
