import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { collectPages } from '$lib/browsing';
import { replayPinError } from '$lib/authoring';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const requestedName = url.searchParams.get('orkflow') ?? '';
	const requestedVersion = url.searchParams.get('version') ?? '';
	const fromRun = url.searchParams.get('fromRun') ?? '';
	const client = orkstack(fetch);
	try {
		const [orkflows, replay] = await Promise.all([
			collectPages(async (pageToken) => {
				const page = await client.orkflows.listOrkflows({ namespace, pageToken, pageSize: 100 });
				return { items: page.orkflows, nextPageToken: page.nextPageToken };
			}),
			fromRun ? client.runs.getRun({ runId: fromRun, namespace }) : Promise.resolve(undefined)
		]);
		const run = replay?.run;
		if (fromRun && !run) throw new Error(`run ${fromRun} was not found in this namespace`);
		if (run) {
			const pinError = replayPinError(run.id, run.orkflowName, run.orkflowVersion);
			if (pinError) throw new Error(pinError);
		}
		const preselect = run?.orkflowName ?? requestedName;
		const preselectVersion = run?.orkflowVersion ?? requestedVersion;
		const first = orkflows.find((workflow) => workflow.name === preselect) ?? (preselect ? { name: preselect } : orkflows[0]);
		let selectedOrkflow = null;
		if (first) {
			const selected = await client.orkflows.getOrkflow({
				name: first.name,
				version: first.name === preselect ? preselectVersion : '',
				namespace
			});
			selectedOrkflow = selected.orkflow ?? null;
		}
		return {
			orkflows,
			preselect,
			preselectVersion,
			selectedOrkflow,
			replay: run
				? { id: run.id, orkflowName: run.orkflowName, orkflowVersion: run.orkflowVersion, input: run.input?.data ?? {} }
				: null,
			namespace,
			error: null as string | null
		};
	} catch (err) {
		return { orkflows: [], preselect: requestedName, preselectVersion: requestedVersion, selectedOrkflow: null, replay: null, namespace, error: errorMessage(err) };
	}
};
