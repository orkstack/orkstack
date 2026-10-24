import { errorMessage } from '$lib/client';
import { runClient } from '$lib/clients/runs';
import { namespaceFrom } from '$lib/namespace';
import { parseOnly } from '$lib/runDiff';
import type { DiffRunsResponse } from '$lib/gen/orkstack/v1/run_service_pb';
import type { PageLoad } from './$types';

// The comparison is one read, computed by the engine on every call: the page
// holds nothing but the two ids (and an optional field list) in its URL, so a
// link reproduces the same answer for anyone who can read both runs — and no
// run's input ever travels in it.
export const load: PageLoad = async ({ fetch, params, url }) => {
	const namespace = namespaceFrom(url);
	const to = url.searchParams.get('to') ?? '';
	const only = parseOnly(url.searchParams.get('only'));
	const base = { fromId: params.id, toId: to, only, namespace };
	if (to === '') {
		return { ...base, diff: null as DiffRunsResponse | null, error: null as string | null };
	}
	try {
		const diff = await runClient(fetch).diffRuns({
			fromRunId: params.id,
			toRunId: to,
			namespace,
			only
		});
		return { ...base, diff: diff as DiffRunsResponse | null, error: null as string | null };
	} catch (err) {
		return { ...base, diff: null as DiffRunsResponse | null, error: errorMessage(err) };
	}
};
