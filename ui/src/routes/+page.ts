import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// The dashboard is the home page. A redirect to /orkflows would answer "how
// is this namespace doing?" with a list of definitions — the one question a
// definition cannot answer.
//
// Everything here comes from ONE call. GetStats derives every number on read
// from the cluster's index and the archive index, so any replica answers
// identically, including one that started nothing (proto/orkstack/v1/stats.proto).
export const load: PageLoad = async ({ fetch, url, depends }) => {
	// The auto-refresh below re-runs exactly this load and nothing else.
	depends('ork:stats');
	const namespace = namespaceFrom(url);
	try {
		const stats = await orkstack(fetch).stats.getStats({ namespace });
		return { stats, namespace, error: null as string | null };
	} catch (err) {
		return { stats: null, namespace, error: errorMessage(err) };
	}
};
