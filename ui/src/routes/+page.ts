import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { durationFromMs, timestampFromDate } from '@bufbuild/protobuf/wkt';
import { statsWindowError } from '$lib/browsing';
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
	const window = url.searchParams.get('window') ?? '24h';
	const sinceParam = url.searchParams.get('since') ?? '';
	const presetMs: Record<string, number> = { '15m': 15 * 60_000, '6h': 6 * 60 * 60_000, '24h': 24 * 60 * 60_000 };
	const sinceDate = sinceParam ? new Date(sinceParam) : null;
	const since = sinceDate && !Number.isNaN(sinceDate.getTime()) ? timestampFromDate(sinceDate) : undefined;
	const windowError = statsWindowError(window, sinceParam);
	if (windowError) return { stats: null, namespace, window, sinceParam, error: windowError };
	try {
		const stats = await orkstack(fetch).stats.getStats({ namespace, ...(since ? { since } : { lookback: durationFromMs(presetMs[window] ?? presetMs['24h']) }) });
		return { stats, namespace, window: since ? 'custom' : (presetMs[window] ? window : '24h'), sinceParam, error: null as string | null };
	} catch (err) {
		return { stats: null, namespace, window: '24h', sinceParam, error: errorMessage(err) };
	}
};
