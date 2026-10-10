import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { pageNumber, pageSize } from '$lib/browsing';
import { filtersFromUrl } from '$lib/listFilter';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	// The page number is a LABEL for the cursor beside it, counted by the
	// client because the engine answers with a token and no position. A number
	// without a cursor names no page the engine could return, so it is 1.
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	// The filters ride the URL as repeated `filter` parameters in the CLI
	// grammar (docs/UI_CLI_PARITY.md "Filtering lists"). One that does not
	// parse is shown without a request; what a field may take is the
	// engine's to refuse, and its refusal is shown verbatim.
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	const coordinates = { namespace, size, cursor, page, applied };
	if (filterError) return { runs: [], nextPageToken: '', ...coordinates, error: filterError as string | null };
	try {
		const res = await orkstack(fetch).runs.listRuns({ namespace, pageSize: size, pageToken: cursor, filters: applied });
		return { runs: res.runs, nextPageToken: res.nextPageToken, ...coordinates, error: null as string | null };
	} catch (err) {
		return { runs: [], nextPageToken: '', ...coordinates, error: errorMessage(err) };
	}
};
