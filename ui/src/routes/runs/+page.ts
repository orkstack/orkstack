import { errorMessage, orkstack } from '$lib/client';
import { namespaceFrom } from '$lib/namespace';
import { instant, pageNumber, pageSize, runFilterError, statusFromParam } from '$lib/browsing';
import { originKindFromSlug } from '$lib/origin';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch, url }) => {
	const orkflowName = url.searchParams.get('orkflow') ?? '';
	const namespace = namespaceFrom(url);
	const status = url.searchParams.get('status') ?? '';
	const size = pageSize(url.searchParams.get('size') ?? '');
	const cursor = url.searchParams.get('cursor') ?? '';
	// The page number is a LABEL for the cursor beside it, counted by the
	// client because the engine answers with a token and no position. A number
	// without a cursor names no page the engine could return, so it is 1.
	const page = cursor ? pageNumber(url.searchParams.get('page') ?? '') : 1;
	const createdAfter = url.searchParams.get('since') ?? '';
	const createdBefore = url.searchParams.get('until') ?? '';
	// What started the run: a kind slug and, within it, one originating
	// object's id. Both ride the URL like every other filter.
	const origin = url.searchParams.get('origin') ?? '';
	const originId = url.searchParams.get('originId') ?? '';
	const coordinates = { orkflowName, namespace, status, size, cursor, page, createdAfter, createdBefore, origin, originId };
	const filterError = runFilterError(status, createdAfter, createdBefore, origin, originId);
	if (filterError) return { runs: [], nextPageToken: '', ...coordinates, error: filterError };
	try {
		const res = await orkstack(fetch).runs.listRuns({ orkflowName, namespace, status: statusFromParam(status), pageSize: size, pageToken: cursor, createdAfter: instant(createdAfter), createdBefore: instant(createdBefore), originKind: originKindFromSlug(origin), originId });
		return { runs: res.runs, nextPageToken: res.nextPageToken, ...coordinates, error: null as string | null };
	} catch (err) {
		return { runs: [], nextPageToken: '', ...coordinates, error: errorMessage(err) };
	}
};
