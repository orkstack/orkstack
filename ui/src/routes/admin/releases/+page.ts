import { errorMessage, orkstack } from '$lib/client';
import type { ListReleasesResponse } from '$lib/gen/orkstack/v1/service_pb';
import { filtersFromUrl } from '$lib/listFilter';
import type { PageLoad } from './$types';

// ListReleases is the deployment's releases as the cluster routes them
// (proto/orkstack/v1/service.proto's SystemService, docs/DEPLOY.md §3). A
// release is deployment-wide, so this loader takes no namespace out of the
// URL, exactly like ui/src/routes/admin/retention/+page.ts: the namespace
// switcher above does not scope this page.
//
// The URL's `filter` parameters select release rows (ListFilter); every other
// field of the answer is the deployment's, unfiltered.
export const load: PageLoad = async ({ fetch, url }) => {
	const { filters: applied, error: filterError } = filtersFromUrl(url);
	if (filterError) return { applied, releases: null as ListReleasesResponse | null, releasesError: filterError as string | null };
	try {
		const res = await orkstack(fetch).system.listReleases({ filters: applied });
		return { applied, releases: res, releasesError: null as string | null };
	} catch (err) {
		return { applied, releases: null as ListReleasesResponse | null, releasesError: errorMessage(err) };
	}
};
