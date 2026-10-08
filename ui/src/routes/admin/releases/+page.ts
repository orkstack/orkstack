import { errorMessage, orkstack } from '$lib/client';
import type { ListReleasesResponse } from '$lib/gen/orkstack/v1/service_pb';
import type { PageLoad } from './$types';

// ListReleases is the deployment's releases as the cluster routes them
// (proto/orkstack/v1/service.proto's SystemService, docs/DEPLOY.md §3). A
// release is deployment-wide, so this loader takes no namespace out of the
// URL, exactly like ui/src/routes/admin/retention/+page.ts: the namespace
// switcher above does not scope this page.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await orkstack(fetch).system.listReleases({});
		return { releases: res, releasesError: null as string | null };
	} catch (err) {
		return { releases: null as ListReleasesResponse | null, releasesError: errorMessage(err) };
	}
};
