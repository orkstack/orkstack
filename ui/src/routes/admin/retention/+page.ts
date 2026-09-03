import { errorMessage, orkstack } from '$lib/client';
import type { GetRetentionPolicyResponse } from '$lib/gen/orkstack/v1/service_pb';
import type { PageLoad } from './$types';

// GetRetentionPolicy reads the deployment's maximum run retention: the
// ceiling on every namespace's own run_retention, and what a namespace that
// sets none inherits (proto/orkstack/v1/service.proto's SystemService). This
// loader therefore takes no namespace out of the URL either, exactly like
// ui/src/routes/admin/+page.ts: the namespace switcher above does not scope
// this page at all — a namespace's own value is set on /admin/namespaces.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await orkstack(fetch).system.getRetentionPolicy({});
		return { retention: res, retentionError: null as string | null };
	} catch (err) {
		return {
			retention: null as GetRetentionPolicyResponse | null,
			retentionError: errorMessage(err)
		};
	}
};
