import { errorMessage, orkstack } from '$lib/client';
import type { GetExecutionRetentionResponse } from '$lib/gen/orkstack/v1/service_pb';
import type { PageLoad } from './$types';

// GetExecutionRetention reads THIS DEPLOYMENT'S ONE Temporal cluster
// namespace's workflow-execution retention, live from the cluster
// (proto/orkstack/v1/service.proto's SystemService) — a different question
// from GetSystemInfo, but the SAME scope: deployment-wide, not per
// orkstack.v1.Namespace. This loader therefore takes no namespace out of the
// URL either, exactly like ui/src/routes/admin/+page.ts: the namespace
// switcher above does not scope this page at all.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await orkstack(fetch).system.getExecutionRetention({});
		return { retention: res, retentionError: null as string | null };
	} catch (err) {
		return {
			retention: null as GetExecutionRetentionResponse | null,
			retentionError: errorMessage(err)
		};
	}
};
