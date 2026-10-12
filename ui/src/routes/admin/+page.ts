import { errorMessage } from '$lib/client';
import { systemClient } from '$lib/clients/system';
import type { GetSystemInfoResponse } from '$lib/gen/orkstack/v1/system_service_pb';
import type { PageLoad } from './$types';

// GetSystemInfo describes THE REPLICA THAT ANSWERED
// (proto/orkstack/v1/service.proto) — it reads no namespace, no run, no
// orkflow. The namespace
// switcher does not scope this page at all, which is why the loader takes no
// namespace out of the URL: doing so would imply a filter this read has none.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await systemClient(fetch).getSystemInfo({});
		return { system: res, systemError: null as string | null };
	} catch (err) {
		return {
			system: null as GetSystemInfoResponse | null,
			systemError: errorMessage(err)
		};
	}
};
