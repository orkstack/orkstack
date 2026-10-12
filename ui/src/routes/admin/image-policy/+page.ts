import { errorMessage } from '$lib/client';
import { systemClient } from '$lib/clients/system';
import type { ImagePolicy } from '$lib/gen/orkstack/v1/image_policy_pb';
import type { PageLoad } from './$types';

// GetImagePolicy reads the deployment's image supply-chain floor
// (proto/orkstack/v1/service.proto's SystemService). Like
// admin/retention/+page.ts, this loader takes no namespace out of the URL:
// the namespace switcher above does not scope this page — a namespace's own
// tightening (require_signed_by) is set on /admin/namespaces.
export const load: PageLoad = async ({ fetch }) => {
	try {
		const res = await systemClient(fetch).getImagePolicy({});
		return { imagePolicy: res.policy ?? null, imagePolicyError: null as string | null };
	} catch (err) {
		return { imagePolicy: null as ImagePolicy | null, imagePolicyError: errorMessage(err) };
	}
};
