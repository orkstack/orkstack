import { errorMessage } from '#lib/client.ts';
import { accessClient } from '#lib/clients/access.ts';
import type { Permission } from '#lib/gen/orkstack/v1/access_pb.ts';
import type { PageLoad } from './$types';

// The catalogue, for the picker and for what each permission allows.
export const load: PageLoad = async ({ fetch, parent }) => {
	const { mode } = await parent();
	if (mode === 'asserted') return { permissions: [] as Permission[], permissionsError: null as string | null };
	try {
		return { permissions: (await accessClient(fetch).listPermissions({})).permissions, permissionsError: null as string | null };
	} catch (err) {
		return { permissions: [] as Permission[], permissionsError: errorMessage(err) };
	}
};
