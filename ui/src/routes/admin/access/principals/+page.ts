import { errorMessage } from '#lib/client.ts';
import { accessClient } from '#lib/clients/access.ts';
import type { Signin } from '#lib/gen/orkstack/v1/access_pb.ts';
import type { PageLoad } from './$types';

// The sign-ins the engine holds, every principal's (access.read).
export const load: PageLoad = async ({ fetch, parent }) => {
	const { mode } = await parent();
	if (mode === 'asserted') return { signins: [] as Signin[], signinsError: null as string | null };
	try {
		return { signins: (await accessClient(fetch).listSignins({})).signins, signinsError: null as string | null };
	} catch (err) {
		return { signins: [] as Signin[], signinsError: errorMessage(err) };
	}
};
