import { errorMessage } from '#lib/client.ts';
import { accessClient } from '#lib/clients/access.ts';
import type { AccessRevision } from '#lib/gen/orkstack/v1/access_pb.ts';
import type { PageLoad } from './$types';

// The document's revisions, newest first.
export const load: PageLoad = async ({ fetch, parent }) => {
	const { mode } = await parent();
	if (mode === 'asserted') return { revisions: [] as AccessRevision[], revisionsError: null as string | null };
	try {
		return { revisions: (await accessClient(fetch).listAccessRevisions({})).revisions, revisionsError: null as string | null };
	} catch (err) {
		return { revisions: [] as AccessRevision[], revisionsError: errorMessage(err) };
	}
};
