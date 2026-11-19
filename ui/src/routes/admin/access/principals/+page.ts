import { Code, ConnectError } from '@connectrpc/connect';
import { errorMessage } from '#lib/client.ts';
import { accessClient } from '#lib/clients/access.ts';
import { PEOPLE_WINDOWS, peopleWindow, readSeen, type PeopleWindow, type SeenRead } from '#lib/accessPeople.ts';
import type { SeenPrincipal, Signin } from '#lib/gen/orkstack/v1/access_pb.ts';
import type { PageLoad } from './$types';

// The principals seen to sign in over the window the URL names, stepping
// down to a shorter one by itself when the engine refuses a window as too
// large to read (readSeen), and the sign-ins the engine holds (access.read).
// The seen list is read page by page up to PEOPLE_PAGES pages; a longer list
// says so rather than stopping silently. Each read's failure is its own: the
// sign-ins, the disabled and the bound principals are still listed.
const PEOPLE_PAGES = 4;
const PEOPLE_PAGE_SIZE = 500;

export const load: PageLoad = async ({ fetch, parent, url }) => {
	const { mode } = await parent();
	const asked = peopleWindow(url.searchParams.get('window'));
	const none: SeenRead = { asked, shown: null, seen: [] as SeenPrincipal[], truncated: false, error: null, tooLong: false };
	if (mode === 'asserted') return { asked, signins: [] as Signin[], signinsError: null as string | null, seenRead: none };
	const client = accessClient(fetch);
	const readWindow = async (span: PeopleWindow) => {
		const seen: SeenPrincipal[] = [];
		let pageToken = '';
		for (let page = 0; page < PEOPLE_PAGES; page += 1) {
			const resp = await client.listPrincipals({ window: { seconds: BigInt(PEOPLE_WINDOWS[span]), nanos: 0 }, pageSize: PEOPLE_PAGE_SIZE, pageToken });
			seen.push(...resp.principals);
			if (!resp.nextPageToken) return { seen, truncated: false };
			pageToken = resp.nextPageToken;
		}
		return { seen, truncated: true };
	};
	const tooLarge = (err: unknown) => ConnectError.from(err).code === Code.ResourceExhausted;
	const [signins, seenRead] = await Promise.all([
		client.listSignins({}).then(
			(r) => ({ signins: r.signins, error: null as string | null }),
			(err: unknown) => ({ signins: [] as Signin[], error: errorMessage(err) })
		),
		readSeen(asked, readWindow, tooLarge, errorMessage)
	]);
	return { asked, signins: signins.signins, signinsError: signins.error, seenRead };
};
