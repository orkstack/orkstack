import { errorMessage, orkstack } from '$lib/client';
import type { ActivityInfo } from '$lib/gen/orkstack/v1/service_pb';
import type { DescribeOrkerResult } from '$lib/gen/orkstack/v1/activity_pb';
import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// The activity catalog is GLOBAL: what this deployment can run, which is
// engine configuration (§4.2), not something scoped by namespace and
// not something asked of an Orker. `namespace` still travels so a link out of
// this page can carry the switcher's choice.
//
// The catalog and the Orker description are read INDEPENDENTLY and reported
// independently. They answer different questions from different places — the
// catalog is engine configuration, DescribeOrker is a Temporal activity
// dispatched onto the default pool's channel (§6.5) — so a deployment with no
// Orker running must still show its catalog, and an unresolvable registry must
// not hide a healthy Orker. Folding them into one try/catch would have made
// either failure erase the other's answer.
export const load: PageLoad = async ({ fetch, url }) => {
	const namespace = namespaceFrom(url);
	const client = orkstack(fetch);

	const [catalog, orker] = await Promise.all([
		client.activities
			.listActivities({})
			.then((res) => ({ activities: res.activities, error: null as string | null }))
			.catch((err) => ({ activities: [] as ActivityInfo[], error: errorMessage(err) })),
		client.activities
			.describeOrker({})
			.then((res) => ({
				orker: res.orker ?? null,
				error: null as string | null
			}))
			.catch((err) => ({ orker: null as DescribeOrkerResult | null, error: errorMessage(err) }))
	]);

	return {
		namespace,
		activities: catalog.activities,
		activitiesError: catalog.error,
		orker: orker.orker,
		orkerError: orker.error
	};
};
