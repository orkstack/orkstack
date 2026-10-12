import { createClient, type Client } from '@connectrpc/connect';
import { ActivityService } from '../gen/orkstack/v1/activity_service_pb';
import { transport } from '../client';

// The catalog (§4.2 — which images this deployment can run is engine
// configuration, not something to ask an Orker), DescribeOrker (the
// pool-liveness probe, §6.5) and the Orker inventory.
//
// This module imports ActivityService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type ActivityClient = Client<typeof ActivityService>;

export function activityClient(fetchFn: typeof globalThis.fetch): ActivityClient {
	return createClient(ActivityService, transport(fetchFn));
}
