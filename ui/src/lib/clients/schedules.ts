import { createClient, type Client } from '@connectrpc/connect';
import { ScheduleService } from '../gen/orkstack/v1/schedule_pb';
import { transport } from '../client';

// The third run starter: a schedule starts runs of one orkflow at the times
// its spec names, and every field of it is read from the cluster on each call.
//
// This module imports ScheduleService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type ScheduleClient = Client<typeof ScheduleService>;

export function scheduleClient(fetchFn: typeof globalThis.fetch): ScheduleClient {
	return createClient(ScheduleService, transport(fetchFn));
}
