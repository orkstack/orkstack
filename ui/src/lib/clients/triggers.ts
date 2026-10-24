import { createClient, type Client } from '@connectrpc/connect';
import { TriggerService } from '../gen/orkstack/v1/trigger_service_pb';
import { transport } from '../client';

// The inbound counterpart to webhooks: a trigger is bound to one
// orkflow+namespace rather than a deployment-wide subscription.
//
// This module imports TriggerService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type TriggerClient = Client<typeof TriggerService>;

export function triggerClient(fetchFn: typeof globalThis.fetch): TriggerClient {
	return createClient(TriggerService, transport(fetchFn));
}
