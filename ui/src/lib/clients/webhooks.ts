import { createClient, type Client } from '@connectrpc/connect';
import { WebhookService } from '../gen/orkstack/v1/webhook_service_pb';
import { transport } from '../client';

// Run-event subscriptions and their delivery records.
//
// This module imports WebhookService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type WebhookClient = Client<typeof WebhookService>;

export function webhookClient(fetchFn: typeof globalThis.fetch): WebhookClient {
	return createClient(WebhookService, transport(fetchFn));
}
