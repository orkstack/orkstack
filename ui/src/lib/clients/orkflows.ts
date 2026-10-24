import { createClient, type Client } from '@connectrpc/connect';
import { OrkflowService } from '../gen/orkstack/v1/orkflow_service_pb';
import { transport } from '../client';

// Orkflow definitions: validate, apply, versions, plans.
//
// This module imports OrkflowService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type OrkflowClient = Client<typeof OrkflowService>;

export function orkflowClient(fetchFn: typeof globalThis.fetch): OrkflowClient {
	return createClient(OrkflowService, transport(fetchFn));
}
