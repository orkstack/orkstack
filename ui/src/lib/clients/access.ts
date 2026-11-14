import { createClient, type Client } from '@connectrpc/connect';
import { AccessService } from '../gen/orkstack/v1/access_service_pb';
import { transport } from '../client';

// The access document: roles, bindings, service accounts and disabled
// principals, with the catalogue, explain, plan and export, and its
// revisions. Every write sends back the etag it read.
//
// This module imports AccessService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type AccessClient = Client<typeof AccessService>;

export function accessClient(fetchFn: typeof globalThis.fetch): AccessClient {
	return createClient(AccessService, transport(fetchFn));
}
