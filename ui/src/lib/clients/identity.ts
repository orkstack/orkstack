import { createClient, type Client } from '@connectrpc/connect';
import { IdentityService } from '../gen/orkstack/v1/identity_service_pb';
import { transport } from '../client';

// The task identity issuer: its documents and its signing keys
// (docs/TASK_IDENTITY.md).
//
// This module imports IdentityService and nothing else: a route that calls it
// loads that one service's schemas (src/lib/client.ts).
export type IdentityClient = Client<typeof IdentityService>;

export function identityClient(fetchFn: typeof globalThis.fetch): IdentityClient {
	return createClient(IdentityService, transport(fetchFn));
}
