import { createClient, type Client } from '@connectrpc/connect';
import { NamespaceService } from '../gen/orkstack/v1/namespace_service_pb';
import { transport } from '../client';

// Namespaces and the pools they may dispatch onto.
//
// This module imports NamespaceService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type NamespaceClient = Client<typeof NamespaceService>;

export function namespaceClient(fetchFn: typeof globalThis.fetch): NamespaceClient {
	return createClient(NamespaceService, transport(fetchFn));
}
