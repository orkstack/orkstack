import { createClient, type Client } from '@connectrpc/connect';
import { ConcurrencyService } from '../gen/orkstack/v1/concurrency_service_pb';
import { transport } from '../client';

// A namespace's concurrency limits: who holds each key and who waits.
//
// This module imports ConcurrencyService and nothing else: a route that calls it
// loads that one service's schemas (src/lib/client.ts).
export type ConcurrencyClient = Client<typeof ConcurrencyService>;

export function concurrencyClient(fetchFn: typeof globalThis.fetch): ConcurrencyClient {
	return createClient(ConcurrencyService, transport(fetchFn));
}
