import { createClient, type Client } from '@connectrpc/connect';
import { CacheService } from '../gen/orkstack/v1/cache_pb';
import { transport } from '../client';

// The memos cached tasks reuse (docs/DSL.md "Caching"): stats derived on
// read, and a purge that deletes memos and nothing else.
//
// This module imports CacheService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type CacheClient = Client<typeof CacheService>;

export function cacheClient(fetchFn: typeof globalThis.fetch): CacheClient {
	return createClient(CacheService, transport(fetchFn));
}
