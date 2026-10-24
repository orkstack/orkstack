import { createClient, type Client } from '@connectrpc/connect';
import { StatsService } from '../gen/orkstack/v1/stats_pb';
import { transport } from '../client';

// Run statistics, usage, flakes and failure breakdowns.
//
// This module imports StatsService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type StatsClient = Client<typeof StatsService>;

export function statsClient(fetchFn: typeof globalThis.fetch): StatsClient {
	return createClient(StatsService, transport(fetchFn));
}
