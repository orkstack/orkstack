import { createClient, type Client } from '@connectrpc/connect';
import { RunService } from '../gen/orkstack/v1/run_service_pb';
import { transport } from '../client';

// Runs: submit, read, cancel, stream, artifacts and evidence.
//
// This module imports RunService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type RunClient = Client<typeof RunService>;

export function runClient(fetchFn: typeof globalThis.fetch): RunClient {
	return createClient(RunService, transport(fetchFn));
}
