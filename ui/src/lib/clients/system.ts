import { createClient, type Client } from '@connectrpc/connect';
import { SystemService } from '../gen/orkstack/v1/system_service_pb';
import { transport } from '../client';

// GetSystemInfo — the one admin read that describes the REPLICA THAT
// ANSWERED rather than the cluster or the store — retention, telemetry sinks,
// image policy and releases.
//
// This module imports SystemService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type SystemClient = Client<typeof SystemService>;

export function systemClient(fetchFn: typeof globalThis.fetch): SystemClient {
	return createClient(SystemService, transport(fetchFn));
}
