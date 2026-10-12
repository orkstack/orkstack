import { createClient, type Client } from '@connectrpc/connect';
import { AuditService } from '../gen/orkstack/v1/audit_pb';
import { transport } from '../client';

// The control-plane audit trail: one immutable record per write RPC, from
// the shared store. It is itself a read.
//
// This module imports AuditService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type AuditClient = Client<typeof AuditService>;

export function auditClient(fetchFn: typeof globalThis.fetch): AuditClient {
	return createClient(AuditService, transport(fetchFn));
}
