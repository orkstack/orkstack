import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { ConnectError } from '@connectrpc/connect';
import {
	OrkflowService,
	RunService,
	ActivityService,
	WebhookService,
	NamespaceService,
	SystemService
} from './gen/orkstack/v1/service_pb';
import { StatsService } from './gen/orkstack/v1/stats_pb';

// All platform access goes through the clients generated from
// proto/orkstack/v1 (see ui/src/lib/gen). The browser uses same-origin URLs;
// src/hooks.server.ts proxies Connect requests to the engine.
export function orkstack(fetchFn: typeof globalThis.fetch) {
	const transport = createConnectTransport({ baseUrl: '/', fetch: fetchFn });
	return {
		orkflows: createClient(OrkflowService, transport),
		runs: createClient(RunService, transport),
		// activities answers the catalog (§4.2 — which images this deployment
		// can run is engine configuration, not something to ask an Orker) and
		// DescribeOrker, the pool-liveness probe (§6.5).
		activities: createClient(ActivityService, transport),
		webhooks: createClient(WebhookService, transport),
		namespaces: createClient(NamespaceService, transport),
		stats: createClient(StatsService, transport),
		// system answers GetSystemInfo — the one admin read that describes the
		// REPLICA THAT ANSWERED rather than the cluster or the store. It is its
		// own client because it is its own service:
		// folding it into namespaces or stats would misname what scopes it.
		system: createClient(SystemService, transport)
	};
}

export function errorMessage(err: unknown): string {
	if (err instanceof ConnectError) return err.message;
	if (err instanceof Error) return err.message;
	return String(err);
}
