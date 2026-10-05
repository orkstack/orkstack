import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { ConnectError } from '@connectrpc/connect';
import {
	OrkflowService,
	RunService,
	ActivityService,
	WebhookService,
	TriggerService,
	NamespaceService,
	SystemService
} from './gen/orkstack/v1/service_pb';
import { StatsService } from './gen/orkstack/v1/stats_pb';
import { AuditService } from './gen/orkstack/v1/audit_pb';
import { ScheduleService } from './gen/orkstack/v1/schedule_pb';
import { AutomationService } from './gen/orkstack/v1/automation_pb';
import { CacheService } from './gen/orkstack/v1/cache_pb';

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
		// triggers is the inbound counterpart to webhooks: one bound to a
		// specific orkflow+namespace rather than a deployment-wide subscription.
		triggers: createClient(TriggerService, transport),
		// schedules is the third run starter: a schedule starts runs of one
		// orkflow at the times its spec names, and every field of it is read
		// from the cluster on each call.
		schedules: createClient(ScheduleService, transport),
		namespaces: createClient(NamespaceService, transport),
		stats: createClient(StatsService, transport),
		// cache reads and trims the memos cached tasks reuse (docs/DSL.md
		// "Caching"): stats derived on read, and a purge that deletes memos
		// and nothing else.
		cache: createClient(CacheService, transport),
		// system answers GetSystemInfo — the one admin read that describes the
		// REPLICA THAT ANSWERED rather than the cluster or the store. It is its
		// own client because it is its own service:
		// folding it into namespaces or stats would misname what scopes it.
		system: createClient(SystemService, transport),
		// audit reads the control-plane audit trail back: one immutable
		// record per write RPC, from the shared store. It is itself a read.
		audit: createClient(AuditService, transport),
		// automation plans and exports a namespace's automation document. It
		// writes nothing: the Automation page applies a plan by sending each
		// step's request to the services above (src/lib/automation.ts).
		automation: createClient(AutomationService, transport)
	};
}

export function errorMessage(err: unknown): string {
	if (err instanceof ConnectError) return err.message;
	if (err instanceof Error) return err.message;
	return String(err);
}
