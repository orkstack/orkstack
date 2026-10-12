import { ConnectError, type Transport } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';

// All platform access goes through the clients generated from
// proto/orkstack/v1 (see ui/src/lib/gen). The browser uses same-origin URLs;
// src/hooks.server.ts proxies Connect requests to the engine.
//
// There is one client module per service, under src/lib/clients/ —
// orkflowClient, runClient, activityClient, webhookClient, triggerClient,
// scheduleClient, namespaceClient, statsClient, cacheClient, systemClient,
// auditClient, automationClient — and each imports only its own service's
// generated file. Every service has its own .proto (and so its own _pb
// module), so a route that reads runs never loads the schemas of the six
// services it does not call. Do not reintroduce a factory that builds every
// client at once: one import of it would put every service's schemas back on
// every route.
export function transport(fetchFn: typeof globalThis.fetch): Transport {
	return createConnectTransport({ baseUrl: '/', fetch: fetchFn });
}

export function errorMessage(err: unknown): string {
	if (err instanceof ConnectError) return err.message;
	if (err instanceof Error) return err.message;
	return String(err);
}
