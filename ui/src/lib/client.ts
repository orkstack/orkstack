import { createClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { ConnectError } from '@connectrpc/connect';
import {
	OrkflowService,
	RunService,
	PluginService,
	NamespaceService
} from './gen/orkstack/v1/service_pb';

// All platform access goes through the clients generated from
// proto/orkstack/v1 (see ui/src/lib/gen). The browser uses same-origin URLs;
// src/hooks.server.ts proxies Connect requests to the engine.
export function orkstack(fetchFn: typeof globalThis.fetch) {
	const transport = createConnectTransport({ baseUrl: '/', fetch: fetchFn });
	return {
		orkflows: createClient(OrkflowService, transport),
		runs: createClient(RunService, transport),
		plugins: createClient(PluginService, transport),
		namespaces: createClient(NamespaceService, transport)
	};
}

export function errorMessage(err: unknown): string {
	if (err instanceof ConnectError) return err.message;
	if (err instanceof Error) return err.message;
	return String(err);
}
