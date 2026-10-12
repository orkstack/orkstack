import { createClient, type Client } from '@connectrpc/connect';
import { AutomationService } from '../gen/orkstack/v1/automation_pb';
import { transport } from '../client';

// Plans and exports a namespace's automation document. It writes nothing:
// the Automation page applies a plan by sending each step's request to the
// services that own it (src/lib/automation.ts).
//
// This module imports AutomationService and nothing else: a route that calls it loads
// that one service's schemas (src/lib/client.ts).
export type AutomationClient = Client<typeof AutomationService>;

export function automationClient(fetchFn: typeof globalThis.fetch): AutomationClient {
	return createClient(AutomationService, transport(fetchFn));
}
