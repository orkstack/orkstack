import { EventType } from './gen/orkstack/v1/run_pb';

// The Admin → Webhooks page's pure rules, kept here so they are unit-tested
// and cannot drift from orkctl's (cmd/orkctl/webhook.go).

/** The run's own transitions: what a subscription with no events receives. */
export const RUN_EVENTS: EventType[] = [
	EventType.RUN_STARTED,
	EventType.RUN_SUCCEEDED,
	EventType.RUN_FAILED,
	EventType.RUN_CANCELED
];

/**
 * Task events: opt-in by name, always, in lifecycle order — an awaited task's
 * two sit where it reaches them, before its outcome. TASK_LOG is never offered.
 */
export const TASK_EVENTS: EventType[] = [
	EventType.TASK_SCHEDULED,
	EventType.TASK_STARTED,
	EventType.TASK_AWAITING,
	EventType.TASK_RESOLVED,
	EventType.TASK_SUCCEEDED,
	EventType.TASK_FAILED
];

/**
 * The slug rule (AGENTS.md §7) as an HTML `pattern` attribute, which the
 * browser anchors itself. The server stays the authority; this only catches
 * an obvious typo before the RPC.
 */
export const SCOPE_SLUG_PATTERN = '[a-z][a-z0-9-]*';

export interface WebhookScope {
	namespace: string;
	orkflowName: string;
	taskId: string;
}

/**
 * webhookScope renders a subscription's scope as orkctl's SCOPE column does —
 * namespace/orkflow/task with * for an unset component — and the all-empty
 * case in words.
 */
export function webhookScope(hook: WebhookScope): string {
	if (!hook.namespace && !hook.orkflowName && !hook.taskId) return 'Deployment-wide';
	const part = (value: string) => value || '*';
	return `${part(hook.namespace)}/${part(hook.orkflowName)}/${part(hook.taskId)}`;
}
