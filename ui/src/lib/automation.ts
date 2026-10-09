import { clone } from '@bufbuild/protobuf';
import {
	AutomationAction,
	AutomationObjectKind,
	AutomationObjectKindSchema,
	type AutomationStep,
	type PlanAutomationResponse
} from './gen/orkstack/v1/automation_pb';
import { RegisterWebhookRequestSchema, ValidationSeverity } from './gen/orkstack/v1/service_pb';
import { EventType } from './gen/orkstack/v1/run_pb';
import type { orkstack } from './client';
import { shellQuote } from './runDetail';

// The Admin → Automation page's pure rules and its executor, kept here so they
// are unit-tested and cannot drift from orkctl's (cmd/orkctl/apply_automation.go).
//
// The engine PLANS (AutomationService.PlanAutomation) and writes nothing; this
// page, like `orkctl apply`, EXECUTES: it sends each actionable step's carried
// request to the existing RPC it names, in order, so every write is the same
// validated, audited call a person could make by hand.

/**
 * A kind's client spelling: the value's name after AUTOMATION_OBJECT_KIND_,
 * lowercased — namespace, schedule, trigger, webhook. Derived from the
 * descriptor (as run origin kinds are), so a kind added to the proto needs no edit
 * here.
 */
export function automationKindSlug(kind: AutomationObjectKind): string {
	if (kind === AutomationObjectKind.UNSPECIFIED) return '';
	const value = AutomationObjectKindSchema.values.find((entry) => entry.number === kind);
	return value ? value.name.replace(/^AUTOMATION_OBJECT_KIND_/, '').toLowerCase() : '';
}

/** The document section a kind lives in. */
export function automationSectionKey(kind: AutomationObjectKind): string {
	switch (kind) {
		case AutomationObjectKind.NAMESPACE:
			return 'settings';
		case AutomationObjectKind.SCHEDULE:
			return 'schedules';
		case AutomationObjectKind.TRIGGER:
			return 'triggers';
		case AutomationObjectKind.WEBHOOK:
			return 'webhooks';
	}
	return '';
}

/** The plan's one-character column, the one `orkctl apply --dry-run` prints. */
export function automationActionSymbol(action: AutomationAction): string {
	switch (action) {
		case AutomationAction.CREATE:
			return '+';
		case AutomationAction.UPDATE:
			return '~';
		case AutomationAction.DELETE:
			return '-';
		case AutomationAction.UNCHANGED:
			return ' ';
		case AutomationAction.UNDECLARED:
			return '?';
		case AutomationAction.UNMANAGED:
			return '!';
	}
	return '';
}

/** The action as a word, beside its symbol. */
export function automationActionWord(action: AutomationAction): string {
	switch (action) {
		case AutomationAction.CREATE:
			return 'create';
		case AutomationAction.UPDATE:
			return 'update';
		case AutomationAction.DELETE:
			return 'delete';
		case AutomationAction.UNCHANGED:
			return 'unchanged';
		case AutomationAction.UNDECLARED:
			return 'not in this document; kept';
		case AutomationAction.UNMANAGED:
			return 'unmanaged';
	}
	return '';
}

/** CREATE, UPDATE and DELETE carry a request; nothing else writes. */
export function isActionable(action: AutomationAction): boolean {
	return (
		action === AutomationAction.CREATE ||
		action === AutomationAction.UPDATE ||
		action === AutomationAction.DELETE
	);
}

/** Whether a plan carries any ERROR issue — Apply is disabled while it does. */
export function planHasErrors(plan: Pick<PlanAutomationResponse, 'issues'>): boolean {
	return plan.issues.some((issue) => issue.severity === ValidationSeverity.ERROR);
}

/**
 * Whether Apply's fresh re-plan must stop the apply — `orkctl apply
 * --expect-plan`'s refusal: no digest, a digest other than the one shown, or
 * any ERROR. The digest covers the actionable steps only, so an ERROR that
 * appeared since the plan was shown (a referenced Orkflow deleted, say) leaves
 * it unchanged; the issues must be checked on their own.
 */
export function freshPlanRefused(shownDigest: string, fresh: PlanAutomationResponse): boolean {
	return fresh.planDigest === '' || fresh.planDigest !== shownDigest || planHasErrors(fresh);
}

/** The plan's counts line: "1 to create, 3 to update, 0 to delete." */
export function planCounts(steps: AutomationStep[]): string {
	const count = (action: AutomationAction) => steps.filter((step) => step.action === action).length;
	return `${count(AutomationAction.CREATE)} to create, ${count(AutomationAction.UPDATE)} to update, ${count(AutomationAction.DELETE)} to delete.`;
}

/** What executing one step did. */
export interface StepExecution {
	/** The object's id after the step: a CREATE's new id. */
	id: string;
	/** The one-time secret a CREATE disclosed: a trigger token or a generated webhook secret. */
	secret: string;
}

/** The services a step can name. */
export type AutomationWriters = Pick<
	ReturnType<typeof orkstack>,
	'namespaces' | 'schedules' | 'triggers' | 'webhooks'
>;

/**
 * executeStep sends one step's carried request to the RPC it names — the TS
 * twin of orkctl's executor, one case per variant of AutomationStep.request.
 * A webhook create whose entry named secret_env takes its signing secret from
 * `secrets` (the page's password input for that entry); that secret is the
 * operator's own and is never disclosed back.
 */
export async function executeStep(
	client: AutomationWriters,
	step: AutomationStep,
	secrets: Record<string, string>
): Promise<StepExecution> {
	const request = step.request;
	switch (request.case) {
		case 'createNamespace':
			await client.namespaces.createNamespace(request.value);
			return { id: '', secret: '' };
		case 'updateNamespace':
			await client.namespaces.updateNamespace(request.value);
			return { id: '', secret: '' };
		case 'createSchedule': {
			const response = await client.schedules.createSchedule(request.value);
			return { id: response.schedule?.id ?? '', secret: '' };
		}
		case 'updateSchedule':
			await client.schedules.updateSchedule(request.value);
			return { id: step.id, secret: '' };
		case 'deleteSchedule':
			await client.schedules.deleteSchedule(request.value);
			return { id: step.id, secret: '' };
		case 'createTrigger': {
			const response = await client.triggers.createTrigger(request.value);
			return { id: response.trigger?.id ?? '', secret: response.secret };
		}
		case 'updateTrigger':
			await client.triggers.updateTrigger(request.value);
			return { id: step.id, secret: '' };
		case 'deleteTrigger':
			await client.triggers.deleteTrigger(request.value);
			return { id: step.id, secret: '' };
		case 'registerWebhook': {
			const message = clone(RegisterWebhookRequestSchema, request.value);
			if (step.secretEnv) message.secret = secrets[step.name] ?? '';
			const response = await client.webhooks.registerWebhook(message);
			return { id: response.webhook?.id ?? '', secret: step.secretEnv ? '' : response.secret };
		}
		case 'updateWebhook':
			await client.webhooks.updateWebhook(request.value);
			return { id: step.id, secret: '' };
		case 'deleteWebhook':
			await client.webhooks.deleteWebhook(request.value);
			return { id: step.id, secret: '' };
	}
	throw new Error(
		`step ${automationActionWord(step.action)} ${automationKindSlug(step.kind)} ${step.name} carries no request this page can send`
	);
}

const DOCUMENT = 'automation.ork.yaml';

/** `orkctl apply automation.ork.yaml --dry-run -n NS [--prune]`. */
export function automationPlanCli(namespace: string, prune: boolean): string {
	return ['orkctl', 'apply', DOCUMENT, '--dry-run', '-n', shellQuote(namespace || 'default')]
		.concat(prune ? ['--prune'] : [])
		.join(' ');
}

/** The apply the page just performed, pinned to the plan it showed. */
export function automationApplyCli(namespace: string, prune: boolean, digest: string): string {
	return ['orkctl', 'apply', DOCUMENT, '-n', shellQuote(namespace || 'default')]
		.concat(prune ? ['--prune'] : [])
		.concat(['--expect-plan', shellQuote(digest)])
		.join(' ');
}

/** `orkctl export -n NS > automation.ork.yaml`: the adoption command. */
export function automationExportCli(namespace: string): string {
	return `orkctl export -n ${shellQuote(namespace || 'default')} > ${DOCUMENT}`;
}

/** The edit a trigger dialog makes, as `orkctl trigger update`. */
export function triggerUpdateCli(
	id: string,
	namespace: string,
	edit: { name: string; version: string; description: string }
): string {
	const args = ['orkctl', 'trigger', 'update', shellQuote(id), '-n', shellQuote(namespace || 'default')];
	args.push('--name', shellQuote(edit.name));
	if (edit.version) args.push('--version', shellQuote(edit.version));
	else args.push('--follow-current');
	args.push('--description', shellQuote(edit.description));
	return args.join(' ');
}

/** The edit a webhook dialog makes, as `orkctl webhook update`. */
export function webhookUpdateCli(
	id: string,
	edit: {
		name: string;
		url: string;
		events: EventType[];
		namespace: string;
		orkflowName: string;
		taskId: string;
	}
): string {
	const args = ['orkctl', 'webhook', 'update', shellQuote(id)];
	args.push('--name', shellQuote(edit.name), '--url', shellQuote(edit.url));
	if (edit.events.length === 0) args.push('--clear-events');
	for (const event of edit.events) args.push('--event', shellQuote(EventType[event]));
	if (edit.namespace) args.push('-n', shellQuote(edit.namespace));
	else args.push('--deployment-wide');
	args.push('--orkflow', shellQuote(edit.orkflowName), '--task', shellQuote(edit.taskId));
	return args.join(' ');
}
