import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import {
	AutomationAction,
	AutomationActionSchema,
	AutomationObjectKind,
	AutomationObjectKindSchema,
	AutomationStepSchema,
	PlanAutomationResponseSchema
} from '../src/lib/gen/orkstack/v1/automation_pb';
import { ValidationIssueSchema, ValidationSeverity } from '../src/lib/gen/orkstack/v1/orkflow_service_pb';
import { EventType } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	automationActionSymbol,
	automationApplyCli,
	automationExportCli,
	automationKindSlug,
	automationPlanCli,
	automationSectionKey,
	executeStep,
	freshPlanRefused,
	isActionable,
	planHasErrors,
	triggerUpdateCli,
	webhookUpdateCli,
	type AutomationWriters
} from '../src/lib/automation';

// The descriptor walk: a kind added to the proto without a slug fails here,
// exactly as TestAutomationKindAndActionSlugs_MatchTheEnums fails in Go.
test('every declared kind has the slug its enum name derives', () => {
	for (const value of AutomationObjectKindSchema.values) {
		const slug = automationKindSlug(value.number);
		if (value.number === AutomationObjectKind.UNSPECIFIED) {
			expect(slug).toBe('');
			continue;
		}
		expect(slug).toBe(value.name.replace(/^AUTOMATION_OBJECT_KIND_/, '').toLowerCase());
		expect(automationSectionKey(value.number)).not.toBe('');
	}
});

test('every declared action has a symbol, and only create/update/delete are actionable', () => {
	const want: Record<string, string> = {
		CREATE: '+',
		UPDATE: '~',
		DELETE: '-',
		UNCHANGED: ' ',
		UNDECLARED: '?',
		UNMANAGED: '!'
	};
	for (const value of AutomationActionSchema.values) {
		if (value.number === AutomationAction.UNSPECIFIED) continue;
		const name = value.name.replace(/^AUTOMATION_ACTION_/, '');
		expect(automationActionSymbol(value.number)).toBe(want[name]);
		expect(isActionable(value.number)).toBe(['CREATE', 'UPDATE', 'DELETE'].includes(name));
	}
});

// A fake client that records which method each step reached.
function recordingClient(calls: string[]): AutomationWriters {
	const service = (name: string) =>
		new Proxy(
			{},
			{
				get: (_target, method) => async () => {
					calls.push(`${name}.${String(method)}`);
					return { secret: 'disclosed', trigger: { id: 'trg-1' }, webhook: { id: 'wh-1' }, schedule: { id: 'sch-1' } };
				}
			}
		);
	return {
		namespaces: service('namespaces'),
		schedules: service('schedules'),
		triggers: service('triggers'),
		webhooks: service('webhooks')
	} as unknown as AutomationWriters;
}

test('executeStep has a case for every request oneof variant', async () => {
	const oneof = AutomationStepSchema.oneofs.find((o) => o.name === 'request');
	expect(oneof === undefined).toBe(false);
	expect(oneof!.fields.length).toBe(11);
	for (const field of oneof!.fields) {
		const calls: string[] = [];
		const step = create(AutomationStepSchema, {
			name: 'x',
			request: { case: field.localName, value: create(field.message!) } as never
		});
		await executeStep(recordingClient(calls), step, {});
		expect(calls.length).toBe(1);
	}
});

test('a secret_env webhook takes its secret from the form and is never disclosed back', async () => {
	let sent = '';
	const client = {
		webhooks: {
			registerWebhook: async (req: { secret: string }) => {
				sent = req.secret;
				return { webhook: { id: 'wh-9' }, secret: req.secret };
			}
		}
	} as unknown as AutomationWriters;
	const step = create(AutomationStepSchema, {
		name: 'etl-loader',
		secretEnv: 'ETL_WEBHOOK_SECRET',
		request: { case: 'registerWebhook', value: { url: 'https://x.test/' } }
	});
	const result = await executeStep(client, step, { 'etl-loader': 'typed-secret' });
	expect(sent).toBe('typed-secret');
	expect(result).toEqual({ id: 'wh-9', secret: '' });
	expect(step.request.case === 'registerWebhook' && step.request.value.secret).toBe('');
});

test('the CLI builders quote every value through shellQuote', () => {
	expect(automationPlanCli("o'brien", true)).toBe(
		`orkctl apply automation.ork.yaml --dry-run -n 'o'"'"'brien' --prune`
	);
	expect(automationPlanCli('', false)).toBe(`orkctl apply automation.ork.yaml --dry-run -n 'default'`);
	expect(automationApplyCli('payments', false, 'sha256:ab')).toBe(
		`orkctl apply automation.ork.yaml -n 'payments' --expect-plan 'sha256:ab'`
	);
	expect(automationExportCli('payments')).toBe(`orkctl export -n 'payments' > automation.ork.yaml`);
	expect(triggerUpdateCli('trg-1', 'payments', { name: 'gh', version: '', description: "it's" })).toBe(
		`orkctl trigger update 'trg-1' -n 'payments' --name 'gh' --follow-current --description 'it'"'"'s'`
	);
	expect(
		webhookUpdateCli('wh-1', {
			name: 'etl',
			url: 'https://x.test/',
			events: [EventType.RUN_FAILED],
			namespace: '',
			orkflowName: '',
			taskId: ''
		})
	).toBe(
		`orkctl webhook update 'wh-1' --name 'etl' --url 'https://x.test/' --event 'RUN_FAILED' --deployment-wide --orkflow '' --task ''`
	);
});

// Apply's re-plan refuses exactly what `orkctl apply --expect-plan` refuses —
// including an ERROR that appeared since the plan was shown, which leaves the
// digest (actionable steps only) unchanged.
test('a fresh plan with an ERROR is refused even when its digest matches', () => {
	const digest = 'sha256:' + 'a'.repeat(64);
	const plan = (issues: { severity: ValidationSeverity }[], planDigest = digest) =>
		create(PlanAutomationResponseSchema, {
			planDigest,
			issues: issues.map((issue) => create(ValidationIssueSchema, { ...issue, message: 'x' }))
		});
	expect(freshPlanRefused(digest, plan([]))).toBe(false);
	expect(freshPlanRefused(digest, plan([{ severity: ValidationSeverity.WARNING }]))).toBe(false);
	expect(freshPlanRefused(digest, plan([{ severity: ValidationSeverity.ERROR }]))).toBe(true);
	expect(freshPlanRefused(digest, plan([], 'sha256:' + 'b'.repeat(64)))).toBe(true);
	expect(freshPlanRefused(digest, plan([], ''))).toBe(true);
	expect(planHasErrors(plan([{ severity: ValidationSeverity.ERROR }]))).toBe(true);
	expect(planHasErrors(plan([{ severity: ValidationSeverity.WARNING }]))).toBe(false);
});
