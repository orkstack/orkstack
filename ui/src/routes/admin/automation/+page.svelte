<script lang="ts">
	// Admin → Automation: export, plan and apply one namespace's automation
	// document — its settings, schedules, triggers and webhooks, declared by
	// name (docs/AUTOMATION.md).
	//
	// The engine PLANS and writes nothing. This page, like `orkctl apply`,
	// EXECUTES: Apply sends each actionable step's carried request to the
	// existing RPC it names (src/lib/automation.ts executeStep), so every write
	// is the same validated, audited call the rest of Admin makes. Nothing is
	// optimistic: a row's outcome is what the RPC answered.
	import { page } from '$app/state';
	import { invalidateAll } from '$app/navigation';
	import { Code, ConnectError } from '@connectrpc/connect';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import { errorMessage } from '$lib/client';
	import { automationClient } from '$lib/clients/automation';
	import { namespaceClient } from '$lib/clients/namespaces';
	import { scheduleClient } from '$lib/clients/schedules';
	import { triggerClient } from '$lib/clients/triggers';
	import { webhookClient } from '$lib/clients/webhooks';
	import {
		automationActionSymbol,
		automationActionWord,
		automationApplyCli,
		automationExportCli,
		automationKindSlug,
		automationPlanCli,
		automationSectionKey,
		executeStep,
		freshPlanRefused,
		isActionable,
		planCounts,
		planHasErrors,
		type AutomationWriters
	} from '$lib/automation';
	import {
		AutomationAction,
		AutomationObjectKind,
		type AutomationStep,
		type ExportAutomationResponse,
		type PlanAutomationResponse
	} from '$lib/gen/orkstack/v1/automation_pb';
	import { ValidationSeverity } from '$lib/gen/orkstack/v1/orkflow_service_pb';
	import { namespaceLabel } from '$lib/namespace';
	import { automationPlaceholder } from '$lib/placeholders';
	import { shellQuote } from '$lib/runDetail';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const namespace = $derived(data.namespace);
	const invokeOrigin = $derived(page.url.origin);

	// Export.
	let exported = $state<ExportAutomationResponse | null>(null);
	let exporting = $state(false);
	let exportError = $state('');

	// The document and the plan made from it.
	let yaml = $state('');
	let prune = $state(false);
	let plan = $state<PlanAutomationResponse | null>(null);
	let plannedWith = $state<{ yaml: string; prune: boolean; namespace: string } | null>(null);
	let planning = $state(false);
	let planError = $state('');
	let stale = $state(false);

	// secret_env entries: the signing secret the CLI would read from $VAR.
	let secrets = $state<Record<string, string>>({});

	// Apply.
	let confirming = $state(false);
	let applying = $state(false);
	let applyError = $state('');
	type Outcome = { label: 'applied' | 'already gone' | 'failed' | 'not run'; message: string };
	let outcomes = $state<Record<number, Outcome>>({});
	let applied = $state(false);
	type Disclosed = { kind: AutomationObjectKind; name: string; id: string; secret: string };
	let disclosed = $state<Disclosed[]>([]);

	const steps = $derived(plan?.steps ?? []);
	const actionable = $derived(steps.filter((step) => isActionable(step.action)));
	const hasErrors = $derived(plan !== null && planHasErrors(plan));
	const changed = $derived(
		plannedWith !== null &&
			(plannedWith.yaml !== yaml || plannedWith.prune !== prune || plannedWith.namespace !== namespace)
	);
	const secretSteps = $derived(
		actionable.filter((step) => step.action === AutomationAction.CREATE && step.secretEnv)
	);
	const missingSecret = $derived(secretSteps.some((step) => !(secrets[step.name] ?? '').trim()));
	const deletions = $derived(actionable.filter((step) => step.action === AutomationAction.DELETE));
	const unmanagedSections = $derived(
		plan && plan.planDigest
			? [
					AutomationObjectKind.NAMESPACE,
					AutomationObjectKind.SCHEDULE,
					AutomationObjectKind.TRIGGER,
					AutomationObjectKind.WEBHOOK
				].filter((kind) => !plan!.managed.includes(kind))
			: []
	);
	const canApply = $derived(
		plan !== null &&
			!hasErrors &&
			!changed &&
			!missingSecret &&
			!applying &&
			!applied &&
			actionable.length > 0 &&
			plan.planDigest !== ''
	);
	const exportDownload = $derived(
		exported ? `data:application/yaml;charset=utf-8,${encodeURIComponent(exported.yaml)}` : ''
	);

	async function runExport() {
		exporting = true;
		exportError = '';
		try {
			exported = await automationClient(fetch).exportAutomation({ namespace });
		} catch (err) {
			exported = null;
			exportError = errorMessage(err);
		} finally {
			exporting = false;
		}
	}

	function editExport() {
		if (exported) yaml = exported.yaml;
	}

	async function importFile(event: Event) {
		const input = event.currentTarget as HTMLInputElement;
		const file = input.files?.[0];
		if (file) yaml = await file.text();
		input.value = '';
	}

	// requestPlan asks the engine for a plan of the current inputs. The UI
	// always has an explicit namespace — the switcher's — so it always sends
	// one, and a document for another namespace answers with the issue that
	// says so.
	async function requestPlan(): Promise<PlanAutomationResponse> {
		return automationClient(fetch).planAutomation({
			yaml,
			namespace: namespaceLabel(namespace),
			prune
		});
	}

	async function runPlan() {
		planning = true;
		planError = '';
		stale = false;
		applied = false;
		outcomes = {};
		disclosed = [];
		applyError = '';
		try {
			const inputs = { yaml, prune, namespace };
			plan = await requestPlan();
			plannedWith = inputs;
		} catch (err) {
			plan = null;
			plannedWith = null;
			planError = errorMessage(err);
		} finally {
			planning = false;
		}
	}

	// apply re-plans with the same inputs and refuses a plan that moved since
	// it was shown, or that now has an ERROR — exactly orkctl's --expect-plan — then runs each
	// actionable step in order, stopping at the first failure.
	async function apply() {
		if (!plan) return;
		applying = true;
		applyError = '';
		stale = false;
		const shown = plan.planDigest;
		let fresh: PlanAutomationResponse;
		try {
			fresh = await requestPlan();
		} catch (err) {
			applyError = errorMessage(err);
			applying = false;
			return;
		}
		// A fresh plan with an ERROR is stale too: the digest covers only the
		// actionable steps, so an error that appeared since leaves it unchanged.
		if (freshPlanRefused(shown, fresh)) {
			plan = fresh;
			stale = true;
			confirming = false;
			applying = false;
			return;
		}
		confirming = false;
		const client: AutomationWriters = {
			namespaces: namespaceClient(fetch),
			schedules: scheduleClient(fetch),
			triggers: triggerClient(fetch),
			webhooks: webhookClient(fetch)
		};
		const results: Record<number, Outcome> = {};
		const secretsOut: Disclosed[] = [];
		let failed = false;
		for (const [index, step] of fresh.steps.entries()) {
			if (!isActionable(step.action)) continue;
			if (failed) {
				results[index] = { label: 'not run', message: '' };
				continue;
			}
			try {
				const result = await executeStep(client, step, secrets);
				results[index] = { label: 'applied', message: result.id };
				if (result.secret) {
					secretsOut.push({ kind: step.kind, name: step.name, id: result.id, secret: result.secret });
				}
			} catch (err) {
				if (
					step.action === AutomationAction.DELETE &&
					err instanceof ConnectError &&
					err.code === Code.NotFound
				) {
					results[index] = { label: 'already gone', message: '' };
					continue;
				}
				results[index] = { label: 'failed', message: errorMessage(err) };
				failed = true;
			}
			outcomes = { ...results };
		}
		outcomes = results;
		disclosed = secretsOut;
		applied = true;
		applying = false;
		await invalidateAll();
	}

	function changeLine(step: AutomationStep, change: { field: string; before: string; after: string }) {
		return step.action === AutomationAction.CREATE
			? `${change.field}: ${change.after}`
			: `${change.field}: ${change.before} → ${change.after}`;
	}

	const severityLabel = (severity: ValidationSeverity) =>
		severity === ValidationSeverity.WARNING ? 'WARNING' : 'ERROR';

	function curl(id: string, token: string): string {
		return [
			`curl -X POST ${shellQuote(`${invokeOrigin}/v1/triggers/${id}`)} \\`,
			`  -H ${shellQuote(`Authorization: Bearer ${token}`)} \\`,
			`  -H 'Content-Type: application/json' \\`,
			`  -d '{}'`
		].join('\n');
	}

	const inputClass =
		'field h-9 w-full px-3 text-sm';
	const primary = 'btn btn-primary';
	const secondary = 'btn';
</script>

<svelte:head><title>Admin — Automation — OrkStack</title></svelte:head>

<SheetHeader title="Automation">
	{#snippet meta()}Namespace <span class="font-mono font-semibold text-ink">{namespaceLabel(namespace)}</span>{/snippet}
</SheetHeader>

<ScopeNote testid="automation-scope">
	An automation document declares one namespace's settings, schedules, triggers and webhooks by
	name. OrkStack does not store the document — keep it in Git — and nothing here runs on its own:
	a plan writes nothing, and Apply sends the plan's steps to the same services the rest of Admin
	uses. This page works in namespace
	<span class="font-medium text-ink">{namespaceLabel(namespace)}</span>.
</ScopeNote>

<section class="region mb-10 pt-3">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<h2 class="text-md font-semibold text-ink">Export</h2>
		<CliEquivalent command={automationExportCli(namespaceLabel(namespace))} testid="automation-export-cli" />
	</div>
	<p class="mt-1 max-w-prose text-sm text-ink-secondary">
		The document {namespaceLabel(namespace)}'s current automation would be applied from — the way to
		start reviewing a namespace managed by hand.
	</p>
	<div class="mt-3 flex flex-wrap gap-2">
		<button type="button" class={secondary} data-testid="automation-export" disabled={exporting} onclick={runExport}>
			{exporting ? 'Exporting…' : 'Export'}
		</button>
		{#if exported}
			<a
				class={secondary}
				data-testid="automation-export-download"
				href={exportDownload}
				download="automation.ork.yaml">Download</a
			>
			<button type="button" class={secondary} data-testid="automation-export-edit" onclick={editExport}>
				Edit as a plan
			</button>
		{/if}
	</div>
	{#if exportError}
		<div class="mt-3"><ErrorPanel testid="automation-export-error" message={exportError} /></div>
	{/if}
	{#if exported}
		<pre
			data-testid="automation-export-yaml"
			class="mt-3 max-h-96 overflow-auto border border-rule bg-paper-sunken p-3 font-mono text-xs text-ink">{exported.yaml}</pre>
		{#if exported.unmanaged.length > 0}
			<div
				data-testid="automation-unmanaged"
				class="note mt-3 flex gap-3 p-3 text-sm text-ink-secondary"
			>
				<span class="caps shrink-0 pt-0.5 text-ink-muted">Note</span>
				<div class="min-w-0">
				<p class="font-semibold text-ink">
					{exported.unmanaged.length} object{exported.unmanaged.length === 1 ? ' has' : 's have'} no name
					and {exported.unmanaged.length === 1 ? 'is' : 'are'} outside every document.
				</p>
				<p class="mt-1 text-xs">Name one in place — its token or secret is kept — and export again.</p>
				<ul class="mt-2 space-y-1 font-mono text-xs">
					{#each exported.unmanaged as step (step.id)}
						<li>
							{automationKindSlug(step.kind)}
							{step.id} ({step.summary}) — {step.kind === AutomationObjectKind.TRIGGER
								? `orkctl trigger update ${step.id} -n ${namespaceLabel(namespace)} --name NAME`
								: `orkctl webhook update ${step.id} --name NAME`}
						</li>
					{/each}
				</ul>
				</div>
			</div>
		{/if}
	{/if}
</section>

<section class="region mb-10 pt-3">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<h2 class="text-md font-semibold text-ink">Document</h2>
		<CliEquivalent command={automationPlanCli(namespaceLabel(namespace), prune)} testid="automation-plan-cli" />
	</div>
	<textarea
		data-testid="automation-yaml"
		aria-label="Automation document"
		bind:value={yaml}
		rows="16"
		spellcheck="false"
		placeholder={automationPlaceholder(namespaceLabel(namespace))}
		class="field mt-3 w-full p-3 font-mono text-xs"
	></textarea>
	<div class="mt-3 flex flex-wrap items-center gap-4">
		<label class="text-sm font-semibold text-ink-secondary">
			Import a file
			<input
				type="file"
				accept=".yaml,.yml"
				data-testid="automation-import"
				class="ml-2 text-xs"
				onchange={importFile}
			/>
		</label>
		<label class="flex items-center gap-1.5 text-sm text-ink">
			<input type="checkbox" data-testid="automation-prune" bind:checked={prune} />
			Delete named objects this document does not declare
		</label>
		<button
			type="button"
			class={primary}
			data-testid="automation-plan"
			disabled={planning || yaml.trim() === ''}
			onclick={runPlan}
		>
			{planning ? 'Planning…' : 'Plan'}
		</button>
	</div>
</section>

{#if planError}
	<div class="mb-6">
		<ErrorPanel testid="automation-error" message={planError} retryTestid="automation-retry" onretry={runPlan} busy={planning} />
	</div>
{/if}

{#if stale}
	<p
		data-testid="automation-stale"
		class="severity-warning mb-4 border border-red px-3 py-2 text-sm"
	>
		The namespace changed since this plan was made. Review the new plan; nothing was written.
	</p>
{/if}

{#if plan}
	<section class="region mb-10 pt-3" data-testid="automation-plan-section">
		<h2 class="text-md font-semibold text-ink">
			Plan{plan.namespace ? ` — namespace ${plan.namespace}` : ''}
		</h2>
		{#if changed}
			<p class="severity-warning mt-2 text-xs" data-testid="automation-plan-outdated">
				The document or Prune changed since this plan was made. Plan again before applying.
			</p>
		{/if}
		{#if plan.issues.length > 0}
			<ul class="mt-3 space-y-1" data-testid="automation-issues">
				{#each plan.issues as issue, index (index)}
					<li
						class="font-mono text-xs {issue.severity === ValidationSeverity.WARNING
							? 'severity-warning'
							: 'severity-error'}"
					>
						{severityLabel(issue.severity)}
						{issue.field ? `${issue.field}: ` : ''}{issue.message}
					</li>
				{/each}
			</ul>
		{/if}
		{#if steps.length > 0}
			<div class="mt-3 overflow-x-auto border-t-2 border-ink">
				<table class="register min-w-[720px]" data-testid="automation-plan-table">
					<thead>
						<tr>
							<th scope="col">Action</th>
							<th scope="col">Kind</th>
							<th scope="col">Name</th>
							<th scope="col">What</th>
							<th scope="col">Outcome</th>
						</tr>
					</thead>
					<tbody>
						{#each steps as step, index (index)}
							<tr class="align-top" data-testid="automation-step-{index}">
								<td class="font-mono text-xs whitespace-nowrap" data-testid="automation-step-action-{index}">
									{automationActionSymbol(step.action)}
									{automationActionWord(step.action)}
								</td>
								<td class="text-xs">{automationKindSlug(step.kind)}</td>
								<td class="font-mono text-xs">{step.name || '—'}</td>
								<td class="text-xs">
									<p class="font-mono">{step.summary}</p>
									{#if step.id}<p class="font-mono text-ink-muted">{step.id}</p>{/if}
									{#each step.changes as change (change.field)}
										<p class="font-mono text-ink-secondary">{changeLine(step, change)}</p>
									{/each}
									{#each step.notes as note, n (n)}
										<p class="text-ink-muted">note: {note}</p>
									{/each}
								</td>
								<td class="text-xs" data-testid="automation-step-outcome-{index}">
									{#if outcomes[index]}
										<span
											class={outcomes[index].label === 'failed'
												? 'severity-error'
												: outcomes[index].label === 'not run'
													? 'text-ink-muted'
													: 'text-ink'}
										>
											{outcomes[index].label}{outcomes[index].message ? ` ${outcomes[index].message}` : ''}
										</span>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		{#each unmanagedSections as kind (kind)}
			<p class="mt-2 text-xs text-ink-muted" data-testid="automation-unmanaged-section-{automationKindSlug(kind)}">
				({automationSectionKey(kind)}: not managed by this document)
			</p>
		{/each}
		{#if plan.planDigest}
			<p class="mt-3 text-sm font-semibold text-ink" data-testid="automation-plan-summary">Plan: {planCounts(steps)}</p>
			<p class="mt-1 font-mono text-xs text-ink-muted" data-testid="automation-plan-digest">
				Plan digest: {plan.planDigest}
			</p>
			{#if actionable.length === 0}
				<p class="mt-2 text-sm text-ink" data-testid="automation-no-changes">No changes.</p>
			{/if}
		{/if}

		{#if secretSteps.length > 0 && !applied}
			<div class="mt-4 space-y-3">
				{#each secretSteps as step (step.name)}
					<div class="flex flex-col gap-1.5">
						<label class="field-label mb-0" for="automation-secret-env-{step.name}">
							Signing secret for {step.name} (the CLI reads ${step.secretEnv})
						</label>
						<input
							id="automation-secret-env-{step.name}"
							type="password"
							autocomplete="off"
							data-testid="automation-secret-env-{step.name}"
							value={secrets[step.name] ?? ''}
							oninput={(event) => (secrets = { ...secrets, [step.name]: event.currentTarget.value })}
							class={inputClass}
						/>
					</div>
				{/each}
			</div>
		{/if}

		{#if actionable.length > 0}
			<div class="mt-4 flex items-center gap-3">
				<button
					type="button"
					class={primary}
					data-testid="automation-apply"
					disabled={!canApply}
					onclick={() => {
						confirming = true;
						applyError = '';
					}}
				>
					{applying ? 'Applying…' : 'Apply'}
				</button>
				{#if applied}
					<CliEquivalent
						command={automationApplyCli(namespaceLabel(namespace), prune, plan.planDigest)}
						testid="automation-apply-cli"
					/>
				{/if}
			</div>
		{/if}
		{#if applyError}
			<div class="mt-3"><ErrorPanel testid="automation-apply-error" message={applyError} /></div>
		{/if}
	</section>
{/if}

{#if disclosed.length > 0}
	<section
		data-testid="automation-secrets-once"
		class="note mb-10 border-ink p-4 text-sm text-ink-secondary"
	>
		<p class="font-semibold text-ink">Copy these now. They will not be shown again.</p>
		<ul class="mt-3 space-y-3">
			{#each disclosed as secret (secret.id)}
				<li class="border-t border-rule pt-3" data-testid="automation-secret-{secret.name}">
					<div class="flex items-start justify-between gap-3">
						<div class="min-w-0">
							<p class="text-xs">
								{secret.kind === AutomationObjectKind.TRIGGER ? 'Trigger token' : 'Webhook signing secret'} for
								<span class="font-medium">{secret.name}</span>
								<span class="font-mono" data-testid="automation-secret-id-{secret.name}">{secret.id}</span>
							</p>
							<code class="mt-1 block font-mono text-xs break-all" data-testid="automation-secret-value-{secret.name}"
								>{secret.secret}</code
							>
						</div>
						{#if secret.kind === AutomationObjectKind.TRIGGER}
							<CliEquivalent
								command={curl(secret.id, secret.secret)}
								testid="automation-secret-cli-{secret.name}"
								label="Runnable invocation of this trigger, with its token"
							/>
						{/if}
					</div>
				</li>
			{/each}
		</ul>
	</section>
{/if}

{#if confirming && plan}
	<ConfirmDialog
		testid="automation-apply-confirm"
		title="Apply this plan to namespace {plan.namespace}?"
		acceptLabel="Apply"
		acceptTestid="automation-apply-accept"
		cancelTestid="automation-apply-cancel"
		busy={applying}
		error={applyError}
		destructive={deletions.length > 0}
		onaccept={apply}
		oncancel={() => (confirming = false)}
	>
		{#snippet body()}
			<p>{planCounts(steps)}</p>
			{#if deletions.length > 0}
				<p class="mt-2 font-medium text-red-strong">These will be deleted:</p>
				<ul class="mt-1 space-y-0.5 font-mono text-xs" data-testid="automation-apply-deletions">
					{#each deletions as step (step.id)}
						<li>{automationKindSlug(step.kind)} {step.name} {step.id}</li>
					{/each}
				</ul>
			{/if}
			<p class="mt-2 text-ink-muted">
				The plan is made again before anything is written; if the namespace changed since, nothing is
				written and the new plan is shown.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
