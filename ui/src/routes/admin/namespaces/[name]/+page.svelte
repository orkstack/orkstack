<script lang="ts">
	// One namespace's own page: what the list leaves out, and its edit form.
	//
	// Writes are honest, as on the list: an update sends the etag it read, and a
	// conflict is a FailedPrecondition shown as one.
	import { goto, invalidateAll } from '$app/navigation';
	import { page } from '$app/state';
	import { untrack } from 'svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import SignerField from '#lib/components/SignerField.svelte';
	import TelemetryFields from '#lib/components/TelemetryFields.svelte';
	import { isStaleEtagConflict } from '#lib/adminErrors.ts';
	import { errorMessage } from '#lib/client.ts';
	import { namespaceClient } from '#lib/clients/namespaces.ts';
	import { guardUnsaved, leavesRoute } from '#lib/dirtyGuard.ts';
	import type { Namespace } from '#lib/gen/orkstack/v1/namespace_pb.ts';
	import { namespaceLabel } from '#lib/namespace.ts';
	import {
		allowedSecretsLabel,
		budgetCommand,
		budgetPeriodHours,
		budgetRequest,
		buildTelemetryPolicy,
		holdRequest,
		maxRunningRequest,
		taskLogsSelectValue,
		type TaskLogsChoice
	} from '#lib/namespaceForm.ts';
	import { namespaceReturnHref } from '#lib/namespaceList.ts';
	import { parsePoolList, parseSecretPatternLines, validateNamespaceForm, type NamespaceFormErrors } from '#lib/namespaceRules.ts';
	import { hoursToMs } from '#lib/retentionForm.ts';
	import { fmtTimestamp } from '#lib/status.ts';
	import { budgetConfigLabel } from '#lib/usage.ts';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// Back to the page of the list the reader came from; reached any other way,
	// to the list filtered to this namespace, so its row is on screen whichever
	// page it would sort onto (a create lands the same way).
	async function leave() {
		closeEdit();
		await goto(namespaceReturnHref(page.url, data.name, namespaceLabel(data.namespace)), { invalidateAll: true, reset: false });
	}

	// ── edit (etag round trip) ────────────────────────────────────────────
	let editingName = $state('');
	let editDescription = $state('');
	let editDefaultPool = $state('');
	let editAllowedPools = $state('');
	let editRunRetentionHours = $state('');
	let editTelemetrySink = $state('');
	let editTaskLogs = $state<TaskLogsChoice>('inherit');
	let editTelemetryAttributesText = $state('');
	let editHoldReason = $state('');
	let editMaxRunning = $state('');
	// Not edited here (orkctl namespace update --fair-share-weight sets it),
	// but carried through: UpdateNamespace replaces the document, so leaving
	// it out would silently reset the namespace to the default weight.
	let editFairShareWeight = $state(0);
	let editAllowedSecrets = $state('');
	let editBudgetCpuSeconds = $state('');
	let editBudgetGibSeconds = $state('');
	let editBudgetPeriodHours = $state('');
	let editBudgetEnforce = $state(false);
	let editRequireSignedBy = $state('');
	let editEtag = $state('');
	let editErrors = $state<NamespaceFormErrors>({});
	let editBusy = $state(false);
	let editServerError = $state('');
	// A conflict names both what was expected to still be there and that the
	// document has since moved — the wording UpdateNamespace's own refusal
	// uses (internal/api/namespaces.go).
	let editConflict = $state(false);

	function openEdit(ns: Namespace) {
		editingName = ns.name;
		editDescription = ns.description;
		editDefaultPool = ns.defaultPool;
		editAllowedPools = ns.allowedPools.join(', ');
		editRunRetentionHours = ns.runRetention ? String(Number(ns.runRetention.seconds) / 3600) : '';
		editTelemetrySink = ns.telemetry?.sink ?? '';
		editTaskLogs = taskLogsSelectValue(ns.telemetry?.taskLogs);
		editTelemetryAttributesText = ns.telemetry
			? Object.entries(ns.telemetry.attributes)
					.map(([key, value]) => `${key}=${value}`)
					.join('\n')
			: '';
		editHoldReason = ns.hold?.reason ?? '';
		editMaxRunning = ns.maxRunning > 0 ? String(ns.maxRunning) : '';
		editFairShareWeight = ns.fairShareWeight;
		editAllowedSecrets = ns.allowedSecrets.join('\n');
		editBudgetCpuSeconds = ns.budget && ns.budget.cpuSeconds > 0n ? String(ns.budget.cpuSeconds) : '';
		editBudgetGibSeconds = ns.budget && ns.budget.gibSeconds > 0n ? String(ns.budget.gibSeconds) : '';
		editBudgetPeriodHours = budgetPeriodHours(ns.budget);
		editBudgetEnforce = ns.budget?.enforce ?? false;
		editRequireSignedBy = ns.requireSignedBy;
		editEtag = ns.etag;
		editLabels = { ...ns.labels };
		editErrors = {};
		editServerError = '';
		editConflict = false;
		editOpened = editFieldValues();
	}

	// The edit form's fields by name, and the values it was opened with: what
	// the dirty guard compares, and what a conflict retry diffs to tell the
	// reader's edit from what the form merely loaded.
	type EditFields = {
		description: string;
		defaultPool: string;
		allowedPools: string;
		runRetentionHours: string;
		telemetrySink: string;
		taskLogs: TaskLogsChoice;
		telemetryAttributes: string;
		holdReason: string;
		maxRunning: string;
		allowedSecrets: string;
		budgetCpuSeconds: string;
		budgetGibSeconds: string;
		budgetPeriodHours: string;
		budgetEnforce: boolean;
		requireSignedBy: string;
	};
	function editFieldValues(): EditFields {
		return {
			description: editDescription,
			defaultPool: editDefaultPool,
			allowedPools: editAllowedPools,
			runRetentionHours: editRunRetentionHours,
			telemetrySink: editTelemetrySink,
			taskLogs: editTaskLogs,
			telemetryAttributes: editTelemetryAttributesText,
			holdReason: editHoldReason,
			maxRunning: editMaxRunning,
			allowedSecrets: editAllowedSecrets,
			budgetCpuSeconds: editBudgetCpuSeconds,
			budgetGibSeconds: editBudgetGibSeconds,
			budgetPeriodHours: editBudgetPeriodHours,
			budgetEnforce: editBudgetEnforce,
			requireSignedBy: editRequireSignedBy
		};
	}
	function setEditFields(fields: EditFields) {
		editDescription = fields.description;
		editDefaultPool = fields.defaultPool;
		editAllowedPools = fields.allowedPools;
		editRunRetentionHours = fields.runRetentionHours;
		editTelemetrySink = fields.telemetrySink;
		editTaskLogs = fields.taskLogs;
		editTelemetryAttributesText = fields.telemetryAttributes;
		editHoldReason = fields.holdReason;
		editMaxRunning = fields.maxRunning;
		editAllowedSecrets = fields.allowedSecrets;
		editBudgetCpuSeconds = fields.budgetCpuSeconds;
		editBudgetGibSeconds = fields.budgetGibSeconds;
		editBudgetPeriodHours = fields.budgetPeriodHours;
		editBudgetEnforce = fields.budgetEnforce;
		editRequireSignedBy = fields.requireSignedBy;
	}
	let editOpened: EditFields | undefined;
	// changedEditFields is the reader's edit: the fields whose value differs
	// from the one the form was opened with.
	function changedEditFields(): (keyof EditFields)[] {
		const now = editFieldValues();
		const opened = editOpened;
		if (!opened) return [];
		return (Object.keys(now) as (keyof EditFields)[]).filter((key) => now[key] !== opened[key]);
	}
	function takeEditField<K extends keyof EditFields>(to: EditFields, from: EditFields, key: K) {
		to[key] = from[key];
	}

	function closeEdit() {
		editingName = '';
		editConflict = false;
		editServerError = '';
	}
	// Typing here survives a navigation the reader did not confirm (F-ui-ux-3);
	// a save or a Cancel closes the edit first, which is what makes it clean.
	guardUnsaved(
		(navigation) => (leavesRoute(navigation) || navigation.to?.params?.name !== data.name) && editingName !== '' && changedEditFields().length > 0,
		'Discard the namespace changes you have not saved?'
	);

	// editLabels carries a namespace's labels UNTOUCHED through an edit. This
	// form has no control for them, but UpdateNamespace is a full replace of
	// the mutable
	// document (internal/api/namespaces.go), so a submit that omitted them
	// would silently erase whatever labels were there. They are read here and
	// sent back exactly as read; only the fields this form DOES expose can
	// change.
	let editLabels: Record<string, string> = {};

	async function submitEdit() {
		editServerError = '';
		editConflict = false;
		const allowed = parsePoolList(editAllowedPools);
		editErrors = validateNamespaceForm(
			{
				name: editingName,
				defaultPool: editDefaultPool.trim(),
				allowedPools: allowed,
				runRetentionHours: editRunRetentionHours,
				telemetryAttributesText: editTelemetryAttributesText,
				holdReason: editHoldReason,
				maxRunning: editMaxRunning,
				allowedSecretsText: editAllowedSecrets,
				budgetCpuSeconds: editBudgetCpuSeconds,
				budgetGibSeconds: editBudgetGibSeconds,
				budgetPeriodHours: editBudgetPeriodHours,
				budgetEnforce: editBudgetEnforce,
				requireSignedBy: editRequireSignedBy.trim()
			},
			{ requireName: false }
		);
		if (Object.keys(editErrors).length > 0) return;

		editBusy = true;
		try {
			await namespaceClient(fetch).updateNamespace({
				namespace: {
					name: editingName,
					description: editDescription.trim(),
					defaultPool: editDefaultPool.trim(),
					allowedPools: allowed,
					labels: editLabels,
					etag: editEtag,
					// Unlike labels (which this form never edits directly and so
					// carries through untouched), this field IS user-editable —
					// openEdit prefilled it from what was stored — so a blank
					// here is a deliberate choice to inherit the deployment
					// maximum, not an omission to preserve.
					runRetention:
						editRunRetentionHours.trim() === ''
							? undefined
							: durationFromMs(hoursToMs(Number(editRunRetentionHours.trim()))),
					// Also user-editable, prefilled the same way: a submit with
					// every telemetry field back at "no override" sends no
					// policy at all, same as buildTelemetryPolicy documents for
					// create.
					telemetry: buildTelemetryPolicy(editTelemetrySink, editTaskLogs, editTelemetryAttributesText),
					// Prefilled from the row like run retention: a blank hold
					// reason on save is a deliberate release, not an omission.
					hold: holdRequest(editHoldReason),
					maxRunning: maxRunningRequest(editMaxRunning),
					fairShareWeight: editFairShareWeight,
					// Prefilled one entry per line from the row: blank on save
					// sends an empty list, a deliberate choice of unrestricted,
					// not an omission — the rule run retention follows above.
					allowedSecrets: parseSecretPatternLines(editAllowedSecrets),
					// Prefilled from the row: both amounts blank on save sends no
					// budget, which clears it — a deliberate choice, not an
					// omission, the rule every prefilled field here follows.
					budget: budgetRequest(editBudgetCpuSeconds, editBudgetGibSeconds, editBudgetPeriodHours, editBudgetEnforce),
					// Prefilled from the row like everything above: None on save is
					// a deliberate clear. UpdateNamespace replaces the document, so
					// omitting it would silently drop the signer.
					requireSignedBy: editRequireSignedBy.trim()
				}
			});
			await leave();
		} catch (err) {
			// UpdateNamespace's compare-and-swap is FailedPrecondition for TWO
			// unrelated reasons — a stale etag, and this engine having no
			// namespace registry at all (internal/api/namespaces.go's own
			// `registry()`) — and only the first is fixed by "reload and
			// re-apply". isStaleEtagConflict tells them apart by the server's
			// own words (see #lib/adminErrors.ts): a FailedPrecondition that
			// is NOT a stale etag is shown as an ordinary write error, with no
			// conflict-recovery button that would just repeat the failure.
			if (isStaleEtagConflict(err)) {
				editConflict = true;
				editServerError = errorMessage(err);
			} else {
				editServerError = errorMessage(err);
			}
		} finally {
			editBusy = false;
		}
	}

	// reloadEtagAndRetry re-reads the namespace for its CURRENT etag and
	// re-submits this same edit against it — "the loser re-reads and
	// re-applies" (UpdateNamespace's own doc comment). The edit being
	// re-applied is the fields the reader CHANGED since the form opened;
	// every other field — shown or not — is taken from this fresh read, as
	// the etag is. The conflict proves somebody wrote underneath this form,
	// so a field the reader never touched still holds the value the form
	// loaded, not one the reader chose: re-sending it would silently revert
	// that write. Earlier versions did exactly that, first with the hidden
	// labels and fair-share weight, and then with every shown field — a hold
	// placed from another tab was released by a retry whose hold reason the
	// form had loaded blank. A field both sides changed keeps the reader's
	// value: that is the edit being re-applied.
	async function reloadEtagAndRetry() {
		editBusy = true;
		editServerError = '';
		try {
			const name = editingName;
			const res = await namespaceClient(fetch).getNamespace({ name });
			if (!res.namespace) throw new Error(`namespace ${name} was not returned`);
			// The reader may have opened another namespace's form while the
			// read was in flight: that form is theirs now, so this retry ends.
			if (editingName !== name) return;
			// The reader's edit is taken after the read, so nothing typed
			// while it was in flight is lost. openEdit then takes everything
			// from the fresh read, the hidden fields and the etag included,
			// and makes it the form's new opened state; the reader's changes
			// go back on top.
			const mine = editFieldValues();
			const changed = changedEditFields();
			openEdit(res.namespace);
			const merged = editFieldValues();
			for (const key of changed) takeEditField(merged, mine, key);
			setEditFields(merged);
			await submitEdit();
		} catch (err) {
			// The reread itself failed — a NotFound because the namespace was
			// deleted out from under this conflict, or any other error. Either
			// way `editConflict` must NOT survive: the "reload and re-apply"
			// panel promises a namespace to reload, and pointing that button
			// at one that may no longer exist is worse than the ordinary
			// error path below (`{:else if editServerError}`).
			editConflict = false;
			editServerError = errorMessage(err);
		} finally {
			editBusy = false;
		}
	}

	// The form opens on what this page read, and on a namespace reached from
	// this one's own page without a reload.
	$effect(() => {
		const record = data.record;
		if (record && record.name !== untrack(() => editingName)) untrack(() => openEdit(record));
	});
</script>

<svelte:head><title>Namespace {data.name} — OrkStack</title></svelte:head>

<SheetHeader
	title={data.name}
	testid="namespace-detail-heading"
	commands={[
		{ label: 'This namespace', command: `orkctl namespace get ${data.name}` },
		{ label: 'Set this budget', command: budgetCommand(data.name, editBudgetCpuSeconds, editBudgetGibSeconds, editBudgetPeriodHours, editBudgetEnforce) }
	]}
/>

{#if data.error}
	<ErrorPanel testid="namespace-detail-error" retryTestid="namespace-detail-retry" subject="namespace {data.name}" message={data.error} busy={retrying} onretry={retry} />
{:else if data.record}
	{@const record = data.record}
	<dl class="fstrip mb-5" data-testid="namespace-facts">
		<div><dt>Signed by</dt><dd class="font-mono text-sm" data-testid="namespace-require-signed-by-{data.name}">{record.requireSignedBy || '—'}</dd></div>
		<div><dt>Budget</dt><dd class="figure" data-testid="namespace-budget-{data.name}">{budgetConfigLabel(record.budget)}</dd></div>
		<div><dt>Allowed secrets</dt><dd class="font-mono text-sm" data-testid="namespace-allowed-secrets-{data.name}">{allowedSecretsLabel(record.allowedSecrets)}</dd></div>
		<div><dt>Updated</dt><dd data-testid="namespace-updated-{data.name}">{fmtTimestamp(record.updatedAt)}</dd></div>
	</dl>

	<section class="card p-4" aria-labelledby="namespace-edit-heading">
		<h2 id="namespace-edit-heading" class="mb-3 text-md font-semibold text-ink">Edit</h2>
		<form
			data-testid="namespace-edit-form-{data.name}"
			class="grid max-w-4xl gap-4 sm:grid-cols-2"
			onsubmit={(e) => {
				e.preventDefault();
				void submitEdit();
			}}
		>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Description</span>
				<input
					data-testid="ns-edit-description-{data.name}"
					bind:value={editDescription}
					class="field h-9 px-3"
				/>
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Default pool</span>
				<input
					data-testid="ns-edit-default-pool-{data.name}"
					bind:value={editDefaultPool}
					class="field h-9 px-3"
				/>
				{#if editErrors.defaultPool}
					<span class="severity-error text-xs">{editErrors.defaultPool}</span>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="field-label mb-0">Allowed pools <span class="field-hint">(comma-separated)</span></span>
				<input
					data-testid="ns-edit-allowed-pools-{data.name}"
					bind:value={editAllowedPools}
					class="field h-9 px-3"
				/>
				{#if editErrors.allowedPools}
					<span class="severity-error text-xs">{editErrors.allowedPools}</span>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Run retention <span class="field-hint">(hours, blank = inherit)</span></span>
				<input
					data-testid="ns-edit-run-retention-{data.name}"
					bind:value={editRunRetentionHours}
					inputmode="decimal"
					class="field h-9 px-3"
				/>
				{#if editErrors.runRetention}
					<span class="severity-error text-xs">{editErrors.runRetention}</span>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Hold reason <span class="field-hint">(blank = accepting)</span></span>
				<input
					data-testid="ns-edit-hold-reason-{data.name}"
					bind:value={editHoldReason}
					class="field h-9 px-3"
				/>
				<span class="text-xs text-ink-muted">A reason closes the namespace to new runs; blank releases it.</span>
				{#if editErrors.holdReason}
					<span class="severity-error text-xs">{editErrors.holdReason}</span>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Max running <span class="field-hint">(0 = unlimited)</span></span>
				<input
					data-testid="ns-edit-max-running-{data.name}"
					bind:value={editMaxRunning}
					inputmode="numeric"
					class="field h-9 px-3"
				/>
				{#if editErrors.maxRunning}
					<span class="severity-error text-xs">{editErrors.maxRunning}</span>
				{/if}
			</label>
			<fieldset class="flex flex-col gap-2 border-y border-rule py-3 text-sm sm:col-span-2" data-testid="ns-edit-budget-{data.name}">
				<legend class="field-label mb-0 float-left w-full">Budget <span class="field-hint">(reserved cpu and memory over a rolling period; both amounts blank = unbounded)</span></legend>
				<div class="grid gap-3 sm:grid-cols-4">
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget cpu-seconds <span class="field-hint">(blank = unbounded)</span></span>
						<input data-testid="ns-edit-budget-cpu-seconds-{data.name}" bind:value={editBudgetCpuSeconds} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget GiB-seconds <span class="field-hint">(blank = unbounded)</span></span>
						<input data-testid="ns-edit-budget-gib-seconds-{data.name}" bind:value={editBudgetGibSeconds} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget period <span class="field-hint">(whole hours, e.g. 720)</span></span>
						<input data-testid="ns-edit-budget-period-hours-{data.name}" bind:value={editBudgetPeriodHours} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex items-center gap-2 self-end pb-2">
						<input type="checkbox" class="size-4" data-testid="ns-edit-budget-enforce-{data.name}" bind:checked={editBudgetEnforce} />
						<span class="field-label mb-0">Enforce</span>
					</label>
				</div>
				<span class="text-xs text-ink-muted">Units are RESERVED cpu and memory × attempt time, not measured utilization. Unchecked, the budget is reported and never refuses; checked, new runs are refused once it is reached — check the dashboard’s Usage section answers first, because an enforced budget that cannot be measured refuses every submit.</span>
				{#if editErrors.budget}
					<span class="severity-error text-xs" data-testid="ns-edit-error-budget-{data.name}">{editErrors.budget}</span>
				{/if}
			</fieldset>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="field-label mb-0">Allowed secrets <span class="field-hint">(one scheme:pattern per line, e.g. env:SANDBOX_*; blank = unrestricted)</span></span>
				<textarea
					data-testid="ns-edit-allowed-secrets-{data.name}"
					bind:value={editAllowedSecrets}
					rows="2"
					spellcheck="false"
					class="field px-3 py-2 font-mono text-xs"
				></textarea>
				<span class="text-xs text-ink-muted"
					>Bounds which <code>source:</code> values this namespace's tasks may name. Each
					Orker's own <code>ORK_ORKER_SECRET_ALLOW</code> still applies.</span
				>
				{#if editErrors.allowedSecrets}
					<span class="severity-error text-xs" data-testid="ns-edit-error-allowed-secrets-{data.name}"
						>{editErrors.allowedSecrets}</span
					>
				{/if}
			</label>
			<SignerField
				bind:value={editRequireSignedBy}
				keys={data.signingKeys}
				keysError={data.signingKeysError}
				testid="ns-edit-require-signed-by-{data.name}"
				error={editErrors.requireSignedBy}
				errorTestid="ns-edit-error-require-signed-by-{data.name}"
			/>
			<TelemetryFields
				bind:sink={editTelemetrySink}
				bind:taskLogs={editTaskLogs}
				bind:attributesText={editTelemetryAttributesText}
				sinks={data.telemetrySinks}
				sinksError={data.telemetrySinksError}
				idPrefix="ns-edit"
				testIdSuffix="-{data.name}"
				attributesError={editErrors.telemetryAttributes}
			/>
			<div class="flex items-center gap-2 sm:col-span-2">
				<button
					type="submit"
					data-testid="ns-edit-save-{data.name}"
					disabled={editBusy}
					class="btn btn-sm btn-primary"
				>
					{editBusy ? 'Saving…' : 'Save'}
				</button>
				<button
					type="button"
					data-testid="ns-edit-cancel-{data.name}"
					disabled={editBusy}
					onclick={() => void leave()}
					class="btn btn-sm"
				>
					Cancel
				</button>
			</div>
		</form>
		{#if editConflict}
			<div
				class="mt-3 max-w-4xl border border-red px-3 py-2 text-sm text-ink-secondary"
				role="alert"
				data-testid="namespace-edit-conflict-{data.name}"
			>
				<p class="severity-warning">{editServerError}</p>
				<p class="mt-1 text-xs text-ink-secondary">
					Namespace {data.name} changed since it was read — reload and re-apply.
				</p>
				<button
					type="button"
					data-testid="namespace-edit-conflict-retry-{data.name}"
					onclick={reloadEtagAndRetry}
					disabled={editBusy}
					class="btn btn-sm mt-2"
				>
					Reload latest and re-apply my edit
				</button>
			</div>
		{:else if editServerError}
			<p
				class="mt-3 max-w-4xl border border-red bg-red-tint px-3 py-2 text-sm text-red-strong"
				role="alert"
				data-testid="namespace-edit-error-{data.name}"
			>
				{editServerError}
			</p>
		{/if}
	</section>
{/if}
