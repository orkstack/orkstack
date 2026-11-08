<script lang="ts">
	// The Admin Namespaces view: every namespace this
	// deployment has, with create/edit/delete against the real registry.
	//
	// Writes are honest: an update sends the etag it read, a
	// conflict is a FailedPrecondition and is shown as one, a refused delete
	// names the orkflow count the server refused it with (verbatim — the
	// server already names them, so nothing here re-derives that sentence),
	// and `default`'s delete control is disabled BEFORE any click, with the
	// reason inline.
	import { goto, invalidateAll } from '$app/navigation';
	import { guardUnsaved, leavesRoute } from '#lib/dirtyGuard.ts';
	import { navigating, page } from '$app/state';
	import CliEquivalent from '#lib/components/CliEquivalent.svelte';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import ConfirmDialog from '#lib/components/ConfirmDialog.svelte';
	import Icon from '#lib/components/Icon.svelte';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import ScopeNote from '#lib/components/ScopeNote.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import Pagination from '#lib/components/Pagination.svelte';
	import Select from '#lib/components/Select.svelte';
	import TelemetryFields from '#lib/components/TelemetryFields.svelte';
	import SignerField from '#lib/components/SignerField.svelte';
	import { isStaleEtagConflict } from '#lib/adminErrors.ts';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '#lib/browsing.ts';
	import { createdNamespaceHref, namespacePageHref, namespacesEmptyState } from '#lib/namespaceList.ts';
	import { filterFlags, type FieldFilter } from '#lib/listFilter.ts';
	import { ListNamespacesRequestSchema } from '#lib/gen/orkstack/v1/namespace_service_pb.ts';
	import { errorMessage } from '#lib/client.ts';
	import { namespaceClient } from '#lib/clients/namespaces.ts';
	import { create } from '@bufbuild/protobuf';
	import { NamespaceHoldSchema, type Namespace } from '#lib/gen/orkstack/v1/namespace_pb.ts';
	import { DEFAULT_NAMESPACE, namespaceLabel, rememberNamespace, withNamespace } from '#lib/namespace.ts';
	import {
		attributeCountLabel,
		budgetRequest,
		buildTelemetryPolicy,
		holdLabel,
		holdRequest,
		maxRunningLabel,
		maxRunningRequest,
		runRetentionLabel,
		taskLogsLabel,
		telemetrySinkLabel,
		type TaskLogsChoice
	} from '#lib/namespaceForm.ts';
	import {
		DEFAULT_POOL_NAME,
		parsePoolList,
		parseSecretPatternLines,
		validateNamespaceForm,
		type NamespaceFormErrors
	} from '#lib/namespaceRules.ts';
	import { hoursToMs, parseRetentionHours } from '#lib/retentionForm.ts';
	import { shellQuote } from '#lib/runDetail.ts';
	import { fmtTimestamp } from '#lib/status.ts';
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

	// navigatingHere covers every re-entry into this same URL that isn't the
	// explicit Retry click above (which calls `invalidateAll` directly, never
	// `goto`, so `navigating` never turns true for it) — chiefly the
	// namespace switcher re-navigating `/admin/namespaces?ns=…` while this
	// page stays mounted with its stale list still on screen.
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);

	// ── paging and filters ─────────────────────────────────────────────────
	// The same coordinates, builder and cursor stack the Orkflows list keeps
	// (orkflows/+page.svelte; docs/UI_CLI_PARITY.md "Paging in the browser"),
	// plus the applied filters (ListFilter). `ns` rides along so links out of
	// this page keep the switcher's choice; it does not filter this view.
	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), size: String(data.size), cursor: data.cursor, page: String(data.page) });
	const applied = $derived({ filter: data.applied });
	function href(changes: Record<string, string>, filters: Record<string, FieldFilter[]> = applied) {
		return listHref('/admin/namespaces', { ...coordinates, ...changes }, filters);
	}
	const stackKey = $derived(pageStackKey('/admin/namespaces', coordinates, applied));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, data.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[data.page - 1], ahead[data.page]] = [data.cursor, data.nextPageToken];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	const walked = $derived(data.page > 1 && stack[data.page - 1] === data.cursor);
	const previousHref = $derived(walked ? href({ cursor: stack[data.page - 2] ?? '', page: String(data.page - 1) }) : undefined);
	const nextHref = $derived(data.nextPageToken ? href({ cursor: data.nextPageToken, page: String(data.page + 1) }) : undefined);
	const firstHref = $derived(href({ cursor: '', page: '' }));

	// Page size is a field of the filter form and applies with Apply filters,
	// never on change: one form never applies some fields on change and
	// others on submit. The draft is refilled from what the page loaded.
	function sizeFromUrl() {
		return String(data.size);
	}
	let sizeDraft = $state(sizeFromUrl());
	$effect(() => {
		sizeDraft = sizeFromUrl();
	});
	// A new filter set (or size) is a new page 1, and the path back through
	// the set it leaves is dropped rather than trusted later.
	function applyFilters(filters: FieldFilter[]) {
		writePageStack(stackKey, []);
		void goto(href({ size: sizeDraft, cursor: '', page: '' }, { filter: filters }), { reset: false });
	}
	const clearHref = $derived(href({ cursor: '', page: '' }, {}));
	const emptyState = $derived(
		namespacesEmptyState({ filtered: data.applied.length > 0, page: data.page, count: data.namespacesList.length, hasNext: data.nextPageToken !== '' })
	);


	// ── create ─────────────────────────────────────────────────────────────
	let createName = $state('');
	let createDescription = $state('');
	let createDefaultPool = $state('');
	let createAllowedPools = $state('');
	let createRunRetentionHours = $state('');
	let createTelemetrySink = $state('');
	let createTaskLogs = $state<TaskLogsChoice>('inherit');
	let createTelemetryAttributesText = $state('');
	let createHoldReason = $state('');
	let createMaxRunning = $state('');
	let createAllowedSecrets = $state('');
	let createBudgetCpuSeconds = $state('');
	let createBudgetGibSeconds = $state('');
	let createBudgetPeriodHours = $state('');
	let createBudgetEnforce = $state(false);
	let createRequireSignedBy = $state('');
	let createErrors = $state<NamespaceFormErrors>({});
	let createBusy = $state(false);
	let createServerError = $state('');
	let createSuccess = $state('');
	// The create form is a disclosure the header's primary opens; it stays
	// open after a create, so the next one starts where the last one left.
	let createOpen = $state(false);

	// What the create form holds, as one comparable value: the form is dirty
	// when it differs from what it held when it was last opened or reset.
	function createFields() {
		return JSON.stringify([
			createName, createDescription, createDefaultPool, createAllowedPools, createRunRetentionHours,
			createTelemetrySink, createTaskLogs, createTelemetryAttributesText, createHoldReason, createMaxRunning,
			createAllowedSecrets, createBudgetCpuSeconds, createBudgetGibSeconds, createBudgetPeriodHours,
			createBudgetEnforce, createRequireSignedBy
		]);
	}
	let createSeed = createFields();

	function resetCreateForm() {
		createName = '';
		createDescription = '';
		createDefaultPool = '';
		createAllowedPools = '';
		createRunRetentionHours = '';
		createTelemetrySink = '';
		createTaskLogs = 'inherit';
		createTelemetryAttributesText = '';
		createHoldReason = '';
		createMaxRunning = '';
		createAllowedSecrets = '';
		createBudgetCpuSeconds = '';
		createBudgetGibSeconds = '';
		createBudgetPeriodHours = '';
		createBudgetEnforce = false;
		createRequireSignedBy = '';
		createSeed = createFields();
	}

	async function submitCreate() {
		createServerError = '';
		createSuccess = '';
		const allowed = parsePoolList(createAllowedPools);
		createErrors = validateNamespaceForm(
			{
				name: createName.trim(),
				defaultPool: createDefaultPool.trim(),
				allowedPools: allowed,
				runRetentionHours: createRunRetentionHours,
				telemetryAttributesText: createTelemetryAttributesText,
				holdReason: createHoldReason,
				maxRunning: createMaxRunning,
				allowedSecretsText: createAllowedSecrets,
				budgetCpuSeconds: createBudgetCpuSeconds,
				budgetGibSeconds: createBudgetGibSeconds,
				budgetPeriodHours: createBudgetPeriodHours,
				budgetEnforce: createBudgetEnforce,
				requireSignedBy: createRequireSignedBy.trim()
			},
			{ requireName: true }
		);
		// Local validation blocks the request entirely — no RPC is made for a
		// slug the registry would refuse anyway.
		if (Object.keys(createErrors).length > 0) return;

		createBusy = true;
		try {
			const res = await namespaceClient(fetch).createNamespace({
				namespace: {
					name: createName.trim(),
					description: createDescription.trim(),
					defaultPool: createDefaultPool.trim(),
					allowedPools: allowed,
					// Blank means inherit: omit the field entirely rather than send
					// a zero duration, which validNamespaceRetention would refuse
					// as non-positive.
					runRetention:
						createRunRetentionHours.trim() === ''
							? undefined
							: durationFromMs(hoursToMs(Number(createRunRetentionHours.trim()))),
					// undefined when the sink, task-logs and attribute fields are
					// all left at their "no override" value — inherited end to
					// end, the same distinction buildTelemetryPolicy documents.
					telemetry: buildTelemetryPolicy(
						createTelemetrySink,
						createTaskLogs,
						createTelemetryAttributesText
					),
					hold: holdRequest(createHoldReason),
					maxRunning: maxRunningRequest(createMaxRunning),
					// Blank is an empty list: unrestricted.
					allowedSecrets: parseSecretPatternLines(createAllowedSecrets),
					// Both amounts blank: no budget, unbounded.
					budget: budgetRequest(createBudgetCpuSeconds, createBudgetGibSeconds, createBudgetPeriodHours, createBudgetEnforce),
					// "" is None: no signature required beyond the deployment floor.
					requireSignedBy: createRequireSignedBy.trim()
				}
			});
			const createdName = res.namespace?.name ?? createName.trim();
			createSuccess = res.created
				? `Created ${res.namespace?.name}.`
				: `${res.namespace?.name} already exists with this exact spec — nothing changed.`;
			resetCreateForm();
			// The created row is shown by filtering to it on a first page: with
			// the list paged, the page it sorts onto is not necessarily the one
			// on screen. invalidateAll, because the address may not change (it
			// was already filtered to this name) and the row must still appear.
			await goto(createdNamespaceHref(coordinates, createdName), { invalidateAll: true, reset: false });
		} catch (err) {
			createServerError = errorMessage(err);
		} finally {
			createBusy = false;
		}
	}

	// Typing in the create form survives a navigation the reader did not confirm
	// (F-ui-ux-3). Paging, filtering and the namespace switcher stay on this
	// route, where the form stays mounted and loses nothing, so only leaving the
	// route asks.
	guardUnsaved(
		(navigation) => leavesRoute(navigation) && createOpen && createFields() !== createSeed,
		'Discard the namespace changes you have not saved?'
	);

	// ── hold / release (the one-click incident flow) ───────────────────────
	// Both are the same read-etag-then-update round trip submitEdit makes,
	// changing the hold and nothing else: the document is read fresh, so the
	// write carries whatever else is stored now, and a write that lands in
	// between is the stale-etag conflict, told apart by isStaleEtagConflict.
	let pendingHold = $state('');
	let holdReasonInput = $state('');
	let pendingRelease = $state<Namespace | null>(null);
	let admissionBusy = $state(false);
	let admissionError = $state('');

	function askToHold(name: string) {
		pendingHold = name;
		holdReasonInput = '';
		admissionError = '';
	}

	function askToRelease(ns: Namespace) {
		pendingRelease = ns;
		admissionError = '';
	}

	function closeAdmission() {
		pendingHold = '';
		pendingRelease = null;
		holdReasonInput = '';
		admissionError = '';
	}

	async function writeHold(name: string, hold: { reason: string } | undefined) {
		admissionBusy = true;
		admissionError = '';
		try {
			const res = await namespaceClient(fetch).getNamespace({ name });
			const current = res.namespace;
			if (!current) throw new Error(`namespace ${name} came back empty`);
			await namespaceClient(fetch).updateNamespace({
				namespace: { ...current, hold: hold ? create(NamespaceHoldSchema, hold) : undefined }
			});
			closeAdmission();
			await invalidateAll();
		} catch (err) {
			admissionError = isStaleEtagConflict(err)
				? `${errorMessage(err)} — namespace ${name} changed while this was being written; try again.`
				: errorMessage(err);
		} finally {
			admissionBusy = false;
		}
	}

	const holdReasonError = $derived(
		holdReasonInput.trim() === ''
			? ''
			: (validateNamespaceForm(
					{ name: pendingHold, defaultPool: '', allowedPools: [], holdReason: holdReasonInput },
					{ requireName: false }
				).holdReason ?? '')
	);

	// ── delete ─────────────────────────────────────────────────────────────
	let pendingDelete = $state('');
	let deleteBusy = $state(false);
	let deleteError = $state('');

	function askToDelete(name: string) {
		pendingDelete = name;
		deleteError = '';
	}

	async function confirmDelete() {
		deleteBusy = true;
		deleteError = '';
		const deletedName = pendingDelete;
		try {
			await namespaceClient(fetch).deleteNamespace({ name: deletedName });
			pendingDelete = '';
			// The namespace this page/app is currently SCOPED TO no longer
			// exists. Leaving `?ns=` pointed at it would carry a coordinate the
			// registry no longer answers for onto Pools, onto Orkflows, onto
			// every link this page's own layout builds — the exact confusion
			// this area must not cause. Reset to `default`,
			// which always resolves (AGENTS.md §7), rather than leaving a
			// dangling selection for the next click to discover.
			if (namespaceLabel(data.namespace) === deletedName) {
				rememberNamespace(DEFAULT_NAMESPACE);
				await goto(withNamespace(page.url.pathname + page.url.search, DEFAULT_NAMESPACE), {
					replace: true,
					invalidateAll: true
				});
			} else {
				await invalidateAll();
			}
		} catch (err) {
			// The refusal is the server's own sentence, verbatim: it already
			// names the orkflow count and the orkflows in the way
			// (internal/api/namespaces.go DeleteNamespace) — nothing here
			// re-derives or paraphrases it.
			deleteError = errorMessage(err);
		} finally {
			deleteBusy = false;
		}
	}
</script>

<svelte:head><title>Admin — Namespaces — OrkStack</title></svelte:head>

<SheetHeader title="Namespaces" commands={[{ label: 'The namespaces', command: ['orkctl', 'namespace', 'list', ...filterFlags(data.applied)].join(' ') }]}>
	{#snippet actions()}
		<!-- The page's one primary: it opens the create form, a disclosure under
		     the header; open, it hands the primary to the form's own submit. A
		     registry that could not be read offers no form to open. -->
		{#if !data.namespacesError}<button
			type="button"
			data-testid="ns-create-open"
			aria-expanded={createOpen}
			aria-controls="ns-create-form"
			onclick={() => (createOpen = !createOpen)}
			class="btn {createOpen ? '' : 'btn-primary'}">{createOpen ? 'Close the form' : 'Create namespace'}</button
		>{/if}
	{/snippet}
</SheetHeader>

<ScopeNote testid="namespaces-scope-caption">
	All namespaces this deployment has — the namespace selector above does not filter this view; the filters below do.
</ScopeNote>

{#snippet filterForm()}
	<div class="mb-3"><ListFilter
		request={ListNamespacesRequestSchema}
		applied={data.applied}
		testid="namespaces"
		onapply={applyFilters}
		{clearHref}
	>
		<div class="flex items-center gap-2 text-sm">
			<label class="field-label mb-0" for="namespaces-page-size">Page size</label>
			<div class="w-20"><Select id="namespaces-page-size" testid="namespaces-page-size" value={sizeDraft} options={sizeOptions} onchange={(next) => (sizeDraft = next)} /></div>
		</div>
	</ListFilter></div>
{/snippet}

{#if data.namespacesError}
	{@render filterForm()}
	<ErrorPanel
		testid="namespaces-error"
		retryTestid="namespaces-retry"
		subject="the namespace registry"
		message={data.namespacesError}
		busy={retrying}
		onretry={retry}
	/>
{:else}
	{#if createOpen}
	<section id="ns-create-form" class="region mb-10 pt-3" aria-labelledby="ns-create-heading">
		<h2 id="ns-create-heading" class="mb-3 text-md font-semibold text-ink">Create a namespace</h2>
		<form
			class="grid max-w-4xl gap-4 sm:grid-cols-2"
			onsubmit={(e) => {
				e.preventDefault();
				void submitCreate();
			}}
		>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Name <span class="field-hint">(lowercase letters, digits, hyphens; e.g. payments)</span></span>
				<input
					data-testid="ns-create-name"
					bind:value={createName}
					class="field h-9 px-3"
				/>
				{#if createErrors.name}
					<span class="severity-error text-xs" data-testid="ns-create-error-name"
						>{createErrors.name}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Description</span>
				<input
					data-testid="ns-create-description"
					bind:value={createDescription}
					class="field h-9 px-3"
				/>
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Default pool <span class="field-hint">(blank = {DEFAULT_POOL_NAME})</span></span>
				<input
					data-testid="ns-create-default-pool"
					bind:value={createDefaultPool}
					class="field h-9 px-3"
				/>
				{#if createErrors.defaultPool}
					<span class="severity-error text-xs" data-testid="ns-create-error-default-pool"
						>{createErrors.defaultPool}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Allowed pools <span class="field-hint">(comma-separated, e.g. cpu, gpu; blank = unrestricted)</span></span>
				<input
					data-testid="ns-create-allowed-pools"
					bind:value={createAllowedPools}
					class="field h-9 px-3"
				/>
				{#if createErrors.allowedPools}
					<span class="severity-error text-xs" data-testid="ns-create-error-allowed-pools"
						>{createErrors.allowedPools}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Run retention <span class="field-hint">(hours, blank = inherit)</span></span>
				<input
					data-testid="ns-create-run-retention"
					bind:value={createRunRetentionHours}
					inputmode="decimal"
					class="field h-9 px-3"
				/>
				<span class="text-xs text-ink-muted">Blank inherits the deployment maximum.</span>
				{#if createErrors.runRetention}
					<span class="severity-error text-xs" data-testid="ns-create-error-run-retention"
						>{createErrors.runRetention}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Hold reason <span class="field-hint">(blank = accepting)</span></span>
				<input
					data-testid="ns-create-hold-reason"
					bind:value={createHoldReason}
					class="field h-9 px-3"
				/>
				{#if createErrors.holdReason}
					<span class="severity-error text-xs" data-testid="ns-create-error-hold-reason"
						>{createErrors.holdReason}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Max running <span class="field-hint">(0 = unlimited)</span></span>
				<input
					data-testid="ns-create-max-running"
					bind:value={createMaxRunning}
					inputmode="numeric"
					class="field h-9 px-3"
				/>
				{#if createErrors.maxRunning}
					<span class="severity-error text-xs" data-testid="ns-create-error-max-running"
						>{createErrors.maxRunning}</span
					>
				{/if}
			</label>
			<fieldset class="flex flex-col gap-2 border-y border-rule py-3 text-sm sm:col-span-2" data-testid="ns-create-budget">
				<legend class="field-label mb-0 float-left w-full">Budget <span class="field-hint">(reserved cpu and memory over a rolling period; both amounts blank = unbounded)</span></legend>
				<div class="grid gap-3 sm:grid-cols-4">
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget cpu-seconds <span class="field-hint">(blank = unbounded)</span></span>
						<input data-testid="ns-create-budget-cpu-seconds" bind:value={createBudgetCpuSeconds} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget GiB-seconds <span class="field-hint">(blank = unbounded)</span></span>
						<input data-testid="ns-create-budget-gib-seconds" bind:value={createBudgetGibSeconds} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="field-label mb-0">Budget period <span class="field-hint">(whole hours, e.g. 720)</span></span>
						<input data-testid="ns-create-budget-period-hours" bind:value={createBudgetPeriodHours} inputmode="numeric" class="field h-9 px-3" />
					</label>
					<label class="flex items-center gap-2 self-end pb-2">
						<input type="checkbox" class="size-4" data-testid="ns-create-budget-enforce" bind:checked={createBudgetEnforce} />
						<span class="field-label mb-0">Enforce</span>
					</label>
				</div>
				<span class="text-xs text-ink-muted">Units are RESERVED cpu and memory × attempt time, not measured utilization. Unchecked, the budget is reported and never refuses; checked, new runs are refused once it is reached — check the dashboard’s Usage section answers first, because an enforced budget that cannot be measured refuses every submit.</span>
				{#if createErrors.budget}
					<span class="severity-error text-xs" data-testid="ns-create-error-budget">{createErrors.budget}</span>
				{/if}
			</fieldset>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="field-label mb-0">Allowed secrets <span class="field-hint">(one scheme:pattern per line, e.g. env:SANDBOX_*; blank = unrestricted)</span></span>
				<textarea
					data-testid="ns-create-allowed-secrets"
					bind:value={createAllowedSecrets}
					rows="2"
					spellcheck="false"
					class="field px-3 py-2 font-mono text-xs"
				></textarea>
				<span class="text-xs text-ink-muted"
					>Bounds which <code>source:</code> values this namespace's tasks may name. Each Orker's own
					<code>ORK_ORKER_SECRET_ALLOW</code> still applies.</span
				>
				{#if createErrors.allowedSecrets}
					<span class="severity-error text-xs" data-testid="ns-create-error-allowed-secrets"
						>{createErrors.allowedSecrets}</span
					>
				{/if}
			</label>
			<SignerField
				bind:value={createRequireSignedBy}
				keys={data.signingKeys}
				keysError={data.signingKeysError}
				testid="ns-create-require-signed-by"
				error={createErrors.requireSignedBy}
				errorTestid="ns-create-error-require-signed-by"
			/>
			<TelemetryFields
				bind:sink={createTelemetrySink}
				bind:taskLogs={createTaskLogs}
				bind:attributesText={createTelemetryAttributesText}
				sinks={data.telemetrySinks}
				sinksError={data.telemetrySinksError}
				idPrefix="ns-create"
				attributesHint="Stamped on every trace, metric and log of this namespace's runs. Blank sets none of its own."
				attributesError={createErrors.telemetryAttributes}
				attributesErrorTestId="ns-create-error-telemetry-attributes"
			/>
			<div class="sm:col-span-2">
				<button
					type="submit"
					data-testid="ns-create-submit"
					disabled={createBusy}
					class="btn btn-primary"
				>
					{createBusy ? 'Creating…' : 'Create namespace'}
				</button>
			</div>
		</form>
		{#if createServerError}
			<p
				class="mt-3 max-w-4xl border border-red bg-red-tint px-3 py-2 text-sm text-red-strong"
				role="alert"
				data-testid="ns-create-server-error"
			>
				{createServerError}
			</p>
		{/if}
		{#if createSuccess}
			<p class="confirm mt-3 text-sm text-ink" role="status" data-testid="ns-create-success">{createSuccess}</p>
		{/if}
	</section>
	{/if}

	<h2 class="mb-2 text-md font-semibold text-ink">Registry</h2>
	{@render filterForm()}

	{#if busy}
		<p class="text-sm text-ink-muted" data-testid="namespaces-loading">Loading namespaces…</p>
	{:else}
		{#if emptyState === 'only-default'}
			<p class="mb-3 text-sm text-ink-muted" data-testid="namespaces-empty">
				No namespaces beyond <code>default</code> yet — it is undeletable
				below because every Orkflow that names no namespace is in it. Create namespace, at the top of the page, adds one.
			</p>
		{:else if emptyState === 'no-match'}
			<p class="mb-3 text-sm text-ink-muted" data-testid="namespaces-empty">
				No namespaces match these filters.
			</p>
		{:else if emptyState === 'past-end'}
			<p class="mb-3 text-sm text-ink-muted" data-testid="namespaces-empty">
				Nothing on this page — the namespaces it held are gone. Start again from the first page.
			</p>
		{/if}
		{#if data.namespacesList.length > 0}
			<div class="relative overflow-x-auto border-t-2 border-ink">
				<table class="register register-stacked register-pairs sm:min-w-[720px]" data-testid="namespace-list">
					<thead class="max-sm:sr-only">
						<tr>
							<th scope="col">Name</th>
							<th scope="col">Hold</th>
							<th scope="col">Max running</th>
							<th scope="col">Run retention</th>
							<th scope="col">Pools</th>
							<th scope="col">Telemetry</th>
							<th scope="col"><span class="sr-only">Actions</span></th>
						</tr>
					</thead>
					<tbody>
						{#each data.namespacesList as ns (ns.name)}
							<tr
								class="transition-colors {ns.hold ? 'bg-red-tint' : 'hover:bg-hover'}"
								data-testid="namespace-row-{ns.name}"
								data-held={ns.hold ? 'true' : 'false'}
							>
								<td class="max-w-44 max-sm:col-span-2">
									<a class="link font-mono font-semibold" data-testid="namespace-edit-{ns.name}" href={namespacePageHref(ns.name, page.url.search, data.namespace)}>{ns.name}</a>
									{#if ns.description}<p class="truncate text-xs text-ink-muted" data-tip={ns.description}>{ns.description}</p>{/if}
								</td>
								<td data-label="Hold"
									class="max-w-44 truncate {ns.hold ? 'severity-error font-semibold' : 'text-ink-secondary'}"
									data-tip={holdLabel(ns.hold)}
									data-testid="namespace-hold-{ns.name}">{holdLabel(ns.hold)}</td
								>
								<td data-label="Max running" class="figure text-ink-secondary" data-testid="namespace-max-running-{ns.name}"
									>{maxRunningLabel(ns.maxRunning)}</td
								>
								<td data-label="Run retention" class="text-ink-secondary" data-testid="namespace-run-retention-{ns.name}"
									>{runRetentionLabel(ns.runRetention)}</td
								>
								<td data-label="Pools" class="text-ink-secondary" data-testid="namespace-pools-{ns.name}">
									<span class="block whitespace-nowrap"><span class="font-mono">{ns.defaultPool || DEFAULT_POOL_NAME}</span> default</span>
									<span class="block text-xs text-ink-muted">{ns.allowedPools.length > 0 ? `allows ${ns.allowedPools.join(', ')}` : 'allows any pool'}</span>
								</td>
								<td data-label="Telemetry" class="text-ink-secondary">
									<span class="block whitespace-nowrap" data-testid="namespace-telemetry-sink-{ns.name}">{telemetrySinkLabel(ns.telemetry)}</span>
									<span class="block text-xs whitespace-nowrap text-ink-muted">logs <span data-testid="namespace-task-logs-{ns.name}">{taskLogsLabel(ns.telemetry)}</span></span>
									<span class="block text-xs whitespace-nowrap text-ink-muted">attrs <span data-testid="namespace-telemetry-attribute-count-{ns.name}">{attributeCountLabel(ns.telemetry)}</span></span>
								</td>
								<td class="w-40 max-sm:col-span-2 max-sm:order-last">
									<div class="flex flex-wrap items-center gap-2 sm:justify-start">
										{#if ns.hold}
											<button
												type="button"
												data-testid="namespace-release-hold-{ns.name}"
												onclick={() => askToRelease(ns)}
												class="btn btn-sm"
											>
												Release hold
											</button>
										{:else}
											<button
												type="button"
												data-testid="namespace-hold-action-{ns.name}"
												onclick={() => askToHold(ns.name)}
												class="btn btn-sm"
											>
												Hold
											</button>
										{/if}
	{#if ns.name === 'default'}
											<span
												class="text-xs text-ink-muted"
												data-testid="namespace-delete-default-reason"
												data-tip="every Orkflow that names no namespace is in it"
											>
												undeletable
											</span>
											<button
												type="button"
												data-testid="namespace-delete-default"
												disabled
												data-tip="default cannot be deleted: every Orkflow that names no namespace is in it"
												class="btn btn-sm cursor-not-allowed"
											>
												<Icon name="trash" />Delete
											</button>
										{:else}
											<button
												type="button"
												data-testid="namespace-delete-{ns.name}"
												onclick={() => askToDelete(ns.name)}
												class="btn btn-sm"
											>
												<Icon name="trash" />Delete
											</button>
										{/if}
									</div>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		{#if data.namespacesList.length > 0 || data.page > 1}
			<Pagination testid="namespaces" noun="namespace" page={data.page} count={data.namespacesList.length} {previousHref} {nextHref} {firstHref} onnext={next} />
		{/if}
	{/if}
{/if}

{#if pendingHold}
	<ConfirmDialog
		testid="namespace-hold-confirm"
		title="Hold namespace {pendingHold}?"
		acceptLabel="Hold"
		acceptTestid="namespace-hold-confirm-accept"
		cancelTestid="namespace-hold-confirm-cancel"
		busy={admissionBusy}
		acceptDisabled={holdReasonInput.trim() === '' || holdReasonError !== ''}
		error={admissionError}
		destructive
		onaccept={() => writeHold(pendingHold, { reason: holdReasonInput })}
		oncancel={closeAdmission}
	>
		{#snippet body()}
			<p>
				Every new run of <span class="font-mono font-semibold text-ink">{pendingHold}</span> — CLI, UI, run
				again, trigger — is refused until the hold is released. Runs already accepted finish
				untouched.
			</p>
			<label class="mt-3 flex flex-col gap-1.5 text-sm">
				<span class="field-label mb-0">Reason <span class="field-hint">(the next operator reads this)</span></span>
				<input
					data-testid="namespace-hold-reason-input"
					bind:value={holdReasonInput}
					placeholder="partner CI loop, INC-4412"
					class="field h-9 px-3"
				/>
				{#if holdReasonError}
					<span class="severity-error text-xs" data-testid="namespace-hold-reason-error"
						>{holdReasonError}</span
					>
				{/if}
			</label>
			<div class="mt-3 flex items-center justify-end">
				<CliEquivalent
					testid="namespace-hold-cli"
					label="CLI equivalent of this hold"
					command={`orkctl namespace update ${pendingHold} --hold ${shellQuote(holdReasonInput.trim() === '' ? 'REASON' : holdReasonInput)}`}
				/>
			</div>
		{/snippet}
	</ConfirmDialog>
{/if}

{#if pendingRelease}
	<ConfirmDialog
		testid="namespace-release-hold-confirm"
		title="Release the hold on {pendingRelease.name}?"
		acceptLabel="Release hold"
		acceptTestid="namespace-release-hold-confirm-accept"
		cancelTestid="namespace-release-hold-confirm-cancel"
		busy={admissionBusy}
		error={admissionError}
		onaccept={() => writeHold(pendingRelease?.name ?? '', undefined)}
		oncancel={closeAdmission}
	>
		{#snippet body()}
			<p>
				This clears the hold set {fmtTimestamp(pendingRelease?.hold?.since)} for
				<span class="font-medium text-ink" data-testid="namespace-release-hold-reason"
					>"{pendingRelease?.hold?.reason}"</span
				>. New runs are accepted again; nothing refused while it was held is started.
			</p>
			<div class="mt-3 flex items-center justify-end">
				<CliEquivalent
					testid="namespace-release-hold-cli"
					label="CLI equivalent of this release"
					command={`orkctl namespace update ${pendingRelease?.name ?? ''} --release-hold`}
				/>
			</div>
		{/snippet}
	</ConfirmDialog>
{/if}

{#if pendingDelete}
	<ConfirmDialog
		testid="namespace-delete-confirm"
		title="Delete namespace {pendingDelete}?"
		acceptLabel="Delete"
		glyph="trash"
		acceptTestid="namespace-delete-confirm-accept"
		cancelTestid="namespace-delete-confirm-cancel"
		busy={deleteBusy}
		error={deleteError}
		destructive
		onaccept={confirmDelete}
		oncancel={() => {
			pendingDelete = '';
			deleteError = '';
		}}
	>
		{#snippet body()}
			<p>
				This removes the registry entry for <span class="font-mono font-semibold text-ink">{pendingDelete}</span>. It is refused while any Orkflow is still stored in it.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
