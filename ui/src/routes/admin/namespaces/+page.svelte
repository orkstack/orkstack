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
	import { navigating, page } from '$app/state';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import TelemetryFields from '$lib/components/TelemetryFields.svelte';
	import SignerField from '$lib/components/SignerField.svelte';
	import { isStaleEtagConflict } from '$lib/adminErrors';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { createdNamespaceHref, NAMESPACE_FILTER_MAX, namespaceFilterParam, namespacesEmptyState } from '$lib/namespaceList';
	import { errorMessage, orkstack } from '$lib/client';
	import { create } from '@bufbuild/protobuf';
	import { NamespaceHoldSchema, type Budget, type Namespace, type NamespaceHold } from '$lib/gen/orkstack/v1/namespace_pb';
	import { TaskLogExport } from '$lib/gen/orkstack/v1/telemetry_pb';
	import { DEFAULT_NAMESPACE, namespaceLabel, rememberNamespace, withNamespace } from '$lib/namespace';
	import {
		DEFAULT_POOL_NAME,
		parseAttributeLines,
		parsePoolList,
		parseSecretPatternLines,
		validateNamespaceForm,
		type NamespaceFormErrors
	} from '$lib/namespaceRules';
	import { hoursToMs, parseRetentionHours } from '$lib/retentionForm';
	import { shellQuote } from '$lib/runDetail';
	import { fmtDuration } from '$lib/stats';
	import { budgetConfigLabel } from '$lib/usage';
	import { shellWord } from '$lib/flakes';
	import { fmtTimestamp } from '$lib/status';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { Duration } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	// NamespaceTelemetryPolicy is the loosely-typed shape this page builds and
	// sends for `namespace.telemetry` — a plain object rather than the
	// generated TelemetryPolicy message type (which carries its own
	// `$typeName`), the same style the rest of this page's request bodies
	// already use for `namespace` itself.
	type NamespaceTelemetryPolicy = {
		sink: string;
		taskLogs: TaskLogExport;
		attributes: Record<string, string>;
	};

	// runRetentionLabel renders an unset run_retention as what it means: this
	// namespace inherits the deployment maximum, never a dash that could be
	// mistaken for "no retention" (the same rule allowedPoolsLabel-style
	// renderers in this file already follow for allowed_pools).
	function runRetentionLabel(d: Duration | undefined): string {
		return d ? fmtDuration(d) : 'inherited (deployment maximum)';
	}

	// telemetrySinkLabel, taskLogsLabel and attributeCountLabel render a
	// namespace's telemetry policy for the list table, the same way
	// runRetentionLabel above renders run_retention: an unset policy, or a
	// field a policy leaves at its zero value, reads as what it MEANS
	// ("inherited (default sink)", "inherited (Orker default)"), never a dash
	// a reader could mistake for "no telemetry at all" — the CLI's
	// telemetrySinkLabel/taskLogsLabel (cmd/orkctl/namespace.go) render the
	// same three fields the same way.
	// holdLabel and maxRunningLabel render namespace admission the way the
	// CLI's `namespace get` does (cmd/orkctl/namespace.go): "accepting" and
	// "unlimited" spelled out, never a dash that would read as the opposite
	// of the truth.
	function holdLabel(hold: NamespaceHold | undefined): string {
		return hold ? `since ${fmtTimestamp(hold.since)} — "${hold.reason}"` : 'accepting';
	}

	function maxRunningLabel(maxRunning: number): string {
		return maxRunning > 0 ? String(maxRunning) : 'unlimited';
	}

	// allowedSecretsLabel renders a namespace's secret-source list the way
	// the CLI's `namespace list` column does (cmd/orkctl/namespace.go):
	// "unrestricted" spelled out, never a dash, which under a heading naming
	// a security bound would read as "none allowed" — the opposite of what an
	// empty list means.
	function allowedSecretsLabel(allowedSecrets: string[]): string {
		return allowedSecrets.length > 0 ? allowedSecrets.join(', ') : 'unrestricted';
	}

	// holdRequest is what a form's hold field sends: blank is a deliberate
	// "accepting" (no hold), anything else is a hold with that reason. The
	// registry stamps since; a client never sends one.
	function holdRequest(reason: string): { reason: string } | undefined {
		return reason.trim() === '' ? undefined : { reason };
	}

	// maxRunningRequest turns the cap field's text into the number sent; blank
	// is 0, which is unlimited. validateNamespaceForm has already refused
	// anything that is not a non-negative whole number.
	function maxRunningRequest(text: string): number {
		return text.trim() === '' ? 0 : Number(text.trim());
	}

	// budgetRequest turns the four budget fields into the budget sent: both
	// amounts blank is no budget at all — on an edit, a deliberate clear.
	// validateNamespaceForm has already refused everything else the registry
	// would (budgetError).
	function budgetRequest(cpu: string, gib: string, periodHours: string, enforce: boolean) {
		if (cpu.trim() === '' && gib.trim() === '') return undefined;
		return {
			cpuSeconds: BigInt(cpu.trim() === '' ? '0' : cpu.trim()),
			gibSeconds: BigInt(gib.trim() === '' ? '0' : gib.trim()),
			period: durationFromMs(hoursToMs(Number(periodHours.trim()))),
			enforce
		};
	}

	// budgetCommand is the `orkctl namespace update` an edit's save performs
	// for the budget: the four flags, or --clear-budget when both amounts are
	// blank.
	function budgetCommand(name: string, cpu: string, gib: string, periodHours: string, enforce: boolean): string {
		if (cpu.trim() === '' && gib.trim() === '') return `orkctl namespace update ${shellWord(name)} --clear-budget`;
		const parts = ['orkctl', 'namespace', 'update', shellWord(name)];
		parts.push('--budget-cpu-seconds', cpu.trim() === '' ? '0' : cpu.trim());
		parts.push('--budget-gib-seconds', gib.trim() === '' ? '0' : gib.trim());
		parts.push('--budget-period', `${periodHours.trim() || 'H'}h`);
		parts.push(`--budget-enforce=${enforce}`);
		return parts.join(' ');
	}

	function budgetPeriodHours(b: Budget | undefined): string {
		return b?.period ? String(Number(b.period.seconds) / 3600) : '';
	}

	function telemetrySinkLabel(policy: NamespaceTelemetryPolicy | undefined): string {
		return policy?.sink ? policy.sink : 'inherited (default sink)';
	}

	function taskLogsLabel(policy: NamespaceTelemetryPolicy | undefined): string {
		switch (policy?.taskLogs) {
			case TaskLogExport.ENABLED:
				return 'enabled';
			case TaskLogExport.DISABLED:
				return 'disabled';
			default:
				return 'inherited (Orker default)';
		}
	}

	function attributeCountLabel(policy: NamespaceTelemetryPolicy | undefined): string {
		const count = policy ? Object.keys(policy.attributes).length : 0;
		return count === 0 ? '—' : `${count}`;
	}

	// taskLogsSelectValue/taskLogsFromSelectValue convert between the form's
	// three-way select ("inherit" | "enabled" | "disabled") and the proto
	// enum, the same mapping cmd/orkctl/namespace.go's parseTaskLogsFlag
	// makes for --task-logs.
	type TaskLogsChoice = 'inherit' | 'enabled' | 'disabled';

	function taskLogsSelectValue(taskLogs: TaskLogExport | undefined): TaskLogsChoice {
		switch (taskLogs) {
			case TaskLogExport.ENABLED:
				return 'enabled';
			case TaskLogExport.DISABLED:
				return 'disabled';
			default:
				return 'inherit';
		}
	}

	function taskLogsFromSelectValue(choice: TaskLogsChoice): TaskLogExport {
		switch (choice) {
			case 'enabled':
				return TaskLogExport.ENABLED;
			case 'disabled':
				return TaskLogExport.DISABLED;
			default:
				return TaskLogExport.UNSPECIFIED;
		}
	}

	// buildTelemetryPolicy assembles the TelemetryPolicy a create/update
	// request sends from the form's three fields, or undefined when all three
	// are at their "no override" value — the difference between "inherited"
	// and "explicitly asked for every default" (the same distinction
	// applyTelemetryFlags draws in cmd/orkctl/namespace.go).
	function buildTelemetryPolicy(
		sink: string,
		taskLogs: TaskLogsChoice,
		attributesText: string
	): NamespaceTelemetryPolicy | undefined {
		const trimmedSink = sink.trim();
		const { attributes } = parseAttributeLines(attributesText);
		if (trimmedSink === '' && taskLogs === 'inherit' && Object.keys(attributes).length === 0) {
			return undefined;
		}
		return {
			sink: trimmedSink,
			taskLogs: taskLogsFromSelectValue(taskLogs),
			attributes
		};
	}

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

	// ── paging and the name filter ────────────────────────────────────────
	// The same coordinates, builder and cursor stack the Orkflows list keeps
	// (orkflows/+page.svelte; docs/UI_CLI_PARITY.md "Paging in the browser"),
	// plus `q`, the name filter. `ns` rides along so links out of this page
	// keep the switcher's choice; it does not filter this view.
	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), q: data.q, size: String(data.size), cursor: data.cursor, page: String(data.page) });
	function href(changes: Record<string, string>) { return listHref('/admin/namespaces', { ...coordinates, ...changes }); }
	const stackKey = $derived(pageStackKey('/admin/namespaces', coordinates));
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
	// Page size applies on change, as on Orkflows: this page's filter is one
	// field with its own submit, and size is not part of it.
	function setSize(size: string) {
		writePageStack(stackKey, []);
		void goto(href({ size, cursor: '', page: '' }));
	}

	// The filter input is a DRAFT of `q` (the Runs page's rule): applied on
	// submit, refilled from what the page loaded after any navigation, and a
	// new filter is a new page 1.
	function filterDraftFromUrl() {
		return data.q;
	}
	let filterDraft = $state(filterDraftFromUrl());
	$effect(() => {
		filterDraft = filterDraftFromUrl();
	});
	function applyFilter(event: SubmitEvent) {
		event.preventDefault();
		void goto(href({ q: namespaceFilterParam(filterDraft), cursor: '', page: '' }), { keepFocus: true, noScroll: true });
	}
	const emptyState = $derived(
		namespacesEmptyState({ filter: data.q, page: data.page, count: data.namespacesList.length, hasNext: data.nextPageToken !== '' })
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
			const res = await orkstack(fetch).namespaces.createNamespace({
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
			await goto(createdNamespaceHref(coordinates, createdName), { invalidateAll: true, keepFocus: true, noScroll: true });
		} catch (err) {
			createServerError = errorMessage(err);
		} finally {
			createBusy = false;
		}
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
	}

	function closeEdit() {
		editingName = '';
		editConflict = false;
		editServerError = '';
	}
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
			await orkstack(fetch).namespaces.updateNamespace({
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
			closeEdit();
			await invalidateAll();
		} catch (err) {
			// UpdateNamespace's compare-and-swap is FailedPrecondition for TWO
			// unrelated reasons — a stale etag, and this engine having no
			// namespace registry at all (internal/api/namespaces.go's own
			// `registry()`) — and only the first is fixed by "reload and
			// re-apply". isStaleEtagConflict tells them apart by the server's
			// own words (see $lib/adminErrors.ts): a FailedPrecondition that
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
	// re-applies" (UpdateNamespace's own doc comment). The FORM's own
	// fields (description/pool) are left exactly as the user typed them —
	// that is the edit being re-applied. Everything this form does NOT
	// expose is instead refreshed from this fresh read, not just the etag:
	// the earlier version of this fix moved the concurrency token forward
	// while replaying `editLabels` captured at the ORIGINAL open, so a
	// concurrent label change made underneath this edit (the same kind of
	// out-of-band write the conflict itself proves happened) was silently
	// overwritten by the retry. Labels are the one such field this page
	// carries today; a future hidden field belongs here too.
	async function reloadEtagAndRetry() {
		editBusy = true;
		editServerError = '';
		try {
			const res = await orkstack(fetch).namespaces.getNamespace({ name: editingName });
			editEtag = res.namespace?.etag ?? '';
			editLabels = { ...res.namespace?.labels };
			editConflict = false;
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
			const client = orkstack(fetch);
			const res = await client.namespaces.getNamespace({ name });
			const current = res.namespace;
			if (!current) throw new Error(`namespace ${name} came back empty`);
			await client.namespaces.updateNamespace({
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
			await orkstack(fetch).namespaces.deleteNamespace({ name: deletedName });
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
					replaceState: true,
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

<p class="mb-4 text-sm text-fg-muted" data-testid="namespaces-scope-caption">
	All namespaces this deployment has — the namespace selector above does not filter this view; the name filter below does.
</p>

{#if data.namespacesError}
	<ErrorPanel
		testid="namespaces-error"
		retryTestid="namespaces-retry"
		subject="the namespace registry"
		message={data.namespacesError}
		busy={retrying}
		onretry={retry}
	/>
{:else}
	<section class="mb-8 rounded-lg border border-border bg-surface p-5 shadow-xs">
		<h2 class="mb-3 text-sm font-semibold text-fg">Create a namespace</h2>
		<form
			class="grid gap-3 sm:grid-cols-2"
			onsubmit={(e) => {
				e.preventDefault();
				void submitCreate();
			}}
		>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Name</span>
				<input
					data-testid="ns-create-name"
					bind:value={createName}
					placeholder="payments"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.name}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-name"
						>{createErrors.name}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Description</span>
				<input
					data-testid="ns-create-description"
					bind:value={createDescription}
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Default pool (blank = {DEFAULT_POOL_NAME})</span>
				<input
					data-testid="ns-create-default-pool"
					bind:value={createDefaultPool}
					placeholder={DEFAULT_POOL_NAME}
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.defaultPool}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-default-pool"
						>{createErrors.defaultPool}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Allowed pools (comma-separated, blank = unrestricted)</span>
				<input
					data-testid="ns-create-allowed-pools"
					bind:value={createAllowedPools}
					placeholder="cpu, gpu"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.allowedPools}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-allowed-pools"
						>{createErrors.allowedPools}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Run retention (hours, blank = inherit)</span>
				<input
					data-testid="ns-create-run-retention"
					bind:value={createRunRetentionHours}
					inputmode="decimal"
					placeholder="inherit"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				<span class="text-xs text-fg-muted">Blank inherits the deployment maximum.</span>
				{#if createErrors.runRetention}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-run-retention"
						>{createErrors.runRetention}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Hold reason (blank = accepting)</span>
				<input
					data-testid="ns-create-hold-reason"
					bind:value={createHoldReason}
					placeholder="accepting"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.holdReason}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-hold-reason"
						>{createErrors.holdReason}</span
					>
				{/if}
			</label>
			<label class="flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Max running (0 = unlimited)</span>
				<input
					data-testid="ns-create-max-running"
					bind:value={createMaxRunning}
					inputmode="numeric"
					placeholder="0"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if createErrors.maxRunning}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-max-running"
						>{createErrors.maxRunning}</span
					>
				{/if}
			</label>
			<fieldset class="flex flex-col gap-2 rounded-md border border-border px-3 py-2 text-sm sm:col-span-2" data-testid="ns-create-budget">
				<legend class="px-1 font-medium text-fg">Budget (reserved cpu and memory over a rolling period; both amounts blank = unbounded)</legend>
				<div class="grid gap-3 sm:grid-cols-4">
					<label class="flex flex-col gap-1.5">
						<span class="text-xs font-medium text-fg">Budget cpu-seconds</span>
						<input data-testid="ns-create-budget-cpu-seconds" bind:value={createBudgetCpuSeconds} inputmode="numeric" placeholder="unbounded" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="text-xs font-medium text-fg">Budget GiB-seconds</span>
						<input data-testid="ns-create-budget-gib-seconds" bind:value={createBudgetGibSeconds} inputmode="numeric" placeholder="unbounded" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
					</label>
					<label class="flex flex-col gap-1.5">
						<span class="text-xs font-medium text-fg">Budget period (hours)</span>
						<input data-testid="ns-create-budget-period-hours" bind:value={createBudgetPeriodHours} inputmode="numeric" placeholder="720" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
					</label>
					<label class="flex items-center gap-2 self-end pb-2">
						<input type="checkbox" class="size-4 accent-brand-600" data-testid="ns-create-budget-enforce" bind:checked={createBudgetEnforce} />
						<span class="text-xs font-medium text-fg">Enforce</span>
					</label>
				</div>
				<span class="text-xs text-fg-muted">Units are RESERVED cpu and memory × attempt time, not measured utilization. Unchecked, the budget is reported and never refuses; checked, new runs are refused once it is reached — check the Usage card answers first, because an enforced budget that cannot be measured refuses every submit.</span>
				{#if createErrors.budget}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-budget">{createErrors.budget}</span>
				{/if}
			</fieldset>
			<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
				<span class="font-medium text-fg"
					>Allowed secrets (one scheme:pattern per line; blank = unrestricted)</span
				>
				<textarea
					data-testid="ns-create-allowed-secrets"
					bind:value={createAllowedSecrets}
					rows="2"
					placeholder="env:SANDBOX_*"
					spellcheck="false"
					class="rounded-md border border-border bg-surface px-3 py-2 font-mono text-xs text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				></textarea>
				<span class="text-xs text-fg-muted"
					>Bounds which <code>source:</code> values this namespace's tasks may name. Each Orker's own
					<code>ORK_ORKER_SECRET_ALLOW</code> still applies.</span
				>
				{#if createErrors.allowedSecrets}
					<span class="text-xs text-danger-700" data-testid="ns-create-error-allowed-secrets"
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
				attributesHint="Stamped on every signal of this namespace's runs. Blank sets none of its own."
				attributesError={createErrors.telemetryAttributes}
				attributesErrorTestId="ns-create-error-telemetry-attributes"
			/>
			<div class="sm:col-span-2">
				<button
					type="submit"
					data-testid="ns-create-submit"
					disabled={createBusy}
					class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
				>
					{createBusy ? 'Creating…' : 'Create namespace'}
				</button>
			</div>
		</form>
		{#if createServerError}
			<p
				class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
				data-testid="ns-create-server-error"
			>
				{createServerError}
			</p>
		{/if}
		{#if createSuccess}
			<p class="mt-3 text-sm text-succeeded-fg" data-testid="ns-create-success">{createSuccess}</p>
		{/if}
	</section>

	<form class="mb-3 flex flex-wrap items-end gap-2" role="search" onsubmit={applyFilter} data-testid="namespaces-filter-form">
		<div class="flex flex-col gap-1">
			<label class="text-sm font-medium text-fg" for="namespaces-filter">Name contains</label>
			<input
				id="namespaces-filter"
				type="search"
				class="h-9 w-64 rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none"
				placeholder="any name"
				autocomplete="off"
				spellcheck="false"
				maxlength={NAMESPACE_FILTER_MAX}
				bind:value={filterDraft}
				data-testid="namespaces-filter"
			/>
		</div>
		<button
			type="submit"
			class="inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
			data-testid="namespaces-filter-apply">Filter</button
		>
		{#if data.q}
			<a
				href={href({ q: '', cursor: '', page: '' })}
				class="inline-flex h-9 items-center px-2 text-sm font-medium text-brand-700 hover:text-brand-800"
				data-testid="namespaces-filter-clear">Clear filter</a
			>
		{/if}
	</form>

	{#if busy}
		<p class="text-sm text-fg-muted" data-testid="namespaces-loading">Loading namespaces…</p>
	{:else}
		{#if emptyState === 'only-default'}
			<p class="mb-3 text-sm text-fg-muted" data-testid="namespaces-empty">
				No namespaces beyond <code>default</code> yet — it is undeletable
				below because every orkflow that names no namespace is in it. Create one above.
			</p>
		{:else if emptyState === 'no-match'}
			<p class="mb-3 text-sm text-fg-muted" data-testid="namespaces-empty">
				No namespace name contains <code>{data.q}</code>.
			</p>
		{:else if emptyState === 'past-end'}
			<p class="mb-3 text-sm text-fg-muted" data-testid="namespaces-empty">
				Nothing on this page — the namespaces it held are gone. Start again from the first page.
			</p>
		{/if}
		{#if data.namespacesList.length > 0}
			<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
				<table class="w-full min-w-[720px] border-collapse text-left text-sm" data-testid="namespace-list">
					<thead class="bg-surface-sunken">
						<tr>
							<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Name</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Description</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Default pool</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Allowed pools</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Allowed secrets</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Run retention</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Hold</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Max running</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Signed by</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Budget</th>
	<th class="h-10 px-4 text-xs font-medium text-fg-muted">Telemetry sink</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Task logs</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Attrs</th>
							<th class="h-10 px-4 text-xs font-medium text-fg-muted">Updated</th>
							<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted"></th>
						</tr>
					</thead>
					<tbody>
						{#each data.namespacesList as ns (ns.name)}
							<tr
								class="border-t border-border-subtle transition-colors duration-150 {ns.hold
									? 'bg-warning-50 hover:bg-warning-100'
									: 'hover:bg-surface-hover'}"
								data-testid="namespace-row-{ns.name}"
								data-held={ns.hold ? 'true' : 'false'}
							>
								<td class="h-11 px-4 pl-5 align-middle font-medium text-fg">{ns.name}</td>
								<td class="h-11 max-w-xs truncate px-4 align-middle text-fg-secondary"
									>{ns.description || '—'}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary"
									>{ns.defaultPool || DEFAULT_POOL_NAME}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary">
									{ns.allowedPools.length > 0 ? ns.allowedPools.join(', ') : 'unrestricted'}
								</td>
								<td
									class="h-11 max-w-xs truncate px-4 align-middle font-mono text-xs text-fg-secondary"
									title={allowedSecretsLabel(ns.allowedSecrets)}
									data-testid="namespace-allowed-secrets-{ns.name}">{allowedSecretsLabel(ns.allowedSecrets)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="namespace-run-retention-{ns.name}"
									>{runRetentionLabel(ns.runRetention)}</td
								>
								<td
									class="h-11 max-w-xs truncate px-4 align-middle {ns.hold
										? 'font-medium text-warning-800'
										: 'text-fg-secondary'}"
									title={holdLabel(ns.hold)}
									data-testid="namespace-hold-{ns.name}">{holdLabel(ns.hold)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary tabular-nums" data-testid="namespace-max-running-{ns.name}"
									>{maxRunningLabel(ns.maxRunning)}</td
								>
								<td class="h-11 px-4 align-middle font-mono text-xs text-fg-secondary" data-testid="namespace-require-signed-by-{ns.name}"
									>{ns.requireSignedBy || '—'}</td
								>
								<td class="h-11 px-4 align-middle whitespace-nowrap text-fg-secondary" data-testid="namespace-budget-{ns.name}"
									>{budgetConfigLabel(ns.budget)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="namespace-telemetry-sink-{ns.name}"
									>{telemetrySinkLabel(ns.telemetry)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary" data-testid="namespace-task-logs-{ns.name}"
									>{taskLogsLabel(ns.telemetry)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-secondary tabular-nums" data-testid="namespace-telemetry-attribute-count-{ns.name}"
									>{attributeCountLabel(ns.telemetry)}</td
								>
								<td class="h-11 px-4 align-middle text-fg-muted tabular-nums"
									>{fmtTimestamp(ns.updatedAt)}</td
								>
								<td class="h-11 px-4 pr-5 align-middle">
									<div class="flex items-center justify-end gap-2">
										<button
											type="button"
											data-testid="namespace-edit-{ns.name}"
											onclick={() => openEdit(ns)}
											class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
										>
											Edit
										</button>
										{#if ns.hold}
											<button
												type="button"
												data-testid="namespace-release-hold-{ns.name}"
												onclick={() => askToRelease(ns)}
												class="inline-flex h-8 items-center rounded-md border border-warning-300 bg-surface px-3 text-xs font-medium text-warning-800 shadow-xs transition-colors duration-150 hover:bg-warning-50"
											>
												Release hold
											</button>
										{:else}
											<button
												type="button"
												data-testid="namespace-hold-action-{ns.name}"
												onclick={() => askToHold(ns.name)}
												class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
											>
												Hold…
											</button>
										{/if}
	{#if ns.name === 'default'}
											<span
												class="text-xs text-fg-muted"
												data-testid="namespace-delete-default-reason"
												title="every orkflow that names no namespace is in it"
											>
												undeletable
											</span>
											<button
												type="button"
												data-testid="namespace-delete-default"
												disabled
												title="default cannot be deleted: every orkflow that names no namespace is in it"
												class="inline-flex h-8 cursor-not-allowed items-center rounded-md border border-border px-3 text-xs text-fg-muted opacity-50"
											>
												Delete
											</button>
										{:else}
											<button
												type="button"
												data-testid="namespace-delete-{ns.name}"
												onclick={() => askToDelete(ns.name)}
												class="inline-flex h-8 items-center rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50"
											>
												Delete
											</button>
										{/if}
									</div>
								</td>
							</tr>
							{#if editingName === ns.name}
								<tr>
									<td colspan="15" class="bg-surface-sunken px-4 py-4">
										<form
											data-testid="namespace-edit-form-{ns.name}"
											class="grid gap-3 sm:grid-cols-2"
											onsubmit={(e) => {
												e.preventDefault();
												void submitEdit();
											}}
										>
											<label class="flex flex-col gap-1.5 text-sm">
												<span class="font-medium text-fg">Description</span>
												<input
													data-testid="ns-edit-description-{ns.name}"
													bind:value={editDescription}
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
											</label>
											<label class="flex flex-col gap-1.5 text-sm">
												<span class="font-medium text-fg">Default pool</span>
												<input
													data-testid="ns-edit-default-pool-{ns.name}"
													bind:value={editDefaultPool}
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
												{#if editErrors.defaultPool}
													<span class="text-xs text-danger-700">{editErrors.defaultPool}</span>
												{/if}
											</label>
											<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
												<span class="font-medium text-fg">Allowed pools (comma-separated)</span>
												<input
													data-testid="ns-edit-allowed-pools-{ns.name}"
													bind:value={editAllowedPools}
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
												{#if editErrors.allowedPools}
													<span class="text-xs text-danger-700">{editErrors.allowedPools}</span>
												{/if}
											</label>
											<label class="flex flex-col gap-1.5 text-sm">
												<span class="font-medium text-fg">Run retention (hours, blank = inherit)</span>
												<input
													data-testid="ns-edit-run-retention-{ns.name}"
													bind:value={editRunRetentionHours}
													inputmode="decimal"
													placeholder="inherit"
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
												{#if editErrors.runRetention}
													<span class="text-xs text-danger-700">{editErrors.runRetention}</span>
												{/if}
											</label>
											<label class="flex flex-col gap-1.5 text-sm">
												<span class="font-medium text-fg">Hold reason (blank = accepting)</span>
												<input
													data-testid="ns-edit-hold-reason-{ns.name}"
													bind:value={editHoldReason}
													placeholder="accepting"
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
												<span class="text-xs text-fg-muted">A reason closes the namespace to new runs; blank releases it.</span>
												{#if editErrors.holdReason}
													<span class="text-xs text-danger-700">{editErrors.holdReason}</span>
												{/if}
											</label>
											<label class="flex flex-col gap-1.5 text-sm">
												<span class="font-medium text-fg">Max running (0 = unlimited)</span>
												<input
													data-testid="ns-edit-max-running-{ns.name}"
													bind:value={editMaxRunning}
													inputmode="numeric"
													placeholder="0"
													class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												/>
												{#if editErrors.maxRunning}
													<span class="text-xs text-danger-700">{editErrors.maxRunning}</span>
												{/if}
											</label>
											<fieldset class="flex flex-col gap-2 rounded-md border border-border px-3 py-2 text-sm sm:col-span-2" data-testid="ns-edit-budget-{ns.name}">
												<legend class="px-1 font-medium text-fg">Budget (reserved cpu and memory over a rolling period; both amounts blank = unbounded)</legend>
												<div class="grid gap-3 sm:grid-cols-4">
													<label class="flex flex-col gap-1.5">
														<span class="text-xs font-medium text-fg">Budget cpu-seconds</span>
														<input data-testid="ns-edit-budget-cpu-seconds-{ns.name}" bind:value={editBudgetCpuSeconds} inputmode="numeric" placeholder="unbounded" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
													</label>
													<label class="flex flex-col gap-1.5">
														<span class="text-xs font-medium text-fg">Budget GiB-seconds</span>
														<input data-testid="ns-edit-budget-gib-seconds-{ns.name}" bind:value={editBudgetGibSeconds} inputmode="numeric" placeholder="unbounded" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
													</label>
													<label class="flex flex-col gap-1.5">
														<span class="text-xs font-medium text-fg">Budget period (hours)</span>
														<input data-testid="ns-edit-budget-period-hours-{ns.name}" bind:value={editBudgetPeriodHours} inputmode="numeric" placeholder="720" class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none" />
													</label>
													<label class="flex items-center gap-2 self-end pb-2">
														<input type="checkbox" class="size-4 accent-brand-600" data-testid="ns-edit-budget-enforce-{ns.name}" bind:checked={editBudgetEnforce} />
														<span class="text-xs font-medium text-fg">Enforce</span>
													</label>
												</div>
												<span class="text-xs text-fg-muted">Units are RESERVED cpu and memory × attempt time, not measured utilization. Unchecked, the budget is reported and never refuses; checked, new runs are refused once it is reached — check the Usage card answers first, because an enforced budget that cannot be measured refuses every submit.</span>
												{#if editErrors.budget}
													<span class="text-xs text-danger-700" data-testid="ns-edit-error-budget-{ns.name}">{editErrors.budget}</span>
												{/if}
											</fieldset>
											<label class="flex flex-col gap-1.5 text-sm sm:col-span-2">
												<span class="font-medium text-fg"
													>Allowed secrets (one scheme:pattern per line; blank = unrestricted)</span
												>
												<textarea
													data-testid="ns-edit-allowed-secrets-{ns.name}"
													bind:value={editAllowedSecrets}
													rows="2"
													placeholder="unrestricted"
													spellcheck="false"
													class="rounded-md border border-border bg-surface px-3 py-2 font-mono text-xs text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
												></textarea>
												<span class="text-xs text-fg-muted"
													>Bounds which <code>source:</code> values this namespace's tasks may name. Each
													Orker's own <code>ORK_ORKER_SECRET_ALLOW</code> still applies.</span
												>
												{#if editErrors.allowedSecrets}
													<span class="text-xs text-danger-700" data-testid="ns-edit-error-allowed-secrets-{ns.name}"
														>{editErrors.allowedSecrets}</span
													>
												{/if}
											</label>
											<SignerField
												bind:value={editRequireSignedBy}
												keys={data.signingKeys}
												keysError={data.signingKeysError}
												testid="ns-edit-require-signed-by-{ns.name}"
												error={editErrors.requireSignedBy}
												errorTestid="ns-edit-error-require-signed-by-{ns.name}"
											/>
											<TelemetryFields
												bind:sink={editTelemetrySink}
												bind:taskLogs={editTaskLogs}
												bind:attributesText={editTelemetryAttributesText}
												sinks={data.telemetrySinks}
												sinksError={data.telemetrySinksError}
												idPrefix="ns-edit"
												testIdSuffix="-{ns.name}"
												attributesError={editErrors.telemetryAttributes}
											/>
											<div class="flex items-center gap-2 sm:col-span-2">
												<button
													type="submit"
													data-testid="ns-edit-save-{ns.name}"
													disabled={editBusy}
													class="inline-flex h-8 items-center rounded-md bg-brand-600 px-3 text-xs font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 disabled:pointer-events-none disabled:opacity-50"
												>
													{editBusy ? 'Saving…' : 'Save'}
												</button>
												<CliEquivalent
													command={budgetCommand(ns.name, editBudgetCpuSeconds, editBudgetGibSeconds, editBudgetPeriodHours, editBudgetEnforce)}
													testid="ns-edit-cli-{ns.name}"
												/>
												<button
													type="button"
													data-testid="ns-edit-cancel-{ns.name}"
													disabled={editBusy}
													onclick={closeEdit}
													class="inline-flex h-8 items-center rounded-md border border-border bg-surface px-3 text-xs font-medium text-fg shadow-xs transition-colors duration-150 hover:bg-surface-hover disabled:pointer-events-none disabled:opacity-50"
												>
													Cancel
												</button>
											</div>
										</form>
										{#if editConflict}
											<div
												class="mt-3 rounded-md border border-warning-200 bg-warning-50 px-3 py-2 text-sm text-warning-800"
												data-testid="namespace-edit-conflict-{ns.name}"
											>
												<p>{editServerError}</p>
												<p class="mt-1 text-xs text-warning-700">
													Namespace {ns.name} changed since it was read — reload and re-apply.
												</p>
												<button
													type="button"
													data-testid="namespace-edit-conflict-retry-{ns.name}"
													onclick={reloadEtagAndRetry}
													disabled={editBusy}
													class="mt-2 inline-flex h-7 items-center rounded-md border border-warning-300 bg-surface px-3 text-xs font-medium text-warning-800 shadow-xs hover:bg-warning-50 disabled:pointer-events-none disabled:opacity-50"
												>
													Reload latest and re-apply my edit
												</button>
											</div>
										{:else if editServerError}
											<p
												class="mt-3 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
												data-testid="namespace-edit-error-{ns.name}"
											>
												{editServerError}
											</p>
										{/if}
									</td>
								</tr>
							{/if}
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
		{#if data.namespacesList.length > 0 || data.page > 1}
			<Pagination testid="namespaces" noun="namespace" page={data.page} count={data.namespacesList.length} {previousHref} {nextHref} {firstHref} onnext={next}>
				<div class="flex items-center gap-2 text-sm"><label class="font-medium text-fg" for="namespaces-page-size">Page size</label><div class="w-20"><Select id="namespaces-page-size" testid="namespaces-page-size" value={String(data.size)} options={sizeOptions} onchange={setSize} /></div></div>
			</Pagination>
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
				Every new run of <span class="font-medium text-fg">{pendingHold}</span> — CLI, UI, run
				again, trigger — is refused until the hold is released. Runs already accepted finish
				untouched.
			</p>
			<label class="mt-3 flex flex-col gap-1.5 text-sm">
				<span class="font-medium text-fg">Reason (the next operator reads this)</span>
				<input
					data-testid="namespace-hold-reason-input"
					bind:value={holdReasonInput}
					placeholder="partner CI loop, INC-4412"
					class="h-9 rounded-md border border-border bg-surface px-3 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				{#if holdReasonError}
					<span class="text-xs text-danger-700" data-testid="namespace-hold-reason-error"
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
				<span class="font-medium text-fg" data-testid="namespace-release-hold-reason"
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
				This removes the registry entry for <span class="font-medium text-fg">{pendingDelete}</span
				>. It is refused while any orkflow is still stored in it.
			</p>
		{/snippet}
	</ConfirmDialog>
{/if}
