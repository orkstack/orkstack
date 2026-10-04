<script lang="ts">
	// The submission draft, for one orkflow the caller has already named.
	//
	// This is what the standalone Submit Run page used to be, minus its orkflow
	// PICKER: both places that open it — an Orkflow's own page, and a run's
	// "Run again" — already know which orkflow they mean, so choosing one
	// again would be a question with a single correct answer. What is left is
	// the part that genuinely varies: WHICH VERSION runs, WHAT INPUT it gets,
	// and the two advanced switches (idempotency key, empty-pool override).
	//
	// It renders no modal of its own. The caller owns the Dialog (or renders
	// this inline) and owns what happens after a successful submit: this
	// component reports the new run's id through `onsubmitted` and never
	// navigates, which is why the identical component works both as the
	// Orkflow page's "Submit a run" dialog and as the run page's "Run again".
	import { untrack } from 'svelte';
	import { page } from '$app/state';
	import { create } from '@bufbuild/protobuf';
	import type { JsonObject } from '@bufbuild/protobuf';
	import { errorMessage, orkstack } from '$lib/client';
	import { defaultsForInputs, inputSpecs, parseInputObject, planCli, submitCli } from '$lib/authoring';
	import { parseAssumedOutputs } from '$lib/plan';
	import AwaitCallbacks from './AwaitCallbacks.svelte';
	import CliEquivalent from './CliEquivalent.svelte';
	import DeclaredInputs from './DeclaredInputs.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import HoldBanner from './HoldBanner.svelte';
	import PlanTable from './PlanTable.svelte';
	import Select from './Select.svelte';
	import { PayloadSchema } from '$lib/gen/orkstack/v1/common_pb';
	import type { NamespaceHold } from '$lib/gen/orkstack/v1/namespace_pb';
	import type { Orkflow } from '$lib/gen/orkstack/v1/orkflow_pb';
	import { namespaceLabel } from '$lib/namespace';
	import type { AwaitCallback, OrkflowVersion, PlanRunResponse } from '$lib/gen/orkstack/v1/service_pb';
	import type { Payload } from '$lib/gen/orkstack/v1/common_pb';

	interface Props {
		namespace: string;
		orkflowName: string;
		/** Omitted or empty pins nothing: the engine resolves "current" at submit. */
		initialVersion?: string;
		/** Seeds the draft; omitted means the version's declared defaults. */
		initialInput?: JsonObject;
		/** Set by "Run again" — names the run this draft was copied from. */
		replayFromRunId?: string;
		/** The caller navigates (or closes); this component never does. */
		onsubmitted: (runId: string) => void;
	}

	let {
		namespace,
		orkflowName,
		initialVersion = '',
		initialInput = undefined,
		replayFromRunId = '',
		onsubmitted
	}: Props = $props();

	// CURRENT is the sentinel for "do not pin a version". It cannot be the
	// empty string: an empty <select>/Select value is indistinguishable from
	// an unset control, and this choice — pin, or follow the pointer — is the
	// one thing a reader of this form must not have to guess about.
	const CURRENT = '__current__';

	let selectedVersion = $state(untrack(() => initialVersion || CURRENT));
	let selectedOrkflow = $state<Orkflow | null>(null);
	let versions = $state<OrkflowVersion[]>([]);
	let versionsLoading = $state(false);
	let selectionError = $state('');
	// hold is the namespace's hold as the server holds it NOW, read fresh every
	// time the panel opens or the version changes. While it is set the submit
	// is disabled on the strength of the server's own state; a max_running
	// refusal is never predicted here — the count is the server's, at the
	// instant of the submit — and arrives verbatim in the ErrorPanel below.
	let hold = $state<NamespaceHold | null>(null);
	let inputsValue = $state<JsonObject>(untrack(() => initialInput ?? {}));
	let inputsJson = $state(untrack(() => JSON.stringify(initialInput ?? {}, null, 2)));
	let inputMode = $state<'form' | 'json'>('form');
	let idempotencyKey = $state('');
	let allowEmptyPool = $state(false);
	// originReference is recorded on the run as its origin's reference — a CI
	// job URL, a ticket. The server validates it; a refusal lands verbatim in
	// the ErrorPanel like every other.
	let originReference = $state('');
	let submitting = $state(false);
	let formError = $state('');
	let typedInputError = $state('');
	let loadedCoordinate = $state('');
	let selectionRequest = $state(0);
	// A plan describes ONE draft. It is cleared whenever the version, the input
	// or the assumed outputs change, so a table can never be read as the answer
	// to a draft it was not computed for — the silent disagreement the Plan
	// action exists to remove. A plan is a read: it sends no idempotency key,
	// no empty-pool override and no origin, and it never disables Submit.
	let plan = $state<PlanRunResponse | null>(null);
	let planning = $state(false);
	let planError = $state('');
	let assumedOutputsJson = $state('{}');
	let planRequest = 0;
	// The submission's callback tokens, shown ONCE in place of the form. Held
	// only here, in memory — like the Triggers tab's one-time secret — and
	// never put in a URL, localStorage, or the run page.
	let disclosed = $state<{ runId: string; callbacks: AwaitCallback[] } | null>(null);

	async function loadSelection(version: string, preserveInput = false) {
		const name = orkflowName;
		if (!name) return;
		const request = ++selectionRequest;
		const requested = namespace;
		versionsLoading = true;
		selectionError = '';
		selectedOrkflow = null;
		try {
			const client = orkstack(fetch);
			const [catalog, detail, namespaceDoc] = await Promise.all([
				client.orkflows.listOrkflowVersions({ name, namespace: requested }),
				client.orkflows.getOrkflow({
					name,
					version: version === CURRENT ? '' : version,
					namespace: requested
				}),
				// A namespace read that fails does not block the draft: the
				// server is the gate, and it refuses a held submit either way.
				client.namespaces.getNamespace({ name: requested }).catch(() => null)
			]);
			// A slower earlier request must not overwrite a newer selection —
			// the version dropdown is the control an operator changes twice in
			// a row while the first answer is still in flight.
			if (!stillWanted(request, requested, name, version)) return;
			versions = catalog.versions;
			selectedOrkflow = detail.orkflow ?? null;
			hold = namespaceDoc?.namespace?.hold ?? null;
			if (!preserveInput) setInputs(defaultsForInputs(detail.orkflow?.inputs));
		} catch (err) {
			if (stillWanted(request, requested, name, version)) {
				selectedOrkflow = null;
				selectionError = errorMessage(err);
			}
		} finally {
			if (stillWanted(request, requested, name, version)) versionsLoading = false;
		}
	}

	function stillWanted(request: number, forNamespace: string, name: string, version: string) {
		return (
			request === selectionRequest &&
			forNamespace === namespace &&
			name === orkflowName &&
			version === selectedVersion
		);
	}

	$effect(() => {
		// The coordinate is the whole identity of the draft the caller asked
		// for. When it changes the draft is a DIFFERENT one, so everything the
		// operator typed against the old one goes — including the idempotency
		// key, which belongs to the submission it was typed for and to no other.
		//
		// Joined on NUL because the separator has to be a character none of the
		// four parts can contain: two different coordinates that spelled one
		// string would skip the reset and keep the previous draft.
		const coordinate = `${namespace}\u0000${orkflowName}\u0000${initialVersion}\u0000${replayFromRunId}`;
		if (coordinate !== loadedCoordinate) {
			loadedCoordinate = coordinate;
			selectedVersion = initialVersion || CURRENT;
			selectedOrkflow = null;
			versions = [];
			selectionError = '';
			formError = '';
			typedInputError = '';
			idempotencyKey = '';
			allowEmptyPool = false;
			assumedOutputsJson = '{}';
			plan = null;
			planError = '';
			originReference = '';
			setInputs(initialInput ?? {});
		}
		const version = selectedVersion;
		if (!orkflowName) return;
		// The seeded input survives only for the coordinate it was seeded FOR.
		// Picking a different version asks a different question — that version's
		// declared defaults — and answering it with the previous version's
		// values would put fields the new one never declared into the payload.
		const preserveInput = initialInput !== undefined && version === (initialVersion || CURRENT);
		// untrack: loadSelection's synchronous prefix reads/writes selectionRequest,
		// versionsLoading and selectedOrkflow. Left tracked, this effect would depend
		// on its own call's side effects and retrigger itself without end.
		untrack(() => {
			void loadSelection(version, preserveInput);
		});
	});

	$effect(() => {
		// Keyed on the three things a plan answers for. Reading them is the
		// subscription; the clear is untracked so it does not subscribe to plan.
		void selectedVersion;
		void inputsJson;
		void inputsValue;
		void assumedOutputsJson;
		untrack(() => {
			planRequest++;
			plan = null;
			planError = '';
			planning = false;
		});
	});

	function setInputs(next: JsonObject) {
		inputsValue = next;
		inputsJson = JSON.stringify(next, null, 2);
	}

	function setInputMode(mode: 'form' | 'json') {
		if (mode === 'json' && typedInputError) {
			formError = 'Fix the invalid declared input before switching to JSON so the draft is not lost.';
			return;
		}
		if (mode === 'form') {
			// The JSON text is the authority in JSON mode, so it has to parse
			// before the fields can show it. Refusing here is what keeps a
			// half-typed object from being silently discarded by the switch.
			const parsed = parseInputObject(inputsJson);
			if ('error' in parsed) {
				formError = parsed.error;
				return;
			}
			inputsValue = parsed.value;
		}
		inputMode = mode;
	}

	// Two utilities that set the same property cannot both be written on one
	// element and trusted to resolve in source order, so the active and idle
	// recipes are whole class strings rather than a base plus an override.
	function modeClass(active: boolean): string {
		const tone = active ? 'font-semibold text-fg' : 'text-fg-secondary hover:text-fg';
		return `rounded px-1.5 py-0.5 transition-colors duration-150 focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none ${tone}`;
	}

	function currentVersionLabel() {
		return versions.find((entry) => entry.isCurrent)?.version ?? selectedOrkflow?.version ?? '…';
	}

	const chosenVersion = $derived(selectedVersion === CURRENT ? '' : selectedVersion);
	const specs = $derived(inputSpecs(selectedOrkflow?.inputs));
	const versionOptions = $derived([
		{ value: CURRENT, label: `Current (${currentVersionLabel()}) — resolves when submitted` },
		...versions.map((version) => ({
			value: version.version,
			label: `${version.version}${version.isCurrent ? ' — current' : ''}${version.description ? ` — ${version.description}` : ''}`
		}))
	]);
	const cliParsedInput = $derived(parseInputObject(inputsJson));
	const cli = $derived(
		'value' in cliParsedInput
			? submitCli({
					name: orkflowName,
					namespace,
					version: chosenVersion,
					input: cliParsedInput.value,
					idempotencyKey,
					allowEmptyPool,
					originReference,
					sourceRunId: replayFromRunId
				})
			: ''
	);

	const assumedParsed = $derived(parseAssumedOutputs(assumedOutputsJson));
	const assumedError = $derived('error' in assumedParsed ? assumedParsed.error : '');
	const planCommand = $derived(
		'value' in cliParsedInput && 'value' in assumedParsed
			? planCli({
					name: orkflowName,
					namespace,
					version: chosenVersion,
					input: cliParsedInput.value,
					assumedOutputs: assumedParsed.value
				})
			: ''
	);

	async function runPlan() {
		planError = '';
		const parsed = parseInputObject(inputsJson);
		if ('error' in parsed) {
			planError = parsed.error;
			return;
		}
		if (typedInputError) {
			planError = typedInputError;
			return;
		}
		if (!('value' in assumedParsed) || !selectedOrkflow) return;
		const assumedOutputs: Record<string, Payload> = {};
		for (const [taskId, data] of Object.entries(assumedParsed.value)) {
			assumedOutputs[taskId] = create(PayloadSchema, { data });
		}
		const request = ++planRequest;
		planning = true;
		try {
			const res = await orkstack(fetch).orkflows.planRun({
				orkflowName: selectedOrkflow.name,
				orkflowVersion: chosenVersion,
				namespace,
				input: create(PayloadSchema, { data: parsed.value }),
				assumedOutputs
			});
			if (request === planRequest) plan = res;
		} catch (err) {
			if (request === planRequest) planError = errorMessage(err);
		} finally {
			if (request === planRequest) planning = false;
		}
	}

	async function submit() {
		formError = '';
		const parsed = parseInputObject(inputsJson);
		if ('error' in parsed) {
			formError = parsed.error;
			return;
		}
		if (typedInputError) {
			formError = typedInputError;
			return;
		}
		if (!selectedOrkflow) {
			formError = 'This orkflow version has not loaded yet.';
			return;
		}
		inputsValue = parsed.value;
		submitting = true;
		let runId: string;
		try {
			const res = await orkstack(fetch).runs.submitRun({
				orkflowName: selectedOrkflow.name,
				orkflowVersion: chosenVersion,
				namespace,
				idempotencyKey,
				allowEmptyPool,
				originReference,
				// A draft opened from Run again records run-again <source>: a
				// claim the server checks exists in this namespace.
				sourceRunId: replayFromRunId,
				input: create(PayloadSchema, { data: parsed.value })
			});
			if (!res.run) throw new Error('engine returned no run');
			runId = res.run.id;
			if (res.awaitCallbacks.length > 0) {
				// Shown once, before anything navigates: the panel reports the run
				// only when the reader chooses to continue.
				disclosed = { runId, callbacks: res.awaitCallbacks };
				return;
			}
		} catch (err) {
			// The key is deliberately left in the field: a submission whose
			// result is uncertain is retried with the SAME key, which is the
			// only way the second attempt can deduplicate against the first.
			formError = errorMessage(err);
			submitting = false;
			return;
		}
		// Reported outside the try, because the run already exists: a caller
		// that throws while navigating must not be told the submission failed.
		// `submitting` stays true for the same reason — the caller is closing
		// this panel or leaving for the run, and re-enabling the button here
		// would offer a second submission of a draft that already succeeded.
		onsubmitted(runId);
	}
</script>

{#if disclosed}
	<AwaitCallbacks
		runId={disclosed.runId}
		callbacks={disclosed.callbacks}
		origin={page.url.origin}
		oncontinue={() => {
			const runId = disclosed?.runId ?? '';
			disclosed = null;
			onsubmitted(runId);
		}}
	/>
{:else}
<form
	class="space-y-5"
	data-testid="run-submit-panel"
	onsubmit={(event) => {
		event.preventDefault();
		void submit();
	}}
>
	{#if replayFromRunId}
		<section
			data-testid="run-submit-replay-banner"
			class="rounded-md border border-warning-200 bg-warning-50 px-4 py-3 text-sm text-warning-800"
		>
			<strong>Reviewing a new run from {replayFromRunId}.</strong>
			It starts {orkflowName}@{chosenVersion || currentVersionLabel()} with that run’s input. Submitting
			creates a new run and does not reuse its idempotency key.
		</section>
	{/if}

	<div class="flex flex-col gap-1.5">
		<label for="run-submit-version" class="text-sm font-medium text-fg">Version</label>
		<Select
			id="run-submit-version"
			testid="run-submit-version"
			value={selectedVersion}
			options={versionOptions}
			disabled={versionsLoading}
			onchange={(next) => (selectedVersion = next)}
		/>
		<p data-testid="run-submit-version-mode" class="text-xs text-fg-muted">
			{chosenVersion
				? `Pinned explicitly to ${chosenVersion}.`
				: `Current pointer (${currentVersionLabel()}) will be resolved by the engine.`}
		</p>
	</div>

	{#if selectionError}
		<ErrorPanel
			testid="run-submit-selection-error"
			subject="the versions of {orkflowName}"
			message={selectionError}
		/>
	{:else if versionsLoading && !selectedOrkflow}
		<p data-testid="run-submit-loading" class="text-sm text-fg-muted">Loading {orkflowName}…</p>
	{/if}

	{#if selectedOrkflow}
		<div class="flex items-center justify-between gap-3 border-b border-border pb-2">
			<h3 class="text-sm font-medium text-fg">Input</h3>
			<div class="flex gap-2 text-xs">
				<button
					type="button"
					data-testid="run-submit-input-form-mode"
					onclick={() => setInputMode('form')}
					class={modeClass(inputMode === 'form')}
				>
					Fields
				</button>
				<button
					type="button"
					data-testid="run-submit-input-json-mode"
					onclick={() => setInputMode('json')}
					class={modeClass(inputMode === 'json')}
				>
					JSON
				</button>
			</div>
		</div>

		{#if inputMode === 'form'}
			<DeclaredInputs
				inputs={selectedOrkflow.inputs}
				value={inputsValue}
				onchange={setInputs}
				oninvalid={(message) => (typedInputError = message)}
				resetKey={`${selectedOrkflow.name}@${selectedOrkflow.version}:${inputMode}`}
			/>
			{#if typedInputError}
				<p data-testid="run-submit-declared-input-error" class="text-sm text-danger-700">
					{typedInputError}
				</p>
			{/if}
			{#if specs.length === 0}
				<p class="text-sm text-fg-muted">
					This orkflow declares no inputs. Use JSON to send an explicit object.
				</p>
			{/if}
		{:else}
			<div class="flex flex-col gap-1.5">
				<label for="run-submit-inputs-json" class="text-sm font-medium text-fg"
					>Inputs (JSON object)</label
				>
				<textarea
					id="run-submit-inputs-json"
					data-testid="run-submit-inputs-json"
					bind:value={inputsJson}
					rows="8"
					spellcheck="false"
					class="min-h-[160px] rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				></textarea>
				<p class="text-xs text-fg-muted">
					JSON is lossless: it retains values and keys beyond the declared form fields.
				</p>
			</div>
		{/if}
	{/if}

	<details class="rounded-md border border-border bg-surface p-3">
		<summary class="cursor-pointer text-sm font-medium text-fg">Advanced submission options</summary>
		<div class="mt-3 space-y-3">
			<div class="flex flex-col gap-1.5">
				<label for="run-submit-idempotency-key" class="text-sm text-fg">
					Idempotency key <span class="text-fg-muted">(optional)</span>
				</label>
				<input
					id="run-submit-idempotency-key"
					data-testid="run-submit-idempotency-key"
					bind:value={idempotencyKey}
					class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				<p class="text-xs text-fg-muted">
					If the result is uncertain, retry with this same key. It is retained after an error.
				</p>
			</div>
			<label class="flex items-start gap-2 text-sm text-fg">
				<input
					data-testid="run-submit-allow-empty-pool"
					type="checkbox"
					bind:checked={allowEmptyPool}
					class="mt-0.5 accent-brand-600"
				/>
				Submit even if a resolved pool currently has no live Orker.
			</label>
			<div class="flex flex-col gap-1.5">
				<label for="run-submit-origin-ref" class="text-sm text-fg">
					Origin reference <span class="text-fg-muted">(optional)</span>
				</label>
				<input
					id="run-submit-origin-ref"
					data-testid="submit-origin-ref"
					bind:value={originReference}
					placeholder="https://ci.example/jobs/123"
					class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				/>
				<p class="text-xs text-fg-muted">
					A CI job URL, a ticket, or an event id recorded on the run; up to 256 bytes.
				</p>
			</div>
			<div class="flex flex-col gap-1.5">
				<label for="run-submit-assumed-outputs" class="text-sm text-fg">
					Assumed task outputs <span class="text-fg-muted">(JSON object keyed by task id, optional)</span>
				</label>
				<textarea
					id="run-submit-assumed-outputs"
					data-testid="run-submit-assumed-outputs"
					bind:value={assumedOutputsJson}
					rows="3"
					spellcheck="false"
					class="rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
				></textarea>
				{#if assumedError}
					<p data-testid="run-submit-assumed-outputs-error" class="text-sm text-danger-700">
						{assumedError}
					</p>
				{/if}
				<p class="text-xs text-fg-muted">
					Stubs a task's values.json so a condition that reads an upstream output can be planned.
					Tasks not listed are planned with an empty output.
				</p>
			</div>
		</div>
	</details>

	{#if planError}
		<ErrorPanel testid="run-plan-error" subject="a plan of {orkflowName}" message={planError} />
	{/if}

	{#if plan}
		<PlanTable {plan} />
	{/if}

	{#if formError}
		<ErrorPanel testid="run-submit-error" message={formError} />
	{/if}

	{#if hold}
		<HoldBanner namespace={namespaceLabel(namespace)} {hold} testid="submit-hold-banner" />
	{/if}

	<div class="flex flex-wrap items-center justify-between gap-3">
		<div class="flex items-center gap-2">
			<button
				type="button"
				data-testid="run-submit-plan"
				onclick={() => void runPlan()}
				disabled={planning ||
					versionsLoading ||
					!!selectionError ||
					!!typedInputError ||
					!!assumedError ||
					!selectedOrkflow}
				class="inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:bg-surface-sunken focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50"
			>
				{planning ? 'Planning…' : 'Plan'}
			</button>
			<button
				type="submit"
				data-testid="run-submit-submit"
				disabled={submitting ||
					!!hold ||
					versionsLoading ||
					!!selectionError ||
					!!typedInputError ||
					!selectedOrkflow}
				class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800 focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas focus-visible:outline-none disabled:pointer-events-none disabled:opacity-50"
			>
				{submitting ? 'Submitting…' : 'Submit run'}
			</button>
		</div>
		{#if cli}
			<div class="flex items-center gap-2">
				<CliEquivalent command={cli} testid="run-submit-cli" label="CLI equivalent of this submission" />
				{#if planCommand}
					<CliEquivalent command={planCommand} testid="run-submit-plan-cli" label="CLI equivalent of this plan" />
				{/if}
			</div>
		{:else}
			<span data-testid="run-submit-cli-unavailable" class="text-xs text-warning-700">
				Fix the input JSON to export a CLI command.
			</span>
		{/if}
	</div>
</form>
{/if}
