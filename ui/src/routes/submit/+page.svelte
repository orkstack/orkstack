<script lang="ts">
	import { untrack } from 'svelte';
	import { goto, invalidateAll } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import type { JsonObject } from '@bufbuild/protobuf';
	import { errorMessage, orkstack } from '$lib/client';
	import DeclaredInputs from '$lib/components/DeclaredInputs.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { defaultsForInputs, inputSpecs, parseInputObject, submitCli } from '$lib/authoring';
	import { PayloadSchema } from '$lib/gen/orkstack/v1/common_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const CURRENT = '__current__';
	let retrying = $state(false);
	let selectedName = $state(untrack(() => data.preselect || data.orkflows[0]?.name || ''));
	let selectedVersion = $state(untrack(() => data.preselectVersion || CURRENT));
	let selectedOrkflow = $state(untrack(() => data.selectedOrkflow));
	let versions = $state<{ version: string; isCurrent: boolean; description: string }[]>([]);
	let versionsLoading = $state(false);
	let selectionError = $state('');
	let inputsValue = $state<JsonObject>(untrack(() => (data.replay?.input ?? defaultsForInputs(data.selectedOrkflow?.inputs)) as JsonObject));
	let inputsJson = $state(untrack(() => JSON.stringify(data.replay?.input ?? defaultsForInputs(data.selectedOrkflow?.inputs), null, 2)));
	let inputMode = $state<'form' | 'json'>('form');
	let idempotencyKey = $state('');
	let allowEmptyPool = $state(false);
	let submitting = $state(false);
	let formError = $state('');
	let copied = $state(false);
	let typedInputError = $state('');
	let loadedCoordinate = $state('');
	let selectionRequest = $state(0);

	async function retry() { retrying = true; try { await invalidateAll(); } finally { retrying = false; } }

	async function loadSelection(name: string, version: string, preserveInput = false) {
		if (!name) return;
		const request = ++selectionRequest;
		const namespace = data.namespace;
		versionsLoading = true;
		selectionError = '';
		selectedOrkflow = null;
		try {
			const client = orkstack(fetch);
			const [catalog, detail] = await Promise.all([
				client.orkflows.listOrkflowVersions({ name, namespace }),
				client.orkflows.getOrkflow({ name, version: version === CURRENT ? '' : version, namespace })
			]);
			if (request !== selectionRequest || namespace !== data.namespace || name !== selectedName || version !== selectedVersion) return;
			versions = catalog.versions;
			selectedOrkflow = detail.orkflow ?? null;
			if (!preserveInput) setInputs(defaultsForInputs(detail.orkflow?.inputs));
		} catch (err) {
			if (request === selectionRequest && namespace === data.namespace && name === selectedName && version === selectedVersion) {
				selectedOrkflow = null;
				selectionError = errorMessage(err);
			}
		} finally {
			if (request === selectionRequest && namespace === data.namespace && name === selectedName && version === selectedVersion) versionsLoading = false;
		}
	}

	$effect(() => {
		const coordinate = `${data.namespace}\u0000${data.preselect}\u0000${data.preselectVersion}\u0000${data.replay?.id ?? ''}`;
		if (coordinate !== loadedCoordinate) {
			loadedCoordinate = coordinate;
			selectedName = data.preselect || data.orkflows[0]?.name || '';
			selectedVersion = data.preselectVersion || CURRENT;
			selectedOrkflow = data.selectedOrkflow;
			versions = [];
			selectionError = '';
			typedInputError = '';
			idempotencyKey = '';
			allowEmptyPool = false;
			setInputs((data.replay?.input ?? defaultsForInputs(data.selectedOrkflow?.inputs)) as JsonObject);
		}
		const name = selectedName;
		const version = selectedVersion;
		if (!name) return;
		const preserveInput = data.replay !== null && name === data.preselect && version === data.preselectVersion;
		// untrack: loadSelection's synchronous prefix reads/writes selectionRequest,
		// versionsLoading and selectedOrkflow. Left tracked, this effect would depend
		// on its own call's side effects and retrigger itself without end.
		untrack(() => { void loadSelection(name, version, preserveInput); });
	});

	function selectName(event: Event) { selectedName = (event.currentTarget as HTMLSelectElement).value; selectedVersion = CURRENT; }
	function selectVersion(event: Event) { selectedVersion = (event.currentTarget as HTMLSelectElement).value; }
	function setInputs(next: JsonObject) { inputsValue = next; inputsJson = JSON.stringify(next, null, 2); }
	function setInputMode(mode: 'form' | 'json') {
		if (mode === 'json' && typedInputError) {
			formError = 'Fix the invalid declared input before switching to JSON so the draft is not lost.';
			return;
		}
		if (mode === 'form') {
			const parsed = parseInputObject(inputsJson);
			if ('error' in parsed) { formError = parsed.error; return; }
			inputsValue = parsed.value;
		}
		inputMode = mode;
	}
	function currentVersionLabel() { return versions.find((entry) => entry.isCurrent)?.version ?? selectedOrkflow?.version ?? '…'; }
	const chosenVersion = $derived(selectedVersion === CURRENT ? '' : selectedVersion);
	const specs = $derived(inputSpecs(selectedOrkflow?.inputs));
	const cliParsedInput = $derived(parseInputObject(inputsJson));
	const cli = $derived(selectedOrkflow && 'value' in cliParsedInput ? submitCli({ name: selectedOrkflow.name, namespace: data.namespace, version: chosenVersion, input: cliParsedInput.value, idempotencyKey, allowEmptyPool }) : '');
	async function copyCli() { try { await navigator.clipboard.writeText(cli); copied = true; } catch { formError = 'Could not copy the CLI command. Select and copy it manually.'; } }

	async function submit() {
		formError = '';
		const parsed = parseInputObject(inputsJson);
		if ('error' in parsed) { formError = parsed.error; return; }
		if (typedInputError) { formError = typedInputError; return; }
		if (!selectedOrkflow) { formError = 'Choose an orkflow version first.'; return; }
		inputsValue = parsed.value;
		submitting = true;
		const submittedNamespace = data.namespace;
		try {
			const res = await orkstack(fetch).runs.submitRun({ orkflowName: selectedOrkflow.name, orkflowVersion: chosenVersion, namespace: submittedNamespace, idempotencyKey, allowEmptyPool, input: create(PayloadSchema, { data: parsed.value }) });
			if (!res.run) throw new Error('engine returned no run');
			await goto(withNamespace(`/runs/${res.run.id}`, submittedNamespace));
		} catch (err) { formError = errorMessage(err); submitting = false; }
	}
</script>

<svelte:head><title>Submit run — OrkStack</title></svelte:head>
<h1 class="mb-2 text-xl font-semibold tracking-tight text-fg">Submit a run</h1>
<p class="mb-6 text-sm text-fg-muted">Namespace <span class="font-medium text-fg">{namespaceLabel(data.namespace)}</span>.</p>

{#if data.error}
	<ErrorPanel testid="engine-error" retryTestid="submit-retry" subject="the orkflows in namespace {namespaceLabel(data.namespace)}" message={data.error} busy={retrying} onretry={retry} />
{:else if retrying}
	<p class="text-sm text-fg-muted" data-testid="submit-loading">Loading orkflows…</p>
{:else if data.orkflows.length === 0}
	<p class="text-sm text-fg-muted" data-testid="submit-empty">No orkflows in namespace {namespaceLabel(data.namespace)} yet. <a href={withNamespace('/apply', data.namespace)} class="font-medium text-brand-700 hover:underline">Apply one first.</a></p>
{:else}
	<form class="max-w-3xl space-y-5" data-testid="submit-form" onsubmit={(event) => { event.preventDefault(); void submit(); }}>
		{#if data.replay}<section data-testid="replay-review" class="rounded-md border border-warning-200 bg-warning-50 px-4 py-3 text-sm text-warning-800"><strong>Review a new run from {data.replay.id}.</strong> It starts {data.replay.orkflowName}@{data.replay.orkflowVersion} with that run’s input. Submitting creates a new run and does not reuse its idempotency key.</section>{/if}
		<div class="grid gap-4 sm:grid-cols-2">
			<div class="flex flex-col gap-1.5"><label for="orkflow" class="text-sm font-medium text-fg">Orkflow</label><select id="orkflow" data-testid="orkflow-select" value={selectedName} onchange={selectName} class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg">{#each data.orkflows as workflow (workflow.name + '@' + workflow.version)}<option value={workflow.name}>{workflow.name} — {workflow.description}</option>{/each}</select></div>
			<div class="flex flex-col gap-1.5"><label for="orkflow-version" class="text-sm font-medium text-fg">Version</label><select id="orkflow-version" data-testid="orkflow-version-select" value={selectedVersion} onchange={selectVersion} disabled={versionsLoading} class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg disabled:opacity-50"><option value={CURRENT}>Current ({currentVersionLabel()}) — resolves when submitted</option>{#each versions as version (version.version)}<option value={version.version}>{version.version}{version.isCurrent ? ' — current' : ''}{version.description ? ` — ${version.description}` : ''}</option>{/each}</select><p data-testid="submit-version-mode" class="text-xs text-fg-muted">{chosenVersion ? `Pinned explicitly to ${chosenVersion}.` : `Current pointer (${currentVersionLabel()}) will be resolved by the engine.`}</p></div>
		</div>
		{#if selectionError}<ErrorPanel testid="submit-selection-error" message={selectionError} />{/if}
		{#if selectedOrkflow}
			<div class="flex items-center justify-between gap-3 border-b border-border pb-2"><h2 class="text-sm font-medium text-fg">Input</h2><div class="flex gap-2 text-xs"><button type="button" data-testid="input-form-mode" onclick={() => setInputMode('form')} class:font-semibold={inputMode === 'form'}>Fields</button><button type="button" data-testid="input-json-mode" onclick={() => setInputMode('json')} class:font-semibold={inputMode === 'json'}>JSON</button></div></div>
			{#if inputMode === 'form'}<DeclaredInputs inputs={selectedOrkflow.inputs} value={inputsValue} onchange={setInputs} oninvalid={(message) => (typedInputError = message)} resetKey={`${selectedOrkflow.name}@${selectedOrkflow.version}:${inputMode}`} />{#if typedInputError}<p data-testid="declared-input-error" class="text-sm text-danger-700">{typedInputError}</p>{/if}{#if specs.length === 0}<p class="text-sm text-fg-muted">This orkflow declares no inputs. Use JSON to send an explicit object.</p>{/if}{:else}<div class="flex flex-col gap-1.5"><label for="inputs" class="text-sm font-medium text-fg">Inputs (JSON object)</label><textarea id="inputs" data-testid="inputs-json" bind:value={inputsJson} rows="8" spellcheck="false" class="min-h-[160px] rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg"></textarea><p class="text-xs text-fg-muted">JSON is lossless: it retains values and keys beyond the declared form fields.</p></div>{/if}
		{/if}
		<details class="rounded-md border border-border bg-surface p-3"><summary class="cursor-pointer text-sm font-medium text-fg">Advanced submission options</summary><div class="mt-3 space-y-3"><div class="flex flex-col gap-1.5"><label for="idempotency" class="text-sm text-fg">Idempotency key <span class="text-fg-muted">(optional)</span></label><input id="idempotency" data-testid="idempotency-key" bind:value={idempotencyKey} class="h-9 rounded-md border border-border bg-surface px-3 text-sm text-fg" /><p class="text-xs text-fg-muted">If the result is uncertain, retry with this same key. It is retained after an error.</p></div><label class="flex items-start gap-2 text-sm text-fg"><input data-testid="allow-empty-pool" type="checkbox" bind:checked={allowEmptyPool} /> Submit even if a resolved pool currently has no live Orker.</label></div></details>
		<details class="rounded-md border border-border bg-surface p-3"><summary class="cursor-pointer text-sm font-medium text-fg">CLI equivalent</summary><p class="mt-2 text-xs text-fg-muted">Writes the exact input JSON to a file, then submits it with the same namespace, version, and advanced options.</p>{#if cli}<pre data-testid="submit-cli" class="mt-2 overflow-x-auto rounded bg-surface-sunken p-3 text-xs text-fg"><code>{cli}</code></pre><button type="button" data-testid="copy-submit-cli" onclick={copyCli} class="mt-2 text-sm font-medium text-brand-700">{copied ? 'Copied' : 'Copy command'}</button>{:else}<p class="mt-2 text-sm text-warning-700">Fix the input JSON before exporting a CLI command.</p>{/if}</details>
		{#if formError}<ErrorPanel testid="submit-error" message={formError} />{/if}
		<button type="submit" data-testid="submit-run" disabled={submitting || versionsLoading || !!selectionError || !!typedInputError || !selectedOrkflow} class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white disabled:opacity-50">{submitting ? 'Submitting…' : 'Submit run'}</button>
	</form>
{/if}
