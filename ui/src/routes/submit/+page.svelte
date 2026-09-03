<script lang="ts">
	import { untrack } from 'svelte';
	import { goto, invalidateAll } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import type { JsonObject } from '@bufbuild/protobuf';
	import { errorMessage, orkstack } from '$lib/client';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { PayloadSchema } from '$lib/gen/orkstack/v1/common_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
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

	// One-shot seed of the picker from the loaded page data.
	let selected = $state(untrack(() => data.preselect || data.orkflows[0]?.name || ''));
	let inputsJson = $state('');
	let submitting = $state(false);
	let formError = $state('');

	const orkflow = $derived(data.orkflows.find((w) => w.name === selected));
	const declaredInputNames = $derived(Object.keys(orkflow?.inputs ?? {}));

	function defaultsFor(name: string): JsonObject {
		const wf = data.orkflows.find((w) => w.name === name);
		const out: JsonObject = {};
		const inputs = (wf?.inputs ?? {}) as JsonObject;
		for (const [key, spec] of Object.entries(inputs)) {
			if (spec && typeof spec === 'object' && !Array.isArray(spec)) {
				const def = (spec as JsonObject)['default'];
				if (def !== undefined) out[key] = def;
			}
		}
		return out;
	}

	// (Re)seed the inputs editor whenever the chosen orkflow changes.
	$effect(() => {
		inputsJson = JSON.stringify(defaultsFor(selected), null, 2);
	});

	async function submit() {
		formError = '';
		if (!orkflow) {
			formError = 'Choose an orkflow first.';
			return;
		}
		let parsed: JsonObject;
		try {
			const value: unknown = JSON.parse(inputsJson || '{}');
			if (value === null || typeof value !== 'object' || Array.isArray(value)) {
				formError = 'Inputs must be a JSON object, e.g. {"url": "https://…"}';
				return;
			}
			parsed = value as JsonObject;
		} catch (err) {
			formError = `Inputs are not valid JSON: ${errorMessage(err)}`;
			return;
		}
		submitting = true;
		try {
			const res = await orkstack(fetch).runs.submitRun({
				orkflowName: orkflow.name,
				orkflowVersion: orkflow.version,
				namespace: data.namespace,
				input: create(PayloadSchema, { data: parsed })
			});
			if (!res.run) throw new Error('engine returned no run');
			await goto(withNamespace(`/runs/${res.run.id}`, data.namespace));
		} catch (err) {
			formError = errorMessage(err);
			submitting = false;
		}
	}
</script>

<svelte:head><title>Submit run — OrkStack</title></svelte:head>

<h1 class="mb-6 text-xl font-semibold tracking-tight text-fg">Submit a run</h1>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="submit-retry"
		subject="the orkflows in namespace {namespaceLabel(data.namespace)}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-fg-muted" data-testid="submit-loading">Loading orkflows…</p>
{:else if data.orkflows.length === 0}
	<p class="text-sm text-fg-muted" data-testid="submit-empty">
		No orkflows in namespace {namespaceLabel(data.namespace)} yet.
		<a href={withNamespace('/apply', data.namespace)} class="font-medium text-brand-700 hover:underline"
			>Apply one first.</a
		>
	</p>
{:else}
	<form
		class="max-w-3xl space-y-5"
		data-testid="submit-form"
		onsubmit={(e) => {
			e.preventDefault();
			void submit();
		}}
	>
		<div class="flex flex-col gap-1.5">
			<label for="orkflow" class="text-sm font-medium text-fg">Orkflow</label>
			<select
				id="orkflow"
				data-testid="orkflow-select"
				bind:value={selected}
				class="h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs transition-colors duration-150 hover:border-border-strong focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			>
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<option value={wf.name}>{wf.name}@{wf.version} — {wf.description}</option>
				{/each}
			</select>
		</div>

		<div class="flex flex-col gap-1.5">
			<label for="inputs" class="text-sm font-medium text-fg"> Inputs (JSON object) </label>
			<textarea
				id="inputs"
				data-testid="inputs-json"
				bind:value={inputsJson}
				rows="6"
				spellcheck="false"
				class="min-h-[120px] w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			></textarea>
			{#if orkflow && declaredInputNames.length > 0}
				<p class="text-xs text-fg-muted">
					Declared inputs: {declaredInputNames.join(', ')}
				</p>
			{/if}
		</div>

		{#if formError}
			<ErrorPanel testid="submit-error" message={formError} />
		{/if}

		<button
			type="submit"
			data-testid="submit-run"
			disabled={submitting}
			class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
		>
			{submitting ? 'Submitting…' : 'Submit run'}
		</button>
	</form>
{/if}
