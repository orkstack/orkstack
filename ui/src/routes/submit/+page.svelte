<script lang="ts">
	import { untrack } from 'svelte';
	import { goto } from '$app/navigation';
	import { create } from '@bufbuild/protobuf';
	import type { JsonObject } from '@bufbuild/protobuf';
	import { errorMessage, orkstack } from '$lib/client';
	import { PayloadSchema } from '$lib/gen/orkstack/v1/common_pb';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

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
				input: create(PayloadSchema, { data: parsed })
			});
			if (!res.run) throw new Error('engine returned no run');
			await goto(`/runs/${res.run.id}`);
		} catch (err) {
			formError = errorMessage(err);
			submitting = false;
		}
	}
</script>

<svelte:head><title>Submit run — OrkStack</title></svelte:head>

<h1 class="mb-6 text-2xl font-bold">Submit a run</h1>

{#if data.error}
	<div
		data-testid="engine-error"
		class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
	>
		Could not reach the engine: {data.error}
	</div>
{:else if data.orkflows.length === 0}
	<p class="text-sm text-zinc-400">
		No orkflows applied yet. <a href="/apply" class="text-emerald-300 hover:underline"
			>Apply one first.</a
		>
	</p>
{:else}
	<form
		class="max-w-xl space-y-5"
		data-testid="submit-form"
		onsubmit={(e) => {
			e.preventDefault();
			void submit();
		}}
	>
		<div>
			<label for="orkflow" class="mb-1 block text-sm font-medium text-zinc-300">Orkflow</label>
			<select
				id="orkflow"
				data-testid="orkflow-select"
				bind:value={selected}
				class="w-full rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100"
			>
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<option value={wf.name}>{wf.name}@{wf.version} — {wf.description}</option>
				{/each}
			</select>
		</div>

		<div>
			<label for="inputs" class="mb-1 block text-sm font-medium text-zinc-300">
				Inputs (JSON object)
			</label>
			<textarea
				id="inputs"
				data-testid="inputs-json"
				bind:value={inputsJson}
				rows="6"
				spellcheck="false"
				class="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-zinc-100"
			></textarea>
			{#if orkflow && declaredInputNames.length > 0}
				<p class="mt-1 text-xs text-zinc-500">
					Declared inputs: {declaredInputNames.join(', ')}
				</p>
			{/if}
		</div>

		{#if formError}
			<div
				data-testid="submit-error"
				class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
			>
				{formError}
			</div>
		{/if}

		<button
			type="submit"
			data-testid="submit-run"
			disabled={submitting}
			class="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
		>
			{submitting ? 'Submitting…' : 'Submit run'}
		</button>
	</form>
{/if}
