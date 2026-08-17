<script lang="ts">
	import { goto } from '$app/navigation';
	import { errorMessage, orkstack } from '$lib/client';

	const PLACEHOLDER = `name: my-orkflow
version: "0.1.0"
description: Describe what this orkflow does.

inputs:
  url:
    type: string
    default: "https://example.com/data.json"

tasks:
  - id: fetch
    plugin: http
    with:
      method: GET
      url: "{{ .inputs.url }}"
      capture_body_as: body
`;

	let yaml = $state('');
	let applying = $state(false);
	let formError = $state('');
	let warnings = $state<string[]>([]);

	async function apply() {
		formError = '';
		warnings = [];
		if (!yaml.trim()) {
			formError = 'Paste a orkflow YAML document first.';
			return;
		}
		applying = true;
		try {
			const res = await orkstack(fetch).orkflows.applyOrkflow({ yaml });
			warnings = res.warnings;
			if (res.orkflow) {
				await goto(`/orkflows/${res.orkflow.name}`);
				return;
			}
			throw new Error('engine returned no orkflow');
		} catch (err) {
			formError = errorMessage(err);
			applying = false;
		}
	}
</script>

<svelte:head><title>Apply orkflow — OrkStack</title></svelte:head>

<h1 class="mb-6 text-2xl font-bold">Apply a orkflow</h1>

<form
	class="max-w-3xl space-y-4"
	data-testid="apply-form"
	onsubmit={(e) => {
		e.preventDefault();
		void apply();
	}}
>
	<div>
		<label for="yaml" class="mb-1 block text-sm font-medium text-zinc-300">
			Orkflow YAML (DSL v0)
		</label>
		<textarea
			id="yaml"
			data-testid="apply-yaml"
			bind:value={yaml}
			rows="18"
			spellcheck="false"
			placeholder={PLACEHOLDER}
			class="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 font-mono text-sm text-zinc-100 placeholder:text-zinc-600"
		></textarea>
	</div>

	{#if formError}
		<div
			data-testid="apply-error"
			class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
		>
			{formError}
		</div>
	{/if}
	{#each warnings as warning, i (i)}
		<div
			class="rounded-md border border-amber-500/40 bg-amber-500/10 px-4 py-2 text-sm text-amber-300"
		>
			{warning}
		</div>
	{/each}

	<button
		type="submit"
		data-testid="apply-submit"
		disabled={applying}
		class="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
	>
		{applying ? 'Applying…' : 'Apply orkflow'}
	</button>
</form>
