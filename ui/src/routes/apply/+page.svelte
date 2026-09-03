<script lang="ts">
	import { goto } from '$app/navigation';
	import { errorMessage, orkstack } from '$lib/client';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	// The placeholder must be a document that actually APPLIES: `uses:`, the
	// only image-selecting field in the DSL, selects a first-party activity
	// image. It mirrors examples/http-post.ork.yaml.
	const PLACEHOLDER = `name: my-orkflow
version: "0.1.0"
description: Describe what this orkflow does.

inputs:
  url:
    type: string
    default: "https://httpbin.org/get"

tasks:
  - id: fetch
    uses: http
    with:
      op: get
      url: "{{ .inputs.url }}"
      captureBodyAs: response
`;

	let { data }: PageProps = $props();

	let yaml = $state('');
	let applying = $state(false);
	let formError = $state('');
	let warnings = $state<string[]>([]);

	async function apply() {
		formError = '';
		warnings = [];
		if (!yaml.trim()) {
			formError = 'Paste an orkflow YAML document first.';
			return;
		}
		applying = true;
		try {
			const res = await orkstack(fetch).orkflows.applyOrkflow({ yaml, namespace: data.namespace });
			warnings = res.warnings;
			if (res.orkflow) {
				await goto(withNamespace(`/orkflows/${res.orkflow.name}`, data.namespace));
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

<h1 class="mb-2 text-xl font-semibold tracking-tight text-fg">Apply an orkflow</h1>
<p class="mb-6 text-sm text-fg-muted" data-testid="apply-namespace">
	Into namespace <span class="font-medium text-fg">{namespaceLabel(data.namespace)}</span>.
	A document that names a different one is refused rather than moved.
</p>

<form
	class="max-w-3xl space-y-4"
	data-testid="apply-form"
	onsubmit={(e) => {
		e.preventDefault();
		void apply();
	}}
>
	<div class="flex flex-col gap-1.5">
		<label for="yaml" class="text-sm font-medium text-fg"> Orkflow YAML (DSL v0) </label>
		<textarea
			id="yaml"
			data-testid="apply-yaml"
			bind:value={yaml}
			rows="18"
			spellcheck="false"
			placeholder={PLACEHOLDER}
			class="min-h-[360px] w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs [font-variant-ligatures:none] placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
		></textarea>
	</div>

	{#if formError}
		<ErrorPanel testid="apply-error" message={formError} />
	{/if}
	{#each warnings as warning, i (i)}
		<div class="rounded-md border border-warning-200 bg-warning-50 px-4 py-2 text-sm text-warning-700">
			{warning}
		</div>
	{/each}

	<button
		type="submit"
		data-testid="apply-submit"
		disabled={applying}
		class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
	>
		{applying ? 'Applying…' : 'Apply orkflow'}
	</button>
</form>
