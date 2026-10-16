<script lang="ts">
	import { goto } from '$app/navigation';
	import { guardUnsaved } from '$lib/dirtyGuard';
	import { exampleHref } from '$lib/examples';
	import { errorMessage } from '$lib/client';
	import { orkflowClient } from '$lib/clients/orkflows';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { ValidationSeverity, type ValidationIssue } from '$lib/gen/orkstack/v1/orkflow_service_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { API_VERSION, ORKFLOW_PLACEHOLDER } from '$lib/placeholders';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let yaml = $state('');
	let applying = $state(false);
	let validating = $state(false);
	let formError = $state('');
	let warnings = $state<string[]>([]);
	let issues = $state<ValidationIssue[]>([]);
	let applied = $state<{ name: string; version: string } | null>(null);
	let cleanYaml = $state('');
	let permitNavigation = $state(false);
	const dirty = $derived(yaml !== cleanYaml);

	guardUnsaved(() => dirty && !permitNavigation, 'Discard unsaved orkflow YAML?');

	// An example's YAML is fetched when its card is chosen, not shipped with
	// the page ($lib/examples). A second click while one is loading is not
	// offered, so the editor ends up holding the example last asked for.
	let loadingExample = $state('');
	let exampleError = $state('');
	async function useExample(slug: string) {
		loadingExample = slug;
		exampleError = '';
		try {
			const response = await fetch(exampleHref(slug));
			if (!response.ok) throw new Error(`Could not load example ${slug}: HTTP ${response.status} ${(await response.text()).trim()}`);
			yaml = await response.text();
		} catch (err) {
			exampleError = err instanceof Error ? err.message : String(err);
		} finally {
			loadingExample = '';
		}
	}

	async function validate(): Promise<boolean> {
		formError = '';
		issues = [];
		if (!yaml.trim()) { formError = 'Paste an orkflow YAML document first.'; return false; }
		const validatedYaml = yaml;
		const validatedNamespace = data.namespace;
		validating = true;
		try {
			const result = await orkflowClient(fetch).validateOrkflow({ yaml: validatedYaml, namespace: validatedNamespace });
			if (yaml !== validatedYaml || data.namespace !== validatedNamespace) return false;
			issues = result.issues;
			return !result.issues.some((issue) => issue.severity === ValidationSeverity.ERROR);
		} catch (err) {
			formError = errorMessage(err);
			return false;
		} finally { validating = false; }
	}

	async function apply() {
		formError = '';
		warnings = [];
		applied = null;
		if (!yaml.trim()) { formError = 'Paste an orkflow YAML document first.'; return; }
		const submittedYaml = yaml;
		const submittedNamespace = data.namespace;
		applying = true;
		try {
			const res = await orkflowClient(fetch).applyOrkflow({ yaml: submittedYaml, namespace: submittedNamespace });
			warnings = res.warnings;
			if (res.orkflow) {
				if (res.warnings.length > 0) {
					applied = { name: res.orkflow.name, version: res.orkflow.version };
					if (yaml === submittedYaml) cleanYaml = submittedYaml;
					return;
				}
				permitNavigation = yaml === submittedYaml;
				await goto(withNamespace(`/orkflows/${res.orkflow.name}`, submittedNamespace));
				return;
			}
			throw new Error('engine returned no orkflow');
		} catch (err) {
			formError = errorMessage(err);
		} finally { applying = false; }
	}

	async function importYaml(event: Event) {
		const input = event.currentTarget as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		yaml = await file.text();
		input.value = '';
	}
	function exportYaml() {
		const link = document.createElement('a');
		link.href = URL.createObjectURL(new Blob([yaml], { type: 'application/yaml' }));
		link.download = 'orkflow.yaml';
		link.click();
		URL.revokeObjectURL(link.href);
	}
</script>

<svelte:head><title>Apply orkflow — OrkStack</title></svelte:head>

<h1 class="mb-2 text-xl font-semibold tracking-tight text-fg">Apply an orkflow</h1>
<p class="mb-6 text-sm text-fg-muted" data-testid="apply-namespace">
	Into namespace <span class="font-medium text-fg">{namespaceLabel(data.namespace)}</span>.
	A document that names a different one is refused rather than moved.
</p>

<!--
	The gallery is the repo's `examples/` directory, read at build time by
	+page.server.ts; a card's YAML is fetched from /apply/examples/[slug] only
	when it is chosen. "Use this example" is then the same plain assignment `importYaml` makes,
	so it flows through the existing dirty check: the change is flagged as
	unsaved and the guardUnsaved guard still protects it, but loading an
	example is not itself a navigation and does not prompt.
-->
<section class="mb-6 max-w-3xl">
	<h2 class="mb-3 text-sm font-semibold text-fg">Start from an example</h2>
	<div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3" data-testid="apply-examples">
		{#each data.examples as example (example.slug)}
			<div
				data-testid="apply-example-{example.slug}"
				class="flex flex-col rounded-lg border border-border bg-surface p-4 shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
			>
				<h3 class="font-medium text-fg">{example.name}</h3>
				<p class="mt-1 grow text-sm text-fg-muted">{example.description}</p>
				<button
					type="button"
					data-testid="apply-example-use-{example.slug}"
					disabled={loadingExample !== ''}
					onclick={() => void useExample(example.slug)}
					class="mt-3 self-start text-sm font-medium text-brand-700 hover:underline disabled:opacity-50"
				>
					{loadingExample === example.slug ? 'Loading…' : 'Use this example'}
				</button>
			</div>
		{/each}
	</div>
	{#if exampleError}
		<div class="mt-3"><ErrorPanel testid="apply-example-error" message={exampleError} /></div>
	{/if}
</section>

<form
	class="max-w-3xl space-y-4"
	data-testid="apply-form"
	onsubmit={(e) => {
		e.preventDefault();
		void apply();
	}}
>
	<div class="flex flex-col gap-1.5">
		<label for="yaml" class="text-sm font-medium text-fg"> Orkflow YAML (apiVersion: {API_VERSION}) </label>
		<textarea
			id="yaml"
			data-testid="apply-yaml"
			bind:value={yaml}
			rows="18"
			spellcheck="false"
			placeholder={ORKFLOW_PLACEHOLDER}
			class="min-h-[360px] w-full rounded-md border border-border bg-surface px-3 py-2 font-mono text-[13px] leading-5 text-fg shadow-xs [font-variant-ligatures:none] placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
		></textarea>
		<div class="flex flex-wrap gap-3 text-sm">
			<label class="cursor-pointer font-medium text-brand-700">Import YAML<input data-testid="apply-import" type="file" accept=".yaml,.yml,text/yaml,application/yaml" class="sr-only" onchange={importYaml} /></label>
			<button type="button" data-testid="apply-export" onclick={exportYaml} class="font-medium text-brand-700">Export YAML</button>
			{#if dirty}<span data-testid="apply-unsaved" class="text-warning-700">Unsaved changes</span>{/if}
		</div>
	</div>

	{#if formError}
		<ErrorPanel testid="apply-error" message={formError} />
	{/if}
	{#each warnings as warning, i (i)}
		<div data-testid="apply-warning" class="rounded-md border border-warning-200 bg-warning-50 px-4 py-2 text-sm text-warning-700">
			{warning}
		</div>
	{/each}
	{#if issues.length > 0}
		<div data-testid="apply-validation-issues" class="space-y-2 rounded-md border border-border bg-surface-sunken p-3 text-sm">
			<p class="font-medium text-fg">Validation results</p>
			{#each issues as issue, i (`${issue.field}-${i}`)}
				<p data-testid="apply-validation-issue" data-severity={issue.severity === ValidationSeverity.ERROR ? 'error' : 'warning'} data-field={issue.field} class={issue.severity === ValidationSeverity.ERROR ? 'text-danger-700' : 'text-warning-700'}><span class="font-medium">{issue.field || 'document'}</span>: {issue.message}</p>
			{/each}
		</div>
	{/if}
	{#if applied}
		<p data-testid="apply-success" class="rounded-md border border-success-200 bg-success-50 px-4 py-2 text-sm text-success-700">Applied {applied.name}@{applied.version}. <a class="font-medium underline" href={withNamespace(`/orkflows/${applied.name}`, data.namespace)}>Open orkflow</a></p>
	{/if}

	<button type="button" data-testid="apply-validate" disabled={validating || applying} onclick={() => void validate()} class="mr-3 inline-flex h-9 items-center justify-center rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg disabled:opacity-50">{validating ? 'Validating…' : 'Validate'}</button>
	<button
		type="submit"
		data-testid="apply-submit"
		disabled={applying}
		class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas active:bg-brand-800 disabled:pointer-events-none disabled:opacity-50"
	>
		{applying ? 'Applying…' : 'Apply orkflow'}
	</button>
</form>
