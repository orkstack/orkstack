<script lang="ts">
	import { goto } from '$app/navigation';
	import { guardUnsaved } from '$lib/dirtyGuard';
	import { exampleHref } from '$lib/examples';
	import { errorMessage } from '$lib/client';
	import { orkflowClient } from '$lib/clients/orkflows';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import YamlField from '$lib/yaml/YamlField.svelte';
	import YamlProblems from '$lib/yaml/YamlProblems.svelte';
	import { YamlSession } from '$lib/yaml/session.svelte';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { ValidationSeverity } from '$lib/gen/orkstack/v1/orkflow_service_pb';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { API_VERSION, ORKFLOW_PLACEHOLDER } from '$lib/placeholders';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	let yaml = $state('');
	let applying = $state(false);
	let validating = $state(false);
	let formError = $state('');
	let warnings = $state<string[]>([]);
	let applied = $state<{ name: string; version: string } | null>(null);
	let cleanYaml = $state('');
	let permitNavigation = $state(false);
	const dirty = $derived(yaml !== cleanYaml);

	guardUnsaved(() => dirty && !permitNavigation, 'Discard unsaved Orkflow YAML?');

	// The document's live check: ValidateOrkflow, the same preparation apply
	// runs, asked once typing pauses (src/lib/yaml/session.svelte.ts). Its
	// answer is the only thing that ever says the document has no problems.
	const session = new YamlSession(async (text, namespace, signal) => {
		const res = await orkflowClient(fetch).validateOrkflow({ yaml: text, namespace }, { signal });
		return res.issues;
	}, 'Apply also refuses a version already stored with other content.');

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
		if (!yaml.trim()) { formError = 'Paste an Orkflow YAML document first.'; return false; }
		const validatedYaml = yaml;
		const validatedNamespace = data.namespace;
		validating = true;
		try {
			const issues = await session.validateNow(validatedYaml, validatedNamespace);
			if (issues === null || yaml !== validatedYaml || data.namespace !== validatedNamespace) return false;
			return !issues.some((issue) => issue.severity === ValidationSeverity.ERROR);
		} catch (err) {
			formError = errorMessage(err);
			return false;
		} finally { validating = false; }
	}

	async function apply() {
		formError = '';
		warnings = [];
		applied = null;
		if (!yaml.trim()) { formError = 'Paste an Orkflow YAML document first.'; return; }
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
			throw new Error('engine returned no Orkflow');
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
	setTitleBlock(() => [
		{ label: 'PAGE', value: 'Apply' },
		{ label: 'NAMESPACE', value: namespaceLabel(data.namespace) },
		{ label: 'DRAFT', value: applied ? `applied ${applied.name}@${applied.version}` : dirty ? 'unsaved' : 'empty' }
	]);

	function exportYaml() {
		const link = document.createElement('a');
		link.href = URL.createObjectURL(new Blob([yaml], { type: 'application/yaml' }));
		link.download = 'orkflow.yaml';
		link.click();
		URL.revokeObjectURL(link.href);
	}
</script>

<svelte:head><title>Apply Orkflow — OrkStack</title></svelte:head>

<SheetHeader title="Apply an Orkflow">
	{#snippet meta()}
		<p data-testid="apply-namespace">
			Into namespace <span class="font-mono text-ink">{namespaceLabel(data.namespace)}</span>. A document
			that names a different one is refused rather than moved.
		</p>
	{/snippet}
	{#snippet actions()}
		<button
			type="button"
			data-testid="apply-validate"
			disabled={validating || applying}
			onclick={() => void validate()}
			class="btn">{validating ? 'Validating…' : 'Validate'}</button
		>
		<button type="submit" form="apply-form" data-testid="apply-submit" disabled={applying} class="btn btn-primary">
			{applying ? 'Applying…' : 'Apply Orkflow'}
		</button>
	{/snippet}
</SheetHeader>

<form
	id="apply-form"
	class="space-y-4"
	data-testid="apply-form"
	onsubmit={(e) => {
		e.preventDefault();
		void apply();
	}}
>
	<div class="flex flex-col gap-1.5">
		<span id="apply-yaml-label" class="text-ink-muted"><span class="caps">Orkflow YAML</span> <span class="font-mono text-xs">(apiVersion: {API_VERSION})</span></span>
		<YamlField
			bind:value={yaml}
			kind="Orkflow"
			namespace={data.namespace}
			{session}
			testid="apply-yaml"
			label="Orkflow YAML"
			labelledby="apply-yaml-label"
			placeholder={ORKFLOW_PLACEHOLDER}
			rows={18}
		/>
		<div class="flex flex-wrap items-center gap-4 text-sm">
			<label class="link cursor-pointer font-semibold"
				>Import YAML<input
					data-testid="apply-import"
					type="file"
					accept=".yaml,.yml,text/yaml,application/yaml"
					class="sr-only"
					onchange={importYaml}
				/></label
			>
			<button type="button" data-testid="apply-export" onclick={exportYaml} class="link font-semibold">Export YAML</button>
			{#if dirty}<span data-testid="apply-unsaved" class="caps text-ink-secondary">Unsaved changes</span>{/if}
		</div>
	</div>

	{#if formError}
		<ErrorPanel testid="apply-error" message={formError} />
	{/if}
	{#each warnings as warning, i (i)}
		<div data-testid="apply-warning" class="severity-warning border border-red px-4 py-2 text-sm">
			{warning}
		</div>
	{/each}
	<YamlProblems {session} listTestid="apply-validation-issues" issueTestid="apply-validation-issue" testid="apply-yaml" />
	{#if applied}
		<p data-testid="apply-success" class="confirm border border-rule-strong px-4 py-2 text-sm text-ink">
			Applied {applied.name}@{applied.version}.
			<a class="link font-semibold" href={withNamespace(`/orkflows/${applied.name}`, data.namespace)}>Open Orkflow</a>
		</p>
	{/if}
</form>

<!--
	The gallery is the repo's `examples/` directory, read at build time into
	/apply/examples.json (the cards) and one /apply/examples/[file] per
	document; an example's YAML is fetched only when it is chosen. "Use this example" is then the same plain
	assignment `importYaml` makes, so it flows through the existing dirty
	check: the change is flagged as unsaved and the guardUnsaved guard still
	protects it, but loading an example is not itself a navigation and does
	not prompt.
-->
<section class="region mt-10 pt-3" aria-labelledby="apply-examples-heading">
	<h2 id="apply-examples-heading" class="mb-3 text-md font-semibold text-ink">Start from an example</h2>
	<div class="border-t-2 border-ink" data-testid="apply-examples" role="list">
		{#each data.examples as example (example.slug)}
			<div
				data-testid="apply-example-{example.slug}"
				role="listitem"
				class="grid grid-cols-1 items-baseline gap-x-6 gap-y-1 border-b border-rule px-3 py-2.5 transition-colors hover:bg-hover sm:grid-cols-[minmax(10rem,14rem)_1fr_auto]"
			>
				<h3 class="font-semibold text-ink">{example.name}</h3>
				<p class="text-sm text-ink-secondary">{example.description}</p>
				<button
					type="button"
					data-testid="apply-example-use-{example.slug}"
					disabled={loadingExample !== ''}
					onclick={() => void useExample(example.slug)}
					class="btn btn-sm justify-self-start sm:justify-self-end"
				>
					{loadingExample === example.slug ? 'Loading…' : 'Use this example'}
				</button>
			</div>
		{/each}
	</div>
	{#if data.examplesError}
		<div class="mt-3"><ErrorPanel testid="apply-examples-error" message={data.examplesError} /></div>
	{/if}
	{#if exampleError}
		<div class="mt-3"><ErrorPanel testid="apply-example-error" message={exampleError} /></div>
	{/if}
</section>
