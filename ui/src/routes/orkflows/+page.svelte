<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { fmtTimestamp } from '$lib/status';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// A failed list is a failed request, not a permanent state of the world:
	// the retry re-issues it in place instead of leaving the reload button as
	// the only way out.
	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}
</script>

<svelte:head><title>Orkflows — OrkStack</title></svelte:head>

<div class="mb-6 flex items-center justify-between">
	<h1 class="text-2xl font-bold">Orkflows</h1>
	<a
		href={withNamespace('/apply', data.namespace)}
		class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500"
	>
		Apply orkflow
	</a>
</div>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="orkflows-retry"
		subject="the orkflows in namespace {namespaceLabel(data.namespace)}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-zinc-400" data-testid="orkflows-loading">Loading orkflows…</p>
{:else if data.orkflows.length === 0}
	<p class="text-sm text-zinc-400" data-testid="orkflows-empty">
		No orkflows in namespace {namespaceLabel(data.namespace)} yet. Apply one from a YAML document
		to get started.
	</p>
{:else}
	<div class="overflow-hidden rounded-lg border border-zinc-800">
		<table class="w-full text-left text-sm" data-testid="orkflow-list">
			<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
				<tr>
					<th class="px-4 py-3">Name</th>
					<th class="px-4 py-3">Version</th>
					<th class="px-4 py-3">Description</th>
					<th class="px-4 py-3">Tasks</th>
					<th class="px-4 py-3">Applied</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<tr class="hover:bg-zinc-900/60" data-testid="orkflow-row-{wf.name}">
						<td class="px-4 py-3 font-medium">
							<a href={withNamespace(`/orkflows/${wf.name}`, data.namespace)} class="text-emerald-300 hover:underline">{wf.name}</a>
						</td>
						<td class="px-4 py-3 text-zinc-300">{wf.version}</td>
						<td class="max-w-md truncate px-4 py-3 text-zinc-400">{wf.description}</td>
						<td class="px-4 py-3 text-zinc-300" data-testid="orkflow-taskcount-{wf.name}"
							>{wf.tasks.length}</td
						>
						<td class="px-4 py-3 text-zinc-400">{fmtTimestamp(wf.createdAt)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
