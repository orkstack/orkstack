<script lang="ts">
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

<svelte:head><title>Workflows — OrkStack</title></svelte:head>

<div class="mb-6 flex items-center justify-between">
	<h1 class="text-2xl font-bold">Workflows</h1>
	<a
		href="/apply"
		class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500"
	>
		Apply workflow
	</a>
</div>

{#if data.error}
	<div
		data-testid="engine-error"
		class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
	>
		Could not reach the engine: {data.error}
	</div>
{:else if data.workflows.length === 0}
	<p class="text-sm text-zinc-400" data-testid="workflows-empty">
		No workflows applied yet. Apply one from a YAML document to get started.
	</p>
{:else}
	<div class="overflow-hidden rounded-lg border border-zinc-800">
		<table class="w-full text-left text-sm" data-testid="workflow-list">
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
				{#each data.workflows as wf (wf.name + '@' + wf.version)}
					<tr class="hover:bg-zinc-900/60" data-testid="workflow-row-{wf.name}">
						<td class="px-4 py-3 font-medium">
							<a href="/workflows/{wf.name}" class="text-emerald-300 hover:underline">{wf.name}</a>
						</td>
						<td class="px-4 py-3 text-zinc-300">{wf.version}</td>
						<td class="max-w-md truncate px-4 py-3 text-zinc-400">{wf.description}</td>
						<td class="px-4 py-3 text-zinc-300" data-testid="workflow-taskcount-{wf.name}"
							>{wf.tasks.length}</td
						>
						<td class="px-4 py-3 text-zinc-400">{fmtTimestamp(wf.createdAt)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
