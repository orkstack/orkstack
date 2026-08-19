<script lang="ts">
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { fmtTimestamp } from '$lib/status';
	import { withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

<svelte:head><title>Runs — OrkStack</title></svelte:head>

<div class="mb-6 flex items-center justify-between">
	<h1 class="text-2xl font-bold">
		Runs{#if data.orkflowName}
			<span class="ml-2 text-base font-normal text-zinc-400">for {data.orkflowName}</span>
		{/if}
	</h1>
	<a
		href={withNamespace('/submit', data.namespace)}
		class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500"
	>
		Submit run
	</a>
</div>

{#if data.error}
	<div
		data-testid="engine-error"
		class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
	>
		Could not reach the engine: {data.error}
	</div>
{:else if data.runs.length === 0}
	<p class="text-sm text-zinc-400" data-testid="runs-empty">No runs yet.</p>
{:else}
	<div class="overflow-hidden rounded-lg border border-zinc-800">
		<table class="w-full text-left text-sm" data-testid="run-list">
			<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
				<tr>
					<th class="px-4 py-3">Run ID</th>
					<th class="px-4 py-3">Orkflow</th>
					<th class="px-4 py-3">Status</th>
					<th class="px-4 py-3">Created</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-zinc-800">
				{#each data.runs as run (run.id)}
					<tr class="hover:bg-zinc-900/60" data-testid="run-row-{run.id}">
						<td class="px-4 py-3 font-medium">
							<a href={withNamespace(`/runs/${run.id}`, data.namespace)} class="text-emerald-300 hover:underline">{run.id}</a>
						</td>
						<td class="px-4 py-3 text-zinc-300">{run.orkflowName}@{run.orkflowVersion}</td>
						<td class="px-4 py-3"><StatusPill status={run.status} /></td>
						<td class="px-4 py-3 text-zinc-400">{fmtTimestamp(run.createdAt)}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
