<script lang="ts">
	import DagGraph from '$lib/components/DagGraph.svelte';
	import { fmtTimestamp } from '$lib/status';
	import { withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	const wf = $derived(data.orkflow);
</script>

<svelte:head><title>{data.orkflow?.name ?? 'Orkflow'} — OrkStack</title></svelte:head>

{#if data.error}
	<div
		data-testid="engine-error"
		class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
	>
		{data.error}
	</div>
{:else if wf}
	<div class="mb-6">
		<div class="flex items-baseline gap-3">
			<h1 class="text-2xl font-bold" data-testid="orkflow-name">{wf.name}</h1>
			<span class="text-sm text-zinc-400" data-testid="orkflow-version">v{wf.version}</span>
		</div>
		{#if wf.description}
			<p class="mt-1 text-sm text-zinc-400">{wf.description}</p>
		{/if}
		<p class="mt-1 text-xs text-zinc-500">Applied {fmtTimestamp(wf.createdAt)}</p>
	</div>

	<section class="mb-8">
		<h2 class="mb-3 text-lg font-semibold">Task DAG</h2>
		<DagGraph tasks={wf.tasks} />
	</section>

	<section class="mb-8">
		<h2 class="mb-3 text-lg font-semibold">Tasks ({wf.tasks.length})</h2>
		<div class="overflow-hidden rounded-lg border border-zinc-800">
			<table class="w-full text-left text-sm">
				<thead class="bg-zinc-900 text-xs uppercase tracking-wide text-zinc-400">
					<tr>
						<th class="px-4 py-3">ID</th>
						<th class="px-4 py-3">Plugin</th>
						<th class="px-4 py-3">Needs</th>
						<th class="px-4 py-3">Timeout</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-zinc-800">
					{#each wf.tasks as task (task.id)}
						<tr data-testid="task-row-{task.id}">
							<td class="px-4 py-2.5 font-medium text-zinc-100">{task.id}</td>
							<td class="px-4 py-2.5 text-zinc-300">{task.plugin}</td>
							<td class="px-4 py-2.5 text-zinc-400" data-testid="task-needs-{task.id}">
								{task.needs.length > 0 ? task.needs.join(', ') : '—'}
							</td>
							<td class="px-4 py-2.5 text-zinc-400">{task.timeoutSeconds}s</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</section>

	<div class="flex gap-3">
		<a
			href={withNamespace(`/submit?orkflow=${wf.name}`, data.namespace)}
			class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500"
		>
			Submit a run
		</a>
		<a
			href={withNamespace(`/runs?orkflow=${wf.name}`, data.namespace)}
			class="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800"
		>
			View runs
		</a>
	</div>
{/if}
