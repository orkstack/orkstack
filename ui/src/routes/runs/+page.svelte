<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { fmtTimestamp } from '$lib/status';
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
</script>

<svelte:head><title>Runs — OrkStack</title></svelte:head>

<div class="mb-6 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
	<h1 class="text-xl font-semibold tracking-tight text-fg">
		Runs{#if data.orkflowName}
			<span class="ml-2 text-base font-normal text-fg-muted">for {data.orkflowName}</span>
		{/if}
	</h1>
	<a
		href={withNamespace('/submit', data.namespace)}
		class="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
	>
		Submit run
	</a>
</div>

{#if data.error}
	<ErrorPanel
		testid="engine-error"
		retryTestid="runs-retry"
		subject="the runs in namespace {namespaceLabel(data.namespace)}"
		message={data.error}
		busy={retrying}
		onretry={retry}
	/>
{:else if retrying}
	<p class="text-sm text-fg-muted" data-testid="runs-loading">Loading runs…</p>
{:else if data.runs.length === 0}
	<p
		class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
		data-testid="runs-empty"
	>
		No runs in namespace {namespaceLabel(data.namespace)} yet.
	</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[640px] border-collapse text-left text-sm" data-testid="run-list">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Run ID</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Orkflow</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Status</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Created</th>
				</tr>
			</thead>
			<tbody>
				{#each data.runs as run (run.id)}
					<tr
						class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
						data-testid="run-row-{run.id}"
					>
						<td class="h-11 px-4 pl-5 align-middle font-medium">
							<a
								href={withNamespace(`/runs/${run.id}`, data.namespace)}
								class="font-mono text-xs text-fg hover:text-brand-700">{run.id}</a
							>
						</td>
						<td class="h-11 px-4 align-middle text-fg-secondary"
							>{run.orkflowName}@{run.orkflowVersion}</td
						>
						<td class="h-11 px-4 align-middle"><StatusPill status={run.status} /></td>
						<td class="h-11 px-4 pr-5 align-middle text-fg-muted tabular-nums"
							>{fmtTimestamp(run.createdAt)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
