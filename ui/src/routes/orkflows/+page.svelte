<script lang="ts">
	import { invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { fmtTimestamp } from '$lib/status';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
	function pageHref(cursor: string, history: string[]) {
		const params = new URLSearchParams({ ns: namespaceLabel(data.namespace), size: String(data.size) });
		if (cursor) params.set('cursor', cursor);
		if (history.length) params.set('history', JSON.stringify(history));
		return `/orkflows?${params.toString()}`;
	}

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

<div class="mb-6 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
	<h1 class="text-xl font-semibold tracking-tight text-fg">Orkflows</h1>
	<a
		href={withNamespace('/apply', data.namespace)}
		class="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
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
	<p class="text-sm text-fg-muted" data-testid="orkflows-loading">Loading orkflows…</p>
{:else if data.orkflows.length === 0}
	<p
		class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
		data-testid="orkflows-empty"
	>
		No orkflows in namespace {namespaceLabel(data.namespace)} yet. Apply one from a YAML document
		to get started.
	</p>
{:else}
	<div class="mb-4 flex gap-2 text-sm"><span class="text-fg-muted">Page size</span>{#each [10, 25, 50, 100] as size}<a href={pageHref('', []) + `&size=${size}`} class:font-semibold={data.size === size}>{size}</a>{/each}</div>
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[640px] border-collapse text-left text-sm" data-testid="orkflow-list">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Name</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Version</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Description</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Tasks</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Applied</th>
				</tr>
			</thead>
			<tbody>
				{#each data.orkflows as wf (wf.name + '@' + wf.version)}
					<tr
						class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
						data-testid="orkflow-row-{wf.name}"
					>
						<td class="h-11 px-4 pl-5 align-middle font-medium">
							<a
								href={withNamespace(`/orkflows/${wf.name}`, data.namespace)}
								class="text-fg hover:text-brand-700">{wf.name}</a
							>
						</td>
						<td class="h-11 px-4 align-middle text-fg-secondary">{wf.version}</td>
						<td class="h-11 max-w-md truncate px-4 align-middle text-fg-secondary"
							>{wf.description}</td
						>
						<td class="h-11 px-4 align-middle text-fg-secondary tabular-nums" data-testid="orkflow-taskcount-{wf.name}"
							>{wf.tasks.length}</td
						>
						<td class="h-11 px-4 pr-5 align-middle text-fg-muted tabular-nums"
							>{fmtTimestamp(wf.createdAt)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<div class="mt-4 flex items-center gap-3" data-testid="orkflow-pagination"><a data-testid="orkflows-previous" aria-disabled={data.history.length === 0} href={data.history.length ? pageHref(data.history.at(-1) ?? '', data.history.slice(0, -1)) : undefined}>Previous</a><span class="text-sm text-fg-muted">{data.cursor ? 'Later page' : 'First page'}</span><a data-testid="orkflows-next" aria-disabled={!data.nextPageToken} href={data.nextPageToken ? pageHref(data.nextPageToken, [...data.history, data.cursor]) : undefined}>Next</a></div>
{/if}
