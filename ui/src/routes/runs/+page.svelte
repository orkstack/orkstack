<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
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
	function href(changes: Record<string, string>) {
		const params = new URLSearchParams();
		params.set('ns', namespaceLabel(data.namespace));
		for (const [key, value] of Object.entries({ orkflow: data.orkflowName, status: data.status, size: String(data.size), since: data.createdAfter, until: data.createdBefore, ...changes })) if (value) params.set(key, value);
		return `/runs?${params.toString()}`;
	}
	function filter(event: SubmitEvent) {
		event.preventDefault();
		const form = new FormData(event.currentTarget as HTMLFormElement);
		void goto(href({ orkflow: String(form.get('orkflow') ?? ''), status: String(form.get('status') ?? ''), size: String(form.get('size') ?? '25'), since: String(form.get('since') ?? ''), until: String(form.get('until') ?? ''), cursor: '', history: '' }));
	}
	function nextHref() { return href({ cursor: data.nextPageToken, history: JSON.stringify([...data.history, data.cursor]) }); }
	function previousHref() { const previous = data.history.at(-1) ?? ''; return href({ cursor: previous, history: JSON.stringify(data.history.slice(0, -1)) }); }
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

<form class="mb-5 grid gap-3 rounded-lg border border-border bg-surface p-4 sm:grid-cols-3" data-testid="run-filters" onsubmit={filter}>
	<label class="text-sm text-fg-secondary">Orkflow<input name="orkflow" value={data.orkflowName} class="mt-1 h-9 w-full rounded border border-border bg-surface px-2 text-fg" /></label>
	<label class="text-sm text-fg-secondary">Status<select name="status" value={data.status} class="mt-1 h-9 w-full rounded border border-border bg-surface px-2 text-fg"><option value="">All statuses</option><option value="pending">Pending</option><option value="running">Running</option><option value="succeeded">Succeeded</option><option value="failed">Failed</option><option value="canceled">Canceled</option><option value="skipped">Skipped</option></select></label>
	<label class="text-sm text-fg-secondary">Page size<select name="size" value={String(data.size)} class="mt-1 h-9 w-full rounded border border-border bg-surface px-2 text-fg"><option value="10">10</option><option value="25">25</option><option value="50">50</option><option value="100">100</option></select></label>
	<label class="text-sm text-fg-secondary">Created since (inclusive)<input name="since" type="datetime-local" value={data.createdAfter} class="mt-1 h-9 w-full rounded border border-border bg-surface px-2 text-fg" /></label>
	<label class="text-sm text-fg-secondary">Created until (exclusive)<input name="until" type="datetime-local" value={data.createdBefore} class="mt-1 h-9 w-full rounded border border-border bg-surface px-2 text-fg" /></label>
	<div class="flex items-end gap-3"><button data-testid="run-filters-apply" class="h-9 rounded bg-brand-600 px-3 text-sm font-medium text-white">Apply filters</button><a href={href({ orkflow: '', status: '', size: '25', since: '', until: '', cursor: '', history: '' })} class="pb-2 text-sm text-brand-700">Clear</a></div>
</form>

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
		No runs match these filters in namespace {namespaceLabel(data.namespace)}.
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
	<div class="mt-4 flex items-center gap-3" data-testid="run-pagination">
		<a data-testid="runs-previous" aria-disabled={data.history.length === 0} class:opacity-50={data.history.length === 0} href={data.history.length ? previousHref() : undefined}>Previous</a>
		<span class="text-sm text-fg-muted">{data.cursor ? 'Later page' : 'First page'}</span>
		<a data-testid="runs-next" aria-disabled={!data.nextPageToken} class:opacity-50={!data.nextPageToken} href={data.nextPageToken ? nextHref() : undefined}>Next</a>
	</div>
{/if}
