<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Select from '$lib/components/Select.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { fmtTimestamp } from '$lib/status';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const statusOptions = [
		{ value: '', label: 'All statuses' },
		{ value: 'pending', label: 'Pending' },
		{ value: 'running', label: 'Running' },
		{ value: 'succeeded', label: 'Succeeded' },
		{ value: 'failed', label: 'Failed' },
		{ value: 'canceled', label: 'Canceled' },
		{ value: 'skipped', label: 'Skipped' }
	];
	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// The filter form is a DRAFT of the URL, not a second source of truth: the
	// URL still carries the filter (href/filter/nextHref below are unchanged),
	// and this only holds what has been typed but not applied yet.
	//
	// It exists because status and page size are no longer native controls —
	// Select is a button and a listbox, which FormData cannot see — and a form
	// split between FormData and component state would be two half-mechanisms
	// to keep in step. One draft, read by filter(), beats a hidden input
	// mirroring a component's value.
	function draftFromUrl() {
		return {
			orkflow: data.orkflowName,
			status: data.status,
			size: String(data.size),
			since: data.createdAfter,
			until: data.createdBefore
		};
	}
	let draft = $state(draftFromUrl());
	// Any navigation that moves the filter coordinates — Clear, Next, Previous,
	// a pasted link — refills the fields from what the page actually loaded, so
	// a stale draft never sits above results it does not describe.
	$effect(() => {
		draft = draftFromUrl();
	});

	// The standalone Submit Run page is gone: a run is started from the Orkflow
	// it belongs to. A list
	// filtered to one orkflow knows which page that is; an unfiltered one can
	// only offer the picker.
	const submitHref = $derived(
		withNamespace(
			data.orkflowName ? `/orkflows/${encodeURIComponent(data.orkflowName)}` : '/orkflows',
			data.namespace
		)
	);

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
		void goto(href({ orkflow: draft.orkflow, status: draft.status, size: draft.size || '25', since: draft.since, until: draft.until, cursor: '', history: '' }));
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
		href={submitHref}
		data-testid="runs-submit-run"
		class="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
	>
		{data.orkflowName ? 'Submit a run' : 'Browse orkflows'}
	</a>
</div>

<form
	class="mb-5 rounded-lg border border-border bg-surface p-4 shadow-xs"
	data-testid="run-filters"
	onsubmit={filter}
>
	<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Orkflow</span>
			<input
				name="orkflow"
				data-testid="run-filter-orkflow"
				bind:value={draft.orkflow}
				placeholder="All orkflows"
				class="h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			/>
		</label>
		<div class="flex flex-col gap-1.5 text-sm">
			<label class="font-medium text-fg" for="run-filter-status">Status</label>
			<Select
				id="run-filter-status"
				testid="run-filter-status"
				value={draft.status}
				options={statusOptions}
				onchange={(next) => (draft.status = next)}
			/>
		</div>
		<div class="flex flex-col gap-1.5 text-sm">
			<label class="font-medium text-fg" for="run-filter-size">Page size</label>
			<Select
				id="run-filter-size"
				testid="run-filter-size"
				value={draft.size}
				options={sizeOptions}
				onchange={(next) => (draft.size = next)}
			/>
		</div>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg"
				>Created since <span class="font-normal text-fg-muted">(inclusive)</span></span
			>
			<input
				name="since"
				type="datetime-local"
				data-testid="run-filter-since"
				bind:value={draft.since}
				class="h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			/>
		</label>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg"
				>Created until <span class="font-normal text-fg-muted">(exclusive)</span></span
			>
			<input
				name="until"
				type="datetime-local"
				data-testid="run-filter-until"
				bind:value={draft.until}
				class="h-9 w-full rounded-md border border-border bg-surface px-3 text-sm text-fg shadow-xs focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none"
			/>
		</label>
	</div>
	<div class="mt-4 flex items-center gap-4 border-t border-border-subtle pt-4">
		<button
			type="submit"
			data-testid="run-filters-apply"
			class="inline-flex h-9 items-center justify-center rounded-md bg-brand-600 px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 hover:bg-brand-700 active:bg-brand-800"
			>Apply filters</button
		>
		<a
			href={href({ orkflow: '', status: '', size: '25', since: '', until: '', cursor: '', history: '' })}
			class="text-sm font-medium text-brand-700 hover:underline">Clear</a
		>
	</div>
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
