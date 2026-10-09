<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { fmtTimestamp } from '$lib/status';
	import { ORIGIN_KIND_SLUGS, UNRECORDED, UNRECORDED_CAPTION, originKindLabel, originKindSlug } from '$lib/origin';
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
	// "Started by" is the origin kind; its slugs are the URL's spelling.
	const originOptions = [{ value: '', label: 'Any' }, ...ORIGIN_KIND_SLUGS.map((slug) => ({ value: slug, label: originKindLabel(slug) }))];

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
			until: data.createdBefore,
			origin: data.origin,
			originId: data.originId
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
	// The coordinates of what this list is showing, exactly as the address bar
	// spells them, and one builder over them. listHref only ever `set`s, so no
	// gesture here can leave two of the same key in the URL.
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), orkflow: data.orkflowName, status: data.status, size: String(data.size), since: data.createdAfter, until: data.createdBefore, origin: data.origin, originId: data.originId, cursor: data.cursor, page: String(data.page) });
	function href(changes: Record<string, string>) { return listHref('/runs', { ...coordinates, ...changes }); }
	function filter(event: SubmitEvent) {
		event.preventDefault();
		// An id means nothing without a kind, so choosing "Any" drops it.
		const originId = draft.origin ? draft.originId.trim() : '';
		void goto(href({ orkflow: draft.orkflow, status: draft.status, size: draft.size || '25', since: draft.since, until: draft.until, origin: draft.origin, originId, cursor: '', page: '' }));
	}

	// The active origin filter in words, for the empty-state sentence.
	const originPhrase = $derived(
		data.origin ? `started by ${data.origin}${data.originId ? ` ${data.originId}` : ''} ` : ''
	);

	// The cursors BEHIND this page: per filter set, per tab, in sessionStorage
	// rather than in the URL, which used to grow by one escaped engine cursor
	// per Next click (docs/UI_CLI_PARITY.md "Paging in the browser").
	//
	// Read in an `$effect` and not a `$derived`: the first render is the
	// server's, where there is no sessionStorage, and a derived computed there
	// would keep its empty answer through hydration.
	const stackKey = $derived(pageStackKey('/runs', coordinates));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	// Next records the path it is about to take before the router follows the
	// href: this page's cursor at this page's index, the token being followed
	// at the next one. Holes are filled with '' so the stored array stays a
	// list of strings.
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, data.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[data.page - 1], ahead[data.page]] = [data.cursor, data.nextPageToken];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	// Previous is offered only when the stack describes THIS page. "There is an
	// earlier page" was never "the stack is non-empty": a pasted page-3 link
	// has an earlier page and no way to name its cursor, and it is handed
	// First page instead of a Previous that would have to invent one.
	const walked = $derived(data.page > 1 && stack[data.page - 1] === data.cursor);
	const previousHref = $derived(walked ? href({ cursor: stack[data.page - 2] ?? '', page: String(data.page - 1) }) : undefined);
	const nextHref = $derived(data.nextPageToken ? href({ cursor: data.nextPageToken, page: String(data.page + 1) }) : undefined);
	const firstHref = $derived(href({ cursor: '', page: '' }));
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
		<div class="flex flex-col gap-1.5 text-sm">
			<label class="font-medium text-fg" for="run-filter-origin">Started by</label>
			<Select
				id="run-filter-origin"
				testid="run-filter-origin"
				value={draft.origin}
				options={originOptions}
				onchange={(next) => (draft.origin = next)}
			/>
		</div>
		<label class="flex flex-col gap-1.5 text-sm">
			<span class="font-medium text-fg">Origin id</span>
			<input
				name="originId"
				data-testid="run-filter-origin-id"
				bind:value={draft.originId}
				disabled={!draft.origin}
				placeholder="trg-…, sch-…, ork-…"
				class="h-9 w-full rounded-md border border-border bg-surface px-3 font-mono text-sm text-fg shadow-xs placeholder:font-sans placeholder:text-fg-placeholder focus:border-brand-500 focus:ring-2 focus:ring-brand-500/25 focus:outline-none disabled:opacity-50"
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
			href={href({ orkflow: '', status: '', size: '25', since: '', until: '', origin: '', originId: '', cursor: '', page: '' })}
			data-testid="run-filters-clear"
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
		No runs {originPhrase}match these filters in namespace {namespaceLabel(data.namespace)}.
	</p>
{:else}
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[760px] border-collapse text-left text-sm" data-testid="run-list">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Run ID</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Orkflow</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Origin</th>
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
						<td class="h-11 px-4 align-middle text-fg-secondary">
							{#if run.origin && originKindSlug(run.origin.kind)}
								<span data-testid="run-row-origin-{run.id}"
									>{originKindSlug(run.origin.kind)}{#if run.origin.id}{' '}<span class="font-mono text-xs"
											>{run.origin.id}</span
										>{/if}</span
								>
							{:else}
								<span data-testid="run-row-origin-{run.id}" class="text-fg-muted" title={UNRECORDED_CAPTION}
									>{UNRECORDED}</span
								>
							{/if}
						</td>
						<td class="h-11 px-4 align-middle"><StatusPill status={run.status} /></td>
						<td class="h-11 px-4 pr-5 align-middle text-fg-muted tabular-nums"
							>{fmtTimestamp(run.createdAt)}</td
						>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="runs" noun="run" page={data.page} count={data.runs.length} {previousHref} {nextHref} {firstHref} onnext={next} />
{/if}
