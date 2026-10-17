<script lang="ts">
	import { goto, invalidateAll } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import Select from '$lib/components/Select.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { fmtTimestamp } from '$lib/status';
	import { UNRECORDED, UNRECORDED_CAPTION, originKindSlug } from '$lib/origin';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { shellQuote } from '$lib/runDetail';
	import { equalityOf, type FieldFilter } from '$lib/listFilter';
	import { ListRunsRequestSchema } from '$lib/gen/orkstack/v1/run_service_pb';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const sizeOptions = [10, 25, 50, 100].map((size) => ({ value: String(size), label: String(size) }));

	// One Orkflow, when the filters name exactly one: the header says so and
	// Submit a run goes to its page. The standalone Submit Run page is gone —
	// a run is started from the Orkflow it belongs to — so a list that is not
	// narrowed to one can only offer the picker.
	const orkflowName = $derived(equalityOf(data.applied, 'orkflow_name') ?? '');
	const submitHref = $derived(
		withNamespace(orkflowName ? `/orkflows/${encodeURIComponent(orkflowName)}` : '/orkflows', data.namespace)
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
	// spells them, and one builder over them. listHref only ever `set`s a
	// scalar, and writes the filters once each in canonical order, so no
	// gesture here can leave two of the same coordinate in the URL.
	const coordinates = $derived({ ns: namespaceLabel(data.namespace), size: String(data.size), cursor: data.cursor, page: String(data.page) });
	const applied = $derived({ filter: data.applied });
	function href(changes: Record<string, string>, filters: Record<string, FieldFilter[]> = applied) {
		return listHref('/runs', { ...coordinates, ...changes }, filters);
	}

	// The cursors BEHIND this page: per filter set, per tab, in sessionStorage
	// rather than in the URL, which used to grow by one escaped engine cursor
	// per Next click (docs/UI_CLI_PARITY.md "Paging in the browser").
	//
	// Read in an `$effect` and not a `$derived`: the first render is the
	// server's, where there is no sessionStorage, and a derived computed there
	// would keep its empty answer through hydration.
	const stackKey = $derived(pageStackKey('/runs', coordinates, applied));
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

	// Page size is a field of the filter form and applies with Apply filters,
	// like every other field of it: one form never applies some fields on
	// change and others on submit. A new filter set (or size) is a new page 1
	// of a new listing, so the path back through the old one is dropped
	// rather than left to be trusted later.
	function sizeFromUrl() {
		return String(data.size);
	}
	let sizeDraft = $state(sizeFromUrl());
	$effect(() => {
		sizeDraft = sizeFromUrl();
	});
	function applyFilters(filters: FieldFilter[]) {
		writePageStack(stackKey, []);
		void goto(href({ size: sizeDraft || '25', cursor: '', page: '' }, { filter: filters }), { keepFocus: true, noScroll: true });
	}
	const clearHref = $derived(href({ cursor: '', page: '' }, {}));
	const command = $derived(['orkctl', '-n', shellQuote(namespaceLabel(data.namespace)), 'run', 'list']);
</script>

<svelte:head><title>Runs — OrkStack</title></svelte:head>

<div class="mb-6 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
	<h1 class="text-xl font-semibold tracking-tight text-fg" data-testid="runs-heading">
		Runs{#if orkflowName}
			<span class="ml-2 text-base font-normal text-fg-muted">for {orkflowName}</span>
		{/if}
	</h1>
	<a
		href={submitHref}
		data-testid="runs-submit-run"
		class="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 rounded-md bg-brand-600 px-3.5 text-sm font-medium text-on-ink transition-colors hover:bg-brand-700 active:bg-brand-800"
	>
		{orkflowName ? 'Submit a run' : 'Browse orkflows'}
	</a>
</div>

<ListFilter request={ListRunsRequestSchema} applied={data.applied} testid="run" {command} onapply={applyFilters} {clearHref}>
	<div class="flex flex-col gap-1.5 text-sm">
		<label class="font-medium text-fg" for="run-filter-size">Page size</label>
		<div class="w-24"><Select id="run-filter-size" testid="run-filter-size" value={sizeDraft} options={sizeOptions} onchange={(next) => (sizeDraft = next)} /></div>
	</div>
</ListFilter>

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
		class="rounded-md border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
		data-testid="runs-empty"
	>
		{#if data.applied.length > 0}
			No runs match these filters in namespace {namespaceLabel(data.namespace)}.
		{:else}
			No runs in namespace {namespaceLabel(data.namespace)} yet.
		{/if}
	</p>
{:else}
	<div class="overflow-x-auto rounded-md border border-border bg-surface">
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
						class="border-t border-border-subtle transition-colors hover:bg-surface-hover"
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
