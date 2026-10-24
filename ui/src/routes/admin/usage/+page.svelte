<script lang="ts">
	// Admin → Usage: what each namespace RESERVED over one window, side by
	// side — the browser form of `orkctl stats usage --group-by namespace`.
	//
	// One page of namespaces at a time, paged and filtered like Admin →
	// Namespaces, and one GetUsage per row ON THIS PAGE, four at a time, each
	// row with its own loading and error state: a namespace the server cannot
	// measure says so in its own row, in the server's words, and the others
	// still render ($lib/adminUsage).
	import { goto, invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import { errorMessage } from '$lib/client';
	import { statsClient } from '$lib/clients/stats';
	import { measureUsage, type UsageRow } from '$lib/adminUsage';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { ListNamespacesRequestSchema } from '$lib/gen/orkstack/v1/namespace_service_pb';
	import type { GetUsageResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import type { FieldFilter } from '$lib/listFilter';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import { fmtInstant } from '$lib/stats';
	import { USAGE_CAVEAT, USAGE_WINDOWS, adminUsageCli, budgetCellLabel, fmtUnitSeconds } from '$lib/usage';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	type Row = UsageRow<GetUsageResponse>;
	let rows = $state<Record<string, Row>>({});
	let generation = 0;

	async function measure(names: string[], window: string) {
		const mine = ++generation;
		rows = Object.fromEntries(names.map((name) => [name, { loading: true, resp: null, error: null }]));
		await measureUsage(
			names,
			window,
			(req) => statsClient(fetch).getUsage(req),
			(name, row) => (rows[name] = row),
			errorMessage,
			() => mine === generation
		);
	}

	$effect(() => {
		void measure(
			data.namespacesList.map((ns) => ns.name),
			data.window
		);
	});

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// ── paging and filters: Admin → Namespaces' coordinates and cursor stack,
	// plus the window. A new window or filter set is a new page 1.
	const coordinates = $derived({
		ns: namespaceLabel(data.namespace),
		window: data.window,
		size: String(data.size),
		cursor: data.cursor,
		page: String(data.page)
	});
	const applied = $derived({ filter: data.applied });
	function href(changes: Record<string, string>, filters: Record<string, FieldFilter[]> = applied) {
		return listHref('/admin/usage', { ...coordinates, ...changes }, filters);
	}
	const stackKey = $derived(pageStackKey('/admin/usage', coordinates, applied));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, data.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[data.page - 1], ahead[data.page]] = [data.cursor, data.nextPageToken];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	const walked = $derived(data.page > 1 && stack[data.page - 1] === data.cursor);
	const previousHref = $derived(walked ? href({ cursor: stack[data.page - 2] ?? '', page: String(data.page - 1) }) : undefined);
	const nextHref = $derived(data.nextPageToken ? href({ cursor: data.nextPageToken, page: String(data.page + 1) }) : undefined);
	const firstHref = $derived(href({ cursor: '', page: '' }));
	function applyFilters(filters: FieldFilter[]) {
		writePageStack(stackKey, []);
		void goto(href({ cursor: '', page: '' }, { filter: filters }), { keepFocus: true, noScroll: true });
	}
	const clearHref = $derived(href({ cursor: '', page: '' }, {}));
	const windowHref = (window: string) => href({ window, cursor: '', page: '' });
</script>

<svelte:head><title>Admin — Usage — OrkStack</title></svelte:head>

<SheetHeader title="Usage">
	{#snippet meta()}What each namespace reserved over the last <span class="figure text-ink">{data.window}</span>{/snippet}
	{#snippet actions()}
		<span class="caps text-ink-muted" aria-hidden="true">Window</span>
		<div role="group" aria-label="Usage window" class="inline-flex h-8 rounded-sm border border-rule-strong bg-paper">
			{#each USAGE_WINDOWS as preset (preset)}
				<a
					data-testid="usage-window-{preset}"
					aria-current={data.window === preset ? 'page' : undefined}
					href={windowHref(preset)}
					class="figure inline-flex items-center border-r border-rule-strong px-3 text-sm text-ink-secondary transition-colors last:border-r-0 hover:bg-hover hover:text-ink current:bg-blue-tint current:font-semibold current:text-blue-strong"
					>{preset}</a
				>
			{/each}
		</div>
		<CliEquivalent command={adminUsageCli(data.window)} testid="usage-admin-cli" />
	{/snippet}
</SheetHeader>

<ScopeNote testid="usage-admin-scope">
	What each namespace RESERVED over the last {data.window}, each measured on its own, one page of namespaces at a
	time. Deployment-wide: the namespace switcher does not filter this page; the filters below do.
</ScopeNote>

<ListFilter
	request={ListNamespacesRequestSchema}
	applied={data.applied}
	testid="usage-namespaces"
	command={['orkctl', 'namespace', 'list']}
	onapply={applyFilters}
	{clearHref}
/>

{#if data.namespacesError}
	<ErrorPanel
		testid="usage-admin-error"
		retryTestid="usage-admin-retry"
		subject="the namespaces"
		message={data.namespacesError}
		busy={retrying}
		onretry={retry}
	/>
{:else}
	<div class="overflow-x-auto border-t-2 border-ink">
		<table class="register min-w-[900px]" data-testid="usage-table">
			<thead>
				<tr>
					<th scope="col">Namespace</th>
					<th scope="col" class="text-right">Runs</th>
					<th scope="col" class="text-right">Tasks</th>
					<th scope="col" class="text-right">Attempts</th>
					<th scope="col" class="text-right">Task-seconds</th>
					<th scope="col" class="text-right">cpu-seconds</th>
					<th scope="col" class="text-right">GiB-seconds</th>
					<th scope="col">Computed</th>
					<th scope="col">Budget</th>
				</tr>
			</thead>
			<tbody>
				{#each data.namespacesList as ns (ns.name)}
					{@const row = rows[ns.name]}
					<tr class="transition-colors hover:bg-hover" data-testid="usage-ns-{ns.name}">
						<td>
							<a class="link font-mono font-semibold" href={withNamespace('/', ns.name)}>{ns.name}</a>
						</td>
						{#if !row || row.loading}
							<td colspan="8" class="text-xs text-ink-muted" data-testid="usage-ns-{ns.name}-loading"
								>measuring…</td
							>
						{:else if row.error}
							<td colspan="8" class="severity-error text-xs" data-testid="usage-ns-{ns.name}-error"
								>{row.error}</td
							>
						{:else if row.resp}
							{@const total = row.resp.total}
							<td class="figure text-right" data-testid="usage-ns-{ns.name}-runs">{(total?.runs ?? 0n).toString()}</td>
							<td class="figure text-right">{(total?.tasks ?? 0n).toString()}</td>
							<td class="figure text-right">{(total?.attempts ?? 0n).toString()}</td>
							<td class="figure text-right">{fmtUnitSeconds(total?.taskSeconds ?? 0)}</td>
							<td class="figure text-right" data-testid="usage-ns-{ns.name}-cpu-seconds">{fmtUnitSeconds(total?.cpuSeconds ?? 0)}</td>
							<td class="figure text-right">{fmtUnitSeconds(total?.gibSeconds ?? 0)}</td>
							<td class="figure text-xs text-ink-muted">{fmtInstant(row.resp.computedAt)}</td>
							<td class="text-ink-secondary" data-testid="usage-ns-{ns.name}-budget">{budgetCellLabel(row.resp)}</td>
						{/if}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="usage-namespaces" noun="namespace" page={data.page} count={data.namespacesList.length} {previousHref} {nextHref} {firstHref} onnext={next} />
	<p class="mt-3 max-w-prose text-xs text-ink-secondary" data-testid="usage-admin-caveat">{USAGE_CAVEAT}</p>
{/if}
