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
	import ListFilter from '$lib/components/ListFilter.svelte';
	import Pagination from '$lib/components/Pagination.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import { measureUsage, type UsageRow } from '$lib/adminUsage';
	import { listHref, pageStackKey, readPageStack, writePageStack } from '$lib/browsing';
	import { ListNamespacesRequestSchema } from '$lib/gen/orkstack/v1/service_pb';
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
			(req) => orkstack(fetch).stats.getUsage(req),
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

<div class="mb-4 flex flex-wrap items-center gap-3">
	<h2 class="text-base font-semibold text-fg">Usage</h2>
	<p class="text-sm text-fg-muted">
		What each namespace RESERVED over the last {data.window}, each measured on its own, one page of namespaces at a
		time. Deployment-wide: the namespace switcher does not filter this page; the filters below do.
	</p>
</div>

<section class="mb-4 flex flex-wrap items-center gap-3">
	<span class="text-sm font-medium text-fg">Window</span>
	<div role="group" aria-label="Usage window" class="inline-flex h-9 rounded-md border border-border bg-surface shadow-xs">
		{#each USAGE_WINDOWS as preset (preset)}
			<a
				data-testid="usage-window-{preset}"
				aria-current={data.window === preset ? 'page' : undefined}
				href={windowHref(preset)}
				class="inline-flex items-center border-r border-border px-3 text-sm font-medium text-fg-muted transition-colors duration-150 first:rounded-l-md last:rounded-r-md last:border-r-0 hover:bg-surface-hover hover:text-fg current:bg-brand-50 current:text-brand-700"
				>{preset}</a
			>
		{/each}
	</div>
	<div class="ml-auto"><CliEquivalent command={adminUsageCli(data.window)} testid="usage-admin-cli" /></div>
</section>

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
	<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
		<table class="w-full min-w-[900px] border-collapse text-left text-sm" data-testid="usage-table">
			<thead class="bg-surface-sunken">
				<tr>
					<th class="h-10 px-4 pl-5 text-xs font-medium text-fg-muted">Namespace</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Runs</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Tasks</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Attempts</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Task-seconds</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">cpu-seconds</th>
					<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">GiB-seconds</th>
					<th class="h-10 px-4 text-xs font-medium text-fg-muted">Computed</th>
					<th class="h-10 px-4 pr-5 text-xs font-medium text-fg-muted">Budget</th>
				</tr>
			</thead>
			<tbody>
				{#each data.namespacesList as ns (ns.name)}
					{@const row = rows[ns.name]}
					<tr class="border-t border-border-subtle" data-testid="usage-ns-{ns.name}">
						<td class="h-11 px-4 pl-5 align-middle font-medium">
							<a class="text-fg hover:text-brand-700" href={withNamespace('/', ns.name)}>{ns.name}</a>
						</td>
						{#if !row || row.loading}
							<td colspan="8" class="h-11 px-4 align-middle text-xs text-fg-muted" data-testid="usage-ns-{ns.name}-loading"
								>measuring…</td
							>
						{:else if row.error}
							<td colspan="8" class="h-11 px-4 align-middle text-xs text-danger-700" data-testid="usage-ns-{ns.name}-error"
								>{row.error}</td
							>
						{:else if row.resp}
							{@const total = row.resp.total}
							<td class="h-11 px-4 text-right align-middle tabular-nums" data-testid="usage-ns-{ns.name}-runs">{(total?.runs ?? 0n).toString()}</td>
							<td class="h-11 px-4 text-right align-middle tabular-nums">{(total?.tasks ?? 0n).toString()}</td>
							<td class="h-11 px-4 text-right align-middle tabular-nums">{(total?.attempts ?? 0n).toString()}</td>
							<td class="h-11 px-4 text-right align-middle tabular-nums">{fmtUnitSeconds(total?.taskSeconds ?? 0)}</td>
							<td class="h-11 px-4 text-right align-middle tabular-nums" data-testid="usage-ns-{ns.name}-cpu-seconds">{fmtUnitSeconds(total?.cpuSeconds ?? 0)}</td>
							<td class="h-11 px-4 text-right align-middle tabular-nums">{fmtUnitSeconds(total?.gibSeconds ?? 0)}</td>
							<td class="h-11 px-4 align-middle text-xs text-fg-muted">{fmtInstant(row.resp.computedAt)}</td>
							<td class="h-11 px-4 pr-5 align-middle text-fg-secondary" data-testid="usage-ns-{ns.name}-budget">{budgetCellLabel(row.resp)}</td>
						{/if}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
	<Pagination testid="usage-namespaces" noun="namespace" page={data.page} count={data.namespacesList.length} {previousHref} {nextHref} {firstHref} onnext={next} />
	<p class="mt-3 text-xs font-medium text-fg-secondary" data-testid="usage-admin-caveat">{USAGE_CAVEAT}</p>
{/if}
