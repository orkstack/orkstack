<script lang="ts">
	// Admin → Usage: what every namespace RESERVED over one window, side by
	// side — the browser form of `orkctl stats usage --group-by namespace`.
	//
	// One GetUsage per namespace, four at a time, each row with its own
	// loading and error state: a namespace the server cannot measure says so
	// in its own row, in the server's words, and the others still render.
	import { invalidateAll } from '$app/navigation';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import { errorMessage, orkstack } from '$lib/client';
	import type { GetUsageResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import { withNamespace } from '$lib/namespace';
	import { fmtInstant } from '$lib/stats';
	import { USAGE_CAVEAT, USAGE_WINDOWS, adminUsageCli, budgetCellLabel, fmtUnitSeconds } from '$lib/usage';
	import { durationFromMs } from '@bufbuild/protobuf/wkt';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const WINDOW_MS: Record<string, number> = { '24h': 24 * 3_600_000, '168h': 168 * 3_600_000, '720h': 720 * 3_600_000 };
	const FAN_OUT = 4;

	type Row = { loading: boolean; resp: GetUsageResponse | null; error: string | null };
	let rows = $state<Record<string, Row>>({});
	let generation = 0;

	async function measure(names: string[], window: string) {
		const mine = ++generation;
		rows = Object.fromEntries(names.map((name) => [name, { loading: true, resp: null, error: null }]));
		let next = 0;
		async function worker() {
			while (next < names.length) {
				const name = names[next++];
				try {
					const resp = await orkstack(fetch).stats.getUsage({ namespace: name, lookback: durationFromMs(WINDOW_MS[window]) });
					if (mine !== generation) return;
					rows[name] = { loading: false, resp, error: null };
				} catch (err) {
					if (mine !== generation) return;
					rows[name] = { loading: false, resp: null, error: errorMessage(err) };
				}
			}
		}
		await Promise.all(Array.from({ length: Math.min(FAN_OUT, names.length) }, worker));
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

	const href = (window: string) => withNamespace(`/admin/usage?window=${window}`, data.namespace);
</script>

<svelte:head><title>Admin — Usage — OrkStack</title></svelte:head>

<div class="mb-4 flex flex-wrap items-center gap-3">
	<h2 class="text-base font-semibold text-fg">Usage</h2>
	<p class="text-sm text-fg-muted">
		What every namespace RESERVED over the last {data.window}, each measured on its own. Deployment-wide: the namespace
		switcher does not filter this page.
	</p>
</div>

<section class="mb-4 flex flex-wrap items-center gap-3">
	<span class="text-sm font-medium text-fg">Window</span>
	<div role="group" aria-label="Usage window" class="inline-flex h-9 rounded-md border border-border bg-surface shadow-xs">
		{#each USAGE_WINDOWS as preset (preset)}
			<a
				data-testid="usage-window-{preset}"
				aria-current={data.window === preset ? 'page' : undefined}
				href={href(preset)}
				class="inline-flex items-center border-r border-border px-3 text-sm font-medium text-fg-muted transition-colors duration-150 first:rounded-l-md last:rounded-r-md last:border-r-0 hover:bg-surface-hover hover:text-fg current:bg-brand-50 current:text-brand-700"
				>{preset}</a
			>
		{/each}
	</div>
	<div class="ml-auto"><CliEquivalent command={adminUsageCli(data.window)} testid="usage-admin-cli" /></div>
</section>

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
	<p class="mt-3 text-xs font-medium text-fg-secondary" data-testid="usage-admin-caveat">{USAGE_CAVEAT}</p>
{/if}
