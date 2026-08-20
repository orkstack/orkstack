<script lang="ts">
	import { invalidate } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import StatCard from '$lib/components/StatCard.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		UNAVAILABLE,
		fmtAge,
		fmtCount,
		fmtDuration,
		fmtInstant,
		fmtPercent,
		timestampMs
	} from '$lib/stats';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const stats = $derived(data.stats);
	const shown = $derived(namespaceLabel(data.namespace));

	// Every number below is scoped to the namespace the switcher selected, and
	// the answer says which namespace it is FOR: an empty request resolves to
	// `default` server-side, and a page that could not name what it is showing
	// would be showing an unlabelled number.
	const answeredFor = $derived(namespaceLabel(stats?.namespace ?? data.namespace));

	const counts = $derived(stats?.runsByStatus ?? {});
	const running = $derived(stats?.running ?? []);
	const pools = $derived(stats?.pools ?? []);
	const failures = $derived(stats?.mostFailing ?? []);

	const successRate = $derived(
		!stats ? UNAVAILABLE : stats.successRateDefined ? fmtPercent(stats.successRate) : UNAVAILABLE
	);
	const durationSamples = $derived(stats ? Number(stats.durationSampleCount) : 0);
	const p50 = $derived(!stats || durationSamples === 0 ? UNAVAILABLE : fmtDuration(stats.p50Duration));
	const p95 = $derived(!stats || durationSamples === 0 ? UNAVAILABLE : fmtDuration(stats.p95Duration));

	// The percentiles can cover less than the window asked for: how long a run
	// took is known only while the cluster still holds its execution record.
	// Saying so is the difference between "the last three days of latency" and
	// a month of it (proto/orkstack/v1/stats.proto, duration_coverage_since).
	const coverageNote = $derived.by(() => {
		if (!stats || durationSamples === 0) return 'no run finished in this window';
		const since = timestampMs(stats.durationCoverageSince);
		const windowSince = timestampMs(stats.windowSince);
		const base = `${durationSamples} finished ${durationSamples === 1 ? 'run' : 'runs'}`;
		if (since !== null && windowSince !== null && since > windowSince + 60_000) {
			return `${base}, covering only since ${fmtInstant(stats.durationCoverageSince)}`;
		}
		return base;
	});

	const rateNote = $derived(
		stats?.successRateDefined
			? 'succeeded ÷ finished (canceled counts as finished)'
			: 'no run finished in this window'
	);

	// Auto-refresh at the engine's own cache TTL (ORK_STATS_CACHE_TTL, 5s by
	// default): asking faster than the cache can change the answer only costs
	// requests. It invalidates THIS loader, not every loader on the page, so a
	// refresh does not also re-read the namespace list.
	let now = $state(Date.now());
	let refreshing = $state(false);
	$effect(() => {
		const timer = setInterval(() => {
			now = Date.now();
			void invalidate('ork:stats');
		}, 5000);
		return () => clearInterval(timer);
	});

	async function retry() {
		refreshing = true;
		try {
			await invalidate('ork:stats');
		} finally {
			refreshing = false;
		}
	}
</script>

<svelte:head><title>Dashboard — OrkStack</title></svelte:head>

<div class="mb-6 flex flex-wrap items-baseline gap-3">
	<h1 class="text-xl font-bold" data-testid="dashboard-heading">Dashboard</h1>
	<p class="text-sm text-zinc-400" data-testid="dashboard-namespace">
		namespace <span class="font-mono text-zinc-200">{answeredFor}</span>
	</p>
	{#if stats}
		<p class="text-sm text-zinc-500" data-testid="dashboard-window">
			{fmtInstant(stats.windowSince)} → {fmtInstant(stats.windowUntil)}
		</p>
		<p class="ml-auto text-xs text-zinc-500" data-testid="dashboard-computed-at">
			measured {fmtAge(stats.computedAt, now)}
		</p>
	{/if}
</div>

{#if data.error}
	<ErrorPanel
		testid="dashboard-error"
		retryTestid="dashboard-retry"
		subject="the numbers for namespace {shown}"
		message={data.error}
		busy={refreshing}
		onretry={retry}
	/>
{:else if stats}
	<section class="mb-8">
		<div class="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
			<StatCard
				label="Running"
				value={fmtCount(counts['running'])}
				testid="stat-running"
				tone="busy"
			/>
			<StatCard
				label="Succeeded"
				value={fmtCount(counts['succeeded'])}
				testid="stat-succeeded"
				tone="good"
			/>
			<StatCard label="Failed" value={fmtCount(counts['failed'])} testid="stat-failed" tone="bad" />
			<StatCard label="Canceled" value={fmtCount(counts['canceled'])} testid="stat-canceled" />
			<StatCard
				label="Success rate"
				value={successRate}
				note={rateNote}
				testid="stat-success-rate"
				tone="good"
			/>
			<StatCard label="p50 duration" value={p50} note={coverageNote} testid="stat-p50" />
			<StatCard label="p95 duration" value={p95} note={coverageNote} testid="stat-p95" />
		</div>
	</section>

	<div class="grid gap-6 lg:grid-cols-2">
		<section>
			<h2 class="mb-3 text-lg font-semibold">Pool health</h2>
			<p class="mb-3 text-sm text-zinc-500">
				Where a task of this namespace can run, and how many Orkers are serving each pool right
				now.
			</p>
			{#if pools.length === 0}
				<p class="rounded-lg border border-zinc-800 p-4 text-sm text-zinc-500" data-testid="pools-empty">
					No pool could be read for {shown}.
				</p>
			{:else}
				<div class="space-y-2" data-testid="pool-list">
					{#each pools as pool (pool.name)}
						<div
							class="rounded-lg border border-zinc-800 bg-zinc-900/40 px-4 py-3"
							data-testid="pool-{pool.name}"
						>
							<div class="flex items-center justify-between gap-3">
								<div class="font-mono text-sm text-zinc-100">
									{pool.name}
									{#if pool.isDefault}<span
											class="ml-2 rounded-full bg-zinc-700/50 px-2 py-0.5 text-xs text-zinc-300"
											data-testid="pool-default-{pool.name}">default</span
										>{/if}
								</div>
								<span
									data-testid="pool-orkers-{pool.name}"
									class="rounded-full px-2.5 py-0.5 text-xs ring-1 {pool.liveOrkerCount > 0
										? 'bg-emerald-500/15 text-emerald-300 ring-emerald-400/40'
										: 'bg-amber-500/15 text-amber-300 ring-amber-400/40'}"
								>
									{pool.liveOrkerCount}
									{pool.liveOrkerCount === 1 ? 'Orker' : 'Orkers'}
								</span>
							</div>
							<div class="mt-1 text-xs text-zinc-500" data-testid="pool-plugins-{pool.name}">
								{#if pool.plugins.length > 0}
									plugins: {pool.plugins.join(', ')}
								{:else}
									no Orker is serving this pool, so a task sent here would wait
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</section>

		<section>
			<h2 class="mb-3 text-lg font-semibold">Recent failures</h2>
			<p class="mb-3 text-sm text-zinc-500">
				Orkflows whose runs failed in this window, worst first.
			</p>
			{#if failures.length === 0}
				<p
					class="rounded-lg border border-zinc-800 p-4 text-sm text-zinc-500"
					data-testid="failures-empty"
				>
					No run failed in this window.
				</p>
			{:else}
				<table class="w-full text-sm" data-testid="failure-table">
					<thead class="text-xs uppercase text-zinc-500">
						<tr>
							<th class="py-2 text-left font-medium">Orkflow</th>
							<th class="py-2 text-right font-medium">Failed</th>
							<th class="py-2 text-right font-medium">Finished</th>
						</tr>
					</thead>
					<tbody>
						{#each failures as failure (failure.orkflowName)}
							<tr class="border-t border-zinc-800" data-testid="failure-{failure.orkflowName}">
								<td class="py-2">
									<a
										class="text-emerald-300 hover:underline"
										href={withNamespace(
											`/runs?orkflow=${encodeURIComponent(failure.orkflowName)}`,
											data.namespace
										)}>{failure.orkflowName}</a
									>
								</td>
								<td class="py-2 text-right text-rose-300">{failure.failed.toString()}</td>
								<td class="py-2 text-right text-zinc-400">{failure.total.toString()}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</section>
	</div>

	<section class="mt-8">
		<h2 class="mb-3 text-lg font-semibold">Running now</h2>
		{#if running.length === 0}
			<p class="rounded-lg border border-zinc-800 p-4 text-sm text-zinc-500" data-testid="running-empty">
				Nothing is running in {shown}.
			</p>
		{:else}
			<div class="space-y-2" data-testid="running-list">
				{#each running as run (run.id)}
					<a
						href={withNamespace(`/runs/${run.id}`, data.namespace)}
						class="flex items-center justify-between gap-3 rounded-lg border border-zinc-800 bg-zinc-900/40 px-4 py-3 hover:border-zinc-700"
						data-testid="running-{run.id}"
					>
						<div>
							<div class="font-mono text-sm text-zinc-100">{run.id}</div>
							<div class="text-xs text-zinc-500">
								{run.orkflowName}{#if run.orkflowVersion}@{run.orkflowVersion}{/if} · started
								{fmtTimestamp(run.startedAt ?? run.createdAt)}
							</div>
						</div>
						<StatusPill status={run.status} testid="running-status-{run.id}" />
					</a>
				{/each}
			</div>
		{/if}
	</section>
{/if}
