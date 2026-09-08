<script lang="ts">
	import { goto, invalidate } from '$app/navigation';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import StatCard from '$lib/components/StatCard.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		UNAVAILABLE,
		finishedRuns,
		fmtAge,
		fmtCount,
		fmtDuration,
		fmtInstant,
		fmtPercent,
		timestampMs
	} from '$lib/stats';
	import { fmtTimestamp } from '$lib/status';
	import { shellQuote } from '$lib/runDetail';
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
	//
	// The count is stated as a SHARE of the runs it is drawn from — the
	// window's runs that have FINISHED — not as a bare number. Printed alone
	// beside a success rate over a different set, it read as a second,
	// contradictory total: `no run finished` next to `p50 58m over 12 finished
	// runs` was a real answer this page could render.
	//
	// The denominator excludes `running`, and that is the whole point of
	// naming it here. `running` is the one key that is NOT about the window —
	// it is a fact about now (stats.proto), so a namespace with one long-lived
	// run started last week would inflate the total and make the samples a
	// share of a set they are not drawn from. `orkctl stats` counts the same
	// three keys (cmd/orkctl/stats.go), and one number must not have two
	// denominators.
	const finished = $derived(finishedRuns(counts));
	const coverageNote = $derived.by(() => {
		if (!stats || durationSamples === 0) return 'no run finished in this window';
		const since = timestampMs(stats.durationCoverageSince);
		const windowSince = timestampMs(stats.windowSince);
		const base = `${durationSamples} of the window's ${finished} finished runs`;
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
	function windowHref(window: string, since = '') { return withNamespace(`/?window=${encodeURIComponent(window)}${since ? `&since=${encodeURIComponent(since)}` : ''}`, data.namespace); }
	function setCustom(event: SubmitEvent) { event.preventDefault(); const since = String(new FormData(event.currentTarget as HTMLFormElement).get('since') ?? ''); if (since) void goto(windowHref('custom', new Date(since).toISOString())); }
	const statsCli = $derived(`orkctl --namespace ${shellQuote(namespaceLabel(data.namespace))} stats --since ${shellQuote(data.window === 'custom' && data.sinceParam ? data.sinceParam : data.window)}`);
	let copied = $state(false);
	async function copyCli() { await navigator.clipboard.writeText(statsCli); copied = true; }
</script>

<svelte:head><title>Dashboard — OrkStack</title></svelte:head>

<div class="mb-6 flex flex-wrap items-baseline gap-3">
	<h1 class="text-xl font-semibold tracking-tight text-fg" data-testid="dashboard-heading">
		Dashboard
	</h1>
	<p class="text-sm text-fg-muted" data-testid="dashboard-namespace">
		namespace <span class="font-mono text-fg-secondary">{answeredFor}</span>
	</p>
	{#if stats}
		<p class="text-sm text-fg-muted" data-testid="dashboard-window">
			{fmtInstant(stats.windowSince)} → {fmtInstant(stats.windowUntil)}
		</p>
		<p class="ml-auto text-xs text-fg-muted" data-testid="dashboard-computed-at">
			measured {fmtAge(stats.computedAt, now)}
		</p>
	{/if}
</div>

<section class="mb-6 flex flex-wrap items-end gap-3" data-testid="dashboard-window-controls">
	<span class="text-sm font-medium text-fg">Window</span>
	{#each ['15m', '6h', '24h'] as preset}
		<a data-testid="dashboard-window-{preset}" aria-current={data.window === preset ? 'page' : undefined} class="rounded border border-border px-2 py-1 text-sm text-fg" href={windowHref(preset)}>{preset}</a>
	{/each}
	<form class="flex items-end gap-2" onsubmit={setCustom}><label class="text-sm text-fg-secondary">Since<input name="since" type="datetime-local" class="ml-1 h-8 rounded border border-border bg-surface px-2 text-fg" /></label><button class="h-8 rounded border border-border px-2 text-sm text-fg">Custom</button></form>
	<details class="ml-auto"><summary class="cursor-pointer text-sm text-brand-700">CLI equivalent</summary><pre data-testid="dashboard-cli" class="mt-2 rounded bg-surface-sunken p-2 text-xs text-fg"><code>{statsCli}</code></pre><button type="button" onclick={copyCli} class="text-sm text-brand-700">{copied ? 'Copied' : 'Copy command'}</button></details>
</section>

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
		<div class="grid grid-cols-2 gap-4 md:grid-cols-4 xl:grid-cols-7">
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
			<h2 class="mb-3 text-sm font-semibold text-fg">Pool health</h2>
			<p class="mb-3 text-sm text-fg-muted">
				Where a task of this namespace can run, and how many Orkers are serving each pool right
				now.
			</p>
			{#if pools.length === 0}
				<p
					class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
					data-testid="pools-empty"
				>
					No pool could be read for {shown}.
				</p>
			{:else}
				<div class="space-y-2" data-testid="pool-list">
					{#each pools as pool (pool.name)}
						<div
							class="rounded-lg border border-border bg-surface px-4 py-3 shadow-xs"
							data-testid="pool-{pool.name}"
						>
							<div class="flex items-center justify-between gap-3">
								<div class="font-mono text-sm text-fg">
									{pool.name}
									{#if pool.isDefault}<span
											class="ml-2 rounded-full border border-border bg-surface-sunken px-2 py-0.5 text-xs text-fg-secondary"
											data-testid="pool-default-{pool.name}">default</span
										>{/if}
								</div>
								<span
									data-testid="pool-orkers-{pool.name}"
									class="rounded-full border px-2.5 py-0.5 text-xs font-medium {pool.liveOrkerCount >
									0
										? 'border-succeeded-border bg-succeeded-bg text-succeeded-fg'
										: 'border-warning-200 bg-warning-50 text-warning-700'}"
								>
									{pool.liveOrkerCount}
									{pool.liveOrkerCount === 1 ? 'Orker' : 'Orkers'}
								</span>
							</div>
							<!-- Driven by the live Orker COUNT: an activity is an image,
								so a pool carries no inventory of what it can run. -->
							<div class="mt-1 text-xs text-fg-muted" data-testid="pool-serving-{pool.name}">
								{#if pool.liveOrkerCount > 0}
									serving now
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
			<h2 class="mb-3 text-sm font-semibold text-fg">Recent failures</h2>
			<p class="mb-3 text-sm text-fg-muted">
				Orkflows whose runs failed in this window, worst first.
			</p>
			{#if failures.length === 0}
				<p
					class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
					data-testid="failures-empty"
				>
					No run failed in this window.
				</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-border bg-surface shadow-xs">
					<table class="w-full min-w-[420px] border-collapse text-sm" data-testid="failure-table">
						<thead class="bg-surface-sunken">
							<tr>
								<th class="h-10 px-4 pl-5 text-left text-xs font-medium text-fg-muted">Orkflow</th>
								<th class="h-10 px-4 text-right text-xs font-medium text-fg-muted">Failed</th>
								<th class="h-10 px-4 pr-5 text-right text-xs font-medium text-fg-muted">Finished</th
								>
							</tr>
						</thead>
						<tbody>
							{#each failures as failure (failure.orkflowName)}
								<tr
									class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
									data-testid="failure-{failure.orkflowName}"
								>
									<td class="h-11 px-4 pl-5 align-middle">
										<a
											class="font-medium text-fg hover:text-brand-700"
											href={withNamespace(
												`/runs?orkflow=${encodeURIComponent(failure.orkflowName)}`,
												data.namespace
											)}>{failure.orkflowName}</a
										>
									</td>
									<td class="h-11 px-4 text-right align-middle text-failed-fg tabular-nums"
										>{failure.failed.toString()}</td
									>
									<td class="h-11 px-4 pr-5 text-right align-middle text-fg-muted tabular-nums"
										>{failure.total.toString()}</td
									>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</section>
	</div>

	<section class="mt-8">
		<h2 class="mb-3 text-sm font-semibold text-fg">Running now</h2>
		{#if running.length === 0}
			<p
				class="rounded-lg border border-dashed border-border px-6 py-12 text-center text-sm text-fg-muted"
				data-testid="running-empty"
			>
				Nothing is running in {shown}.
			</p>
		{:else}
			<div class="space-y-2" data-testid="running-list">
				{#each running as run (run.id)}
					<a
						href={withNamespace(`/runs/${run.id}`, data.namespace)}
						class="flex items-center justify-between gap-3 rounded-lg border border-border bg-surface px-4 py-3 shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover"
						data-testid="running-{run.id}"
					>
						<div>
							<div class="font-mono text-sm text-fg">{run.id}</div>
							<div class="text-xs text-fg-muted">
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
