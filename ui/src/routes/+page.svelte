<script lang="ts">
	import { pollWhileVisible } from '$lib/visibility';
	import { goto, invalidate } from '$app/navigation';
	import { navigating } from '$app/state';
	import CliEquivalent from '$lib/components/CliEquivalent.svelte';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import FailureFingerprints from '$lib/components/FailureFingerprints.svelte';
	import FlakeIndex from '$lib/components/FlakeIndex.svelte';
	import HoldBanner from '$lib/components/HoldBanner.svelte';
	import Popover from '$lib/components/Popover.svelte';
	import CountCell from '$lib/components/CountCell.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import StatusPill from '$lib/components/StatusPill.svelte';
	import UsageCard from '$lib/components/UsageCard.svelte';
	import { listHref, runWindowFilters, sinceInputValue } from '$lib/browsing';
	import { namespaceLabel, withNamespace } from '$lib/namespace';
	import {
		UNAVAILABLE,
		finishedRuns,
		fmtAge,
		fmtCount,
		fmtDuration,
		fmtInstant,
		fmtInstantMs,
		fmtPercent,
		timestampMs
	} from '$lib/stats';
	import { fmtTimestamp } from '$lib/status';
	import { shellQuote } from '$lib/runDetail';
	import { afterNavigation } from '$lib/settle';
	import { setTitleBlock } from '$lib/sheet.svelte';
	import { Status } from '$lib/gen/orkstack/v1/common_pb';
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
	// refresh does not also re-read the namespace list. A tick never takes the
	// router from a navigation the reader began — it waits that navigation out
	// and is dropped when it left this page ($lib/settle) — so a click on the
	// dashboard is never lost to the timer.
	let now = $state(Date.now());
	let refreshing = $state(false);
	// Only while the tab is visible ($lib/visibility): a background dashboard
	// asks nothing, and coming back refreshes at once.
	$effect(() => {
		let mounted = true;
		const stop = pollWhileVisible(() => {
			now = Date.now();
			void afterNavigation(
				() => navigating.complete,
				() => mounted,
				() => invalidate('ork:stats')
			);
		}, 5000);
		return () => {
			mounted = false;
			stop();
		};
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

	// The custom window is the fourth member of the window group, and the
	// segment states the range it applied: `Since 9/10/2026, 2:30:00 AM`, in
	// the same fmtInstant format as the header line above it, so the two can
	// never disagree about one instant. The draft in the popover is refilled
	// from the URL on every navigation — a field left empty beside a window
	// that is in force was the whole complaint.
	let customOpen = $state(false);
	function sinceDraft() { return sinceInputValue(data.sinceParam); }
	let customSince = $state(sinceDraft());
	$effect(() => {
		customSince = sinceDraft();
	});
	const customLabel = $derived(data.window === 'custom' && data.sinceParam ? `Since ${fmtInstantMs(Date.parse(data.sinceParam))}` : 'Custom');
	const customValid = $derived(!Number.isNaN(Date.parse(customSince)));
	function applyCustom(event: SubmitEvent) { event.preventDefault(); if (!customValid) return; customOpen = false; void goto(windowHref('custom', new Date(customSince).toISOString())); }
	const windowWord = $derived(data.window === 'custom' ? customLabel : `last ${data.window}`);
	setTitleBlock(() => [
		{ label: 'PAGE', value: 'Dashboard' },
		{ label: 'NAMESPACE', value: answeredFor },
		{ label: 'WINDOW', value: windowWord }
	]);
	const statsCli = $derived(`orkctl --namespace ${shellQuote(namespaceLabel(data.namespace))} stats --since ${shellQuote(data.window === 'custom' && data.sinceParam ? data.sinceParam : data.window)}`);
</script>

<svelte:head><title>Dashboard — OrkStack</title></svelte:head>

{#if data.namespaceHold}
	<HoldBanner namespace={shown} hold={data.namespaceHold} testid="dashboard-hold-banner" />
{/if}

{#snippet facts()}
	<p class="flex flex-wrap items-baseline gap-x-3 gap-y-1">
		<span data-testid="dashboard-namespace">namespace <span class="font-mono text-ink">{answeredFor}</span></span>
		{#if stats}
			<span class="figure text-ink-secondary" data-testid="dashboard-window">
				{fmtInstant(stats.windowSince)} → {fmtInstant(stats.windowUntil)}
			</span>
			<span class="text-xs text-ink-muted" data-testid="dashboard-computed-at">measured {fmtAge(stats.computedAt, now)}</span>
		{/if}
	</p>
{/snippet}

{#snippet windowControls()}
	<section class="flex flex-wrap items-center gap-2" data-testid="dashboard-window-controls" aria-label="Window">
		<span class="caps text-ink-muted">Window</span>
		<!-- One control, FOUR mutually exclusive choices: a connected group says
			that plainly, where separate bordered links read as independent buttons
			any number of which could be on. The active member is marked with
			`aria-current="page"`, and `current:` — app.css's `&[aria-current='page']`
			variant — draws it the way the top bar draws "you are here".

			The fourth member is a <button> and not an <a> because it has no address
			to offer until a range has been typed: it opens the range picker and the
			picker navigates. `aria-current` is valid on a button and the `current:`
			variant keys off the attribute rather than the element, so the custom
			window paints as current exactly like the presets — a window in force
			with nothing marked would be a control that hid its own state. -->
		<div role="group" aria-label="Stats window" class="inline-flex h-8 rounded-sm border border-rule-strong bg-paper">
			{#each ['15m', '6h', '24h'] as preset (preset)}
				<a
					data-testid="dashboard-window-{preset}"
					aria-current={data.window === preset ? 'page' : undefined}
					href={windowHref(preset)}
					class="figure inline-flex items-center border-r border-rule-strong px-3 text-sm text-ink-secondary transition-colors hover:bg-hover hover:text-ink current:bg-blue-tint current:font-semibold current:text-blue-strong"
				>{preset}</a>
			{/each}
			<Popover open={customOpen} onOpenChange={(open) => (customOpen = open)} align="end" testid="dashboard-window-custom-popover">
				{#snippet trigger()}
					<button
						type="button"
						data-testid="dashboard-window-custom"
						aria-current={data.window === 'custom' ? 'page' : undefined}
						aria-expanded={customOpen}
						aria-haspopup="dialog"
						onclick={() => (customOpen = !customOpen)}
						class="inline-flex h-full items-center px-3 text-sm text-ink-secondary transition-colors hover:bg-hover hover:text-ink current:bg-blue-tint current:font-semibold current:text-blue-strong"
					>{customLabel}</button>
				{/snippet}
				{#snippet content()}
					<!-- No "until": GetStats takes a `since` and answers with the
						`until` it used, which the header line states. An end this
						request cannot carry would be a control that does nothing. -->
					<form class="flex w-64 flex-col gap-2 text-sm" onsubmit={applyCustom}>
						<label class="font-medium text-ink" for="dashboard-window-custom-since">Show runs created since</label>
						<input
							id="dashboard-window-custom-since"
							data-testid="dashboard-window-custom-since"
							type="datetime-local"
							bind:value={customSince}
							max={sinceInputValue(new Date(now).toISOString())}
							class="field h-9 w-full px-3 text-sm"
						/>
						<button type="submit" data-testid="dashboard-window-custom-apply" disabled={!customValid} class="btn btn-primary">Apply</button>
					</form>
				{/snippet}
			</Popover>
		</div>
		<CliEquivalent command={statsCli} testid="dashboard-cli" />
	</section>
{/snippet}

<SheetHeader title="Dashboard" testid="dashboard-heading" meta={facts} actions={windowControls} />

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
	<!-- The count band: the summary line of the registers below it, never a
		row of hero numbers. Figures are ink at the title block's value size;
		a status count carries its drawn mark, not a coloured number. -->
	<section
		aria-label="Run counts"
		data-testid="dashboard-counts"
		class="mb-8 grid grid-cols-4 border-y border-rule lg:grid-cols-7"
	>
		<CountCell label="Running" value={fmtCount(counts['running'])} testid="stat-running" status={Status.RUNNING} />
		<CountCell label="Succeeded" value={fmtCount(counts['succeeded'])} testid="stat-succeeded" status={Status.SUCCEEDED} />
		<CountCell label="Failed" value={fmtCount(counts['failed'])} testid="stat-failed" status={Status.FAILED} />
		<CountCell label="Canceled" value={fmtCount(counts['canceled'])} testid="stat-canceled" status={Status.CANCELED} />
		<CountCell label="Success rate" value={successRate} note={rateNote} testid="stat-success-rate" class="max-lg:border-t max-lg:border-l-0 max-lg:pl-0 max-lg:sm:pl-0" />
		<CountCell label="p50 duration" value={p50} note={coverageNote} testid="stat-p50" class="max-lg:border-t" />
		<CountCell label="p95 duration" value={p95} note={coverageNote} testid="stat-p95" class="max-lg:border-t" />
	</section>

	<section class="region mb-10 pt-3" aria-labelledby="dashboard-running-heading">
		<h2 id="dashboard-running-heading" class="mb-3 text-md font-semibold text-ink">Running now</h2>
		{#if running.length === 0}
			<p class="text-sm text-ink-muted" data-testid="running-empty">Nothing is running in {shown}.</p>
		{:else}
			<div class="overflow-x-auto">
				<table class="register min-w-[560px]" data-testid="running-list">
					<thead>
						<tr>
							<th scope="col">Run</th>
							<th scope="col">Orkflow</th>
							<th scope="col">Started</th>
							<th scope="col">Status</th>
						</tr>
					</thead>
					<tbody>
						{#each running as run (run.id)}
							<tr class="transition-colors hover:bg-hover" data-testid="running-{run.id}">
								<td><a class="link font-mono text-sm" href={withNamespace(`/runs/${run.id}`, data.namespace)}>{run.id}</a></td>
								<td class="text-ink-secondary">{run.orkflowName}{#if run.orkflowVersion}<span class="font-mono">@{run.orkflowVersion}</span>{/if}</td>
								<td class="figure text-xs text-ink-secondary">{fmtTimestamp(run.startedAt ?? run.createdAt)}</td>
								<td><StatusPill status={run.status} testid="running-status-{run.id}" /></td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</section>

	<section class="region mb-10 pt-3" aria-labelledby="dashboard-failures-heading">
		<h2 id="dashboard-failures-heading" class="text-md font-semibold text-ink">Recent failures</h2>
		<p class="mt-1 mb-3 text-sm text-ink-muted">Orkflows whose runs failed in this window, worst first.</p>
		{#if failures.length === 0}
			<p class="text-sm text-ink-muted" data-testid="failures-empty">No run failed in this window.</p>
		{:else}
			<div class="overflow-x-auto">
				<table class="register min-w-[420px]" data-testid="failure-table">
					<thead>
						<tr>
							<th scope="col">Orkflow</th>
							<th scope="col" class="text-right">Failed</th>
							<th scope="col" class="text-right">Finished</th>
						</tr>
					</thead>
					<tbody>
						{#each failures as failure (failure.orkflowName)}
							<tr class="transition-colors hover:bg-hover" data-testid="failure-{failure.orkflowName}">
								<td>
									<a
										class="link font-semibold"
										href={listHref(
											'/runs',
											{ ns: namespaceLabel(data.namespace) },
											{ filter: runWindowFilters(failure.orkflowName, '', '', '') }
										)}>{failure.orkflowName}</a
									>
								</td>
								<td class="figure text-right font-semibold text-red">{failure.failed.toString()}</td>
								<td class="figure text-right text-ink-secondary">{failure.total.toString()}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
			<!-- The same failed runs, grouped by what failed and why. Fetched
				only when opened; absent when nothing failed, because there is
				nothing to fingerprint. -->
			<FailureFingerprints
				namespace={data.namespace}
				window={data.window}
				sinceParam={data.sinceParam}
				computedAt={stats.computedAt}
			/>
		{/if}
	</section>

	<section class="region mb-10 pt-3" aria-labelledby="dashboard-pools-heading">
		<h2 id="dashboard-pools-heading" class="text-md font-semibold text-ink">Pool health</h2>
		<p class="mt-1 mb-3 text-sm text-ink-muted">
			Where a task of this namespace can run, and how many Orkers are serving each pool right now.
		</p>
		{#if pools.length === 0}
			<p class="text-sm text-ink-muted" data-testid="pools-empty">No pool could be read for {shown}.</p>
		{:else}
			<div class="overflow-x-auto">
				<table class="register min-w-[480px]" data-testid="pool-list">
					<thead>
						<tr>
							<th scope="col">Pool</th>
							<th scope="col">Orkers</th>
							<th scope="col">Serving</th>
						</tr>
					</thead>
					<tbody>
						{#each pools as pool (pool.name)}
							<tr data-testid="pool-{pool.name}" class={pool.liveOrkerCount > 0 ? '' : 'bg-red-tint'}>
								<td class="font-mono text-ink">
									{pool.name}
									{#if pool.isDefault}<span class="stamp ml-2 border-rule-strong text-ink-secondary" data-testid="pool-default-{pool.name}">default</span>{/if}
								</td>
								<td>
									<span data-testid="pool-orkers-{pool.name}" class="figure {pool.liveOrkerCount > 0 ? 'text-ink' : 'font-semibold text-red-strong'}"
										>{pool.liveOrkerCount}
										{pool.liveOrkerCount === 1 ? 'Orker' : 'Orkers'}</span
									>
								</td>
								<!-- Driven by the live Orker COUNT: an activity is an image,
									so a pool carries no inventory of what it can run. -->
								<td class="text-sm {pool.liveOrkerCount > 0 ? 'text-ink-secondary' : 'severity-warning'}" data-testid="pool-serving-{pool.name}">
									{#if pool.liveOrkerCount > 0}
										serving now
									{:else}
										no Orker is serving this pool, so a task sent here would wait
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	</section>

	<!-- The retries the success rate hides: a SUCCEEDED run that needed a
		second attempt counts as a success in every count above. Fetched only
		when opened, and rendered whenever the page has numbers — a namespace
		that dispatched nothing says so inside it. -->
	<section class="mb-10">
		<FlakeIndex
			namespace={data.namespace}
			window={data.window}
			sinceParam={data.sinceParam}
			computedAt={stats.computedAt}
		/>
	</section>

	<!-- What the namespace RESERVED — and, with a budget, against it. Its
		own request on its own timer: the register asks the budget's period
		when there is one, which is a different question from this page's
		window. -->
	<UsageCard
		namespace={data.namespace}
		budget={data.namespaceBudget}
		window={data.window}
		sinceParam={data.sinceParam}
	/>
{/if}
