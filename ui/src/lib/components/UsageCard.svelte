<script lang="ts">
	// "How much of the fleet has this namespace reserved — and against what
	// budget?" — `orkctl stats usage` in a browser.
	//
	// With a budget the card asks the gate's own question: the trailing
	// budget period (lookback = period), so the answer's budget_window is true
	// and the bars are shares the admission gate would act on. Without one it
	// asks about the dashboard's window, and says `unbounded` in place of the
	// bars — never an empty bar.
	//
	// It refetches on its own 60 s timer, never through invalidate('ork:stats'):
	// the dashboard's 5 s refresh would otherwise re-ask a measurement the
	// server caches for minutes (ORK_USAGE_CACHE_TTL).
	import { pollWhileVisible } from '$lib/visibility';
	import { durationFromMs, timestampFromDate, type Duration } from '@bufbuild/protobuf/wkt';
	import { errorMessage, orkstack } from '$lib/client';
	import type { Budget } from '$lib/gen/orkstack/v1/namespace_pb';
	import type { GetUsageResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import { namespaceLabel } from '$lib/namespace';
	import { fmtAge, groupBySpelling } from '$lib/stats';
	import {
		USAGE_DIMENSIONS,
		USAGE_REFRESH_MS,
		budgetModeLabel,
		budgetShareCells,
		fmtGoDuration,
		fmtUnitSeconds,
		shareLabel,
		shareTone,
		usageCell,
		usageCli,
		usageFooter
	} from '$lib/usage';
	import CliEquivalent from './CliEquivalent.svelte';
	import ErrorPanel from './ErrorPanel.svelte';
	import StatCard from './StatCard.svelte';

	interface Props {
		namespace: string;
		/** The namespace's budget, from the layout's snapshot; null = unbounded. */
		budget: Budget | null;
		/** The dashboard's window: a preset, or 'custom' with sinceParam. */
		window: string;
		sinceParam: string;
	}

	let { namespace, budget, window, sinceParam }: Props = $props();

	const PRESET_MS: Record<string, number> = { '15m': 15 * 60_000, '6h': 6 * 60 * 60_000, '24h': 24 * 60 * 60_000 };
	const period = $derived<Duration | undefined>(budget?.period);
	function windowFields() {
		if (period) return { lookback: period };
		const since = sinceParam ? new Date(sinceParam) : null;
		if (window === 'custom' && since && !Number.isNaN(since.getTime())) return { since: timestampFromDate(since) };
		return { lookback: durationFromMs(PRESET_MS[window] ?? PRESET_MS['24h']) };
	}
	const windowWord = $derived(
		period ? fmtGoDuration(period) : window === 'custom' && sinceParam ? sinceParam : window
	);
	const heading = $derived(period ? `Usage — last ${fmtGoDuration(period)} (the budget's period)` : 'Usage — this window');

	let checked = $state<Record<number, boolean>>(Object.fromEntries(USAGE_DIMENSIONS.map((dim) => [dim, false])));
	const groupBy = $derived(USAGE_DIMENSIONS.filter((dim) => checked[dim]));

	let result = $state<GetUsageResponse | null>(null);
	let error = $state<string | null>(null);
	let loading = $state(false);
	let now = $state(Date.now());
	let generation = 0;

	async function load() {
		const mine = ++generation;
		loading = true;
		try {
			const resp = await orkstack(fetch).stats.getUsage({ namespace, ...windowFields(), groupBy });
			if (mine !== generation) return;
			result = resp;
			error = null;
		} catch (err) {
			if (mine !== generation) return;
			// Never an empty card: the server's sentence, verbatim.
			result = null;
			error = errorMessage(err);
		} finally {
			if (mine === generation) loading = false;
		}
	}

	// On mount, when the namespace, the window, the budget or a chip changes,
	// and every USAGE_REFRESH_MS on this card's own timer.
	$effect(() => {
		void namespace;
		void window;
		void sinceParam;
		void period;
		void groupBy;
		void load();
		// The refetch and the clock tick run only while the tab is visible
		// ($lib/visibility); returning refreshes both at once.
		return pollWhileVisible(() => {
			now = Date.now();
			void load();
		}, USAGE_REFRESH_MS);
	});
	$effect(() => pollWhileVisible(() => (now = Date.now()), 5000));

	const shares = $derived(result ? budgetShareCells(result) : []);
	const footer = $derived(result ? usageFooter(result) : []);
	const cli = $derived(usageCli(namespaceLabel(namespace), windowWord, groupBy));
	const toneClass: Record<string, string> = {
		danger: 'bg-danger-500',
		warning: 'bg-warning-600',
		ok: 'bg-brand-600'
	};
</script>

<section class="rounded-lg border border-border bg-surface shadow-xs" data-testid="usage">
	<div class="flex flex-wrap items-center gap-3 border-b border-border-subtle px-4 py-3">
		<h2 class="text-sm font-semibold text-fg" data-testid="usage-heading">{heading}</h2>
		{#if loading}
			<span class="text-xs text-fg-muted" data-testid="usage-loading">loading…</span>
		{/if}
		{#if result}
			<span class="text-xs text-fg-muted" data-testid="usage-computed-at">measured {fmtAge(result.computedAt, now)}</span>
		{/if}
		<div class="ml-auto"><CliEquivalent command={cli} testid="usage-cli" /></div>
	</div>
	<div class="px-4 py-3">
		{#if error}
			<ErrorPanel
				testid="usage-error"
				retryTestid="usage-retry"
				subject="the usage of namespace {namespaceLabel(namespace)}"
				message={error}
				busy={loading}
				onretry={() => void load()}
			/>
		{:else if result}
			<div class="mb-4" data-testid="usage-budget">
				{#if result.budget}
					<p class="mb-2 text-xs text-fg-secondary" data-testid="usage-budget-mode">{budgetModeLabel(result.budget)}</p>
					{#if shares.length === 0}
						<p class="text-xs text-fg-muted" data-testid="usage-budget-window-differs">
							This window is not the budget's; the budget is held against the last {fmtGoDuration(result.budget.period)}.
						</p>
					{/if}
					{#each shares as cell (cell.dim)}
						{@const tone = shareTone(cell, result.budgetReached)}
						<div class="mb-2">
							<div class="mb-1 flex items-center justify-between text-xs">
								<span class="font-medium text-fg">{cell.unit}</span>
								<span class="text-fg-secondary tabular-nums" data-testid="usage-bar-{cell.dim}-label">{shareLabel(cell)}</span>
							</div>
							<div
								class="h-2 w-full overflow-hidden rounded-full bg-surface-sunken"
								role="meter"
								aria-label="{cell.unit} reserved against the budget"
								aria-valuemin="0"
								aria-valuemax={cell.bound}
								aria-valuenow={Math.round(cell.used)}
								data-testid="usage-bar-{cell.dim}"
								data-tone={tone}
							>
								<div class="h-full rounded-full {toneClass[tone]}" style="width: {Math.min(1, cell.ratio) * 100}%"></div>
							</div>
						</div>
					{/each}
				{:else}
					<p class="text-sm font-medium text-fg-secondary" data-testid="usage-unbounded">unbounded</p>
				{/if}
			</div>

			<div class="mb-4 grid grid-cols-1 gap-4 sm:grid-cols-3">
				<StatCard label="Task-seconds" value={fmtUnitSeconds(result.total?.taskSeconds ?? 0)} testid="usage-task-seconds" />
				<StatCard label="cpu-seconds (reserved)" value={fmtUnitSeconds(result.total?.cpuSeconds ?? 0)} testid="usage-cpu-seconds" />
				<StatCard label="GiB-seconds (reserved)" value={fmtUnitSeconds(result.total?.gibSeconds ?? 0)} testid="usage-gib-seconds" />
			</div>

			<div class="mb-3 flex flex-wrap items-center gap-2">
				<span class="text-xs font-medium text-fg-muted">Group by</span>
				{#each USAGE_DIMENSIONS as dim (dim)}
					<label
						class="inline-flex h-7 cursor-pointer items-center gap-1.5 rounded-full border px-2.5 text-xs font-medium transition-colors duration-150 {checked[dim]
							? 'border-brand-200 bg-brand-50 text-brand-700'
							: 'border-border bg-surface text-fg-muted hover:bg-surface-hover'}"
					>
						<input type="checkbox" class="size-3.5 accent-brand-600" data-testid="usage-dim-{groupBySpelling(dim)}" bind:checked={checked[dim]} />
						{groupBySpelling(dim)}
					</label>
				{/each}
			</div>

			{#if result.groupBy.length > 0}
				{#if result.rows.length === 0}
					<p class="mb-3 text-sm text-fg-muted" data-testid="usage-empty">No task was dispatched in this window.</p>
				{:else}
					<div class="mb-3 overflow-x-auto rounded-md border border-border">
						<table class="w-full border-collapse text-xs" data-testid="usage-table">
							<thead class="bg-surface-sunken">
								<tr>
									{#each result.groupBy as dim (dim)}
										<th class="h-8 px-3 text-left font-medium text-fg-muted">{groupBySpelling(dim)}</th>
									{/each}
									<th class="h-8 px-3 text-right font-medium text-fg-muted">runs</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">tasks</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">attempts</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">task-seconds</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">cpu-seconds</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">GiB-seconds</th>
								</tr>
							</thead>
							<tbody>
								{#each result.rows as row, i (i)}
									<tr class="border-t border-border-subtle" data-testid="usage-row-{i}">
										{#each result.groupBy as dim (dim)}
											<td class="px-3 py-2 text-fg">{usageCell(row, dim)}</td>
										{/each}
										<td class="px-3 py-2 text-right tabular-nums">{row.runs.toString()}</td>
										<td class="px-3 py-2 text-right tabular-nums">{row.tasks.toString()}</td>
										<td class="px-3 py-2 text-right tabular-nums">{row.attempts.toString()}</td>
										<td class="px-3 py-2 text-right tabular-nums">{fmtUnitSeconds(row.taskSeconds)}</td>
										<td class="px-3 py-2 text-right tabular-nums">{fmtUnitSeconds(row.cpuSeconds)}</td>
										<td class="px-3 py-2 text-right tabular-nums">{fmtUnitSeconds(row.gibSeconds)}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{/if}

			<div class="space-y-0.5 text-xs text-fg-muted" data-testid="usage-footer">
				{#each footer as line, i (i)}
					<p data-testid={i === 0 ? 'usage-caveat' : `usage-footer-${i}`} class={i === 0 ? 'font-medium text-fg-secondary' : ''}>{line}</p>
				{/each}
			</div>
		{/if}
	</div>
</section>
