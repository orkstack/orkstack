<script lang="ts">
	// "Which tasks only succeed because they are tried again?" — the question
	// the success rate cannot answer, because a task that failed twice and
	// passed on its third attempt is a SUCCEEDED run.
	//
	// It is a disclosure, fetched on demand: GetFlakeIndex reads every finished
	// run's body in the window and one attempt listing per dispatched task
	// (docs/STATS.md §7), so a reader who never opens it never pays for it, and
	// the dashboard itself stays one GetStats call. It asks about the SAME
	// window the page asked about, so its runs are the page's finished runs.
	//
	// It renders exactly what `orkctl stats flakes` renders from the same
	// response (cmd/orkctl/stats_flakes.go): the helpers in $lib/flakes are
	// shared spellings, not a second opinion.
	import { durationFromMs, timestampFromDate, type Timestamp } from '@bufbuild/protobuf/wkt';
	import { errorMessage, orkstack } from '$lib/client';
	import { listHref } from '$lib/browsing';
	import { namespaceLabel } from '$lib/namespace';
	import { groupBySpelling, timestampMs } from '$lib/stats';
	import {
		DEFAULT_FLAKE_DIMENSIONS,
		FLAKE_DIMENSIONS,
		dominantCauseLabel,
		flakeCell,
		flakeFooter,
		flakeGroupBy,
		flakesCli,
		fmtAttemptsPerRun,
		fmtRetryRate
	} from '$lib/flakes';
	import { FailureGroupBy, type GetFlakeIndexResponse } from '$lib/gen/orkstack/v1/stats_pb';
	import CliEquivalent from './CliEquivalent.svelte';
	import ErrorPanel from './ErrorPanel.svelte';

	interface Props {
		/** The namespace the page asked about, as the loader holds it. */
		namespace: string;
		/** The loader's window: a preset, or 'custom' with sinceParam. */
		window: string;
		sinceParam: string;
		/** The page's stats.computedAt: a new measurement is a reason to refetch. */
		computedAt: Timestamp | undefined;
	}

	let { namespace, window, sinceParam, computedAt }: Props = $props();

	// The open state is a per-viewer convenience, nothing more: a browser that
	// refuses storage still gets a working disclosure, closed.
	const STORAGE_KEY = 'orkstack.dashboard.stability';
	let open = $state(false);
	$effect(() => {
		try {
			open = globalThis.localStorage?.getItem(STORAGE_KEY) === 'open';
		} catch {
			// Storage is blocked; the default stands.
		}
	});
	function toggled(event: Event) {
		open = (event.currentTarget as HTMLDetailsElement).open;
		try {
			globalThis.localStorage?.setItem(STORAGE_KEY, open ? 'open' : 'closed');
		} catch {
			// Storage is blocked; the state lives for this page only.
		}
	}

	let checked = $state<Record<number, boolean>>(
		Object.fromEntries(FLAKE_DIMENSIONS.map((dim) => [dim, DEFAULT_FLAKE_DIMENSIONS.includes(dim)]))
	);
	const groupBy = $derived(flakeGroupBy(FLAKE_DIMENSIONS.filter((dim) => checked[dim])));

	let result = $state<GetFlakeIndexResponse | null>(null);
	let error = $state<string | null>(null);
	let loading = $state(false);
	let generation = 0;

	// The same window resolution +page.ts uses for GetStats, so both answers
	// are about one population: a custom window travels as `since`, a preset
	// as a lookback the server's clock resolves.
	const PRESET_MS: Record<string, number> = { '15m': 15 * 60_000, '6h': 6 * 60 * 60_000, '24h': 24 * 60 * 60_000 };
	function windowFields() {
		const since = sinceParam ? new Date(sinceParam) : null;
		if (window === 'custom' && since && !Number.isNaN(since.getTime())) return { since: timestampFromDate(since) };
		return { lookback: durationFromMs(PRESET_MS[window] ?? PRESET_MS['24h']) };
	}

	async function load(dims: FailureGroupBy[]) {
		const mine = ++generation;
		loading = true;
		try {
			const resp = await orkstack(fetch).stats.getFlakeIndex({ namespace, ...windowFields(), groupBy: dims });
			if (mine !== generation) return;
			result = resp;
			error = null;
		} catch (err) {
			if (mine !== generation) return;
			// Never a silently empty table: an answer that could not be had says
			// so — including an engine with no attempt store, whose own sentence
			// is the message.
			result = null;
			error = errorMessage(err);
		} finally {
			if (mine === generation) loading = false;
		}
	}

	// Refetch on open, on a chip, and on every new page measurement while open.
	$effect(() => {
		const dims = groupBy;
		void timestampMs(computedAt);
		void namespace;
		void window;
		void sinceParam;
		if (!open) return;
		void load(dims);
	});

	function iso(ts: Timestamp | undefined): string {
		const ms = timestampMs(ts);
		return ms === null ? '' : new Date(ms).toISOString();
	}

	// The runs list has no task filter, so a row links to what it has: the
	// namespace, the orkflow, and the exact window the index answered — and no
	// status, because the retried runs of a flaky task mostly SUCCEEDED.
	function rowHref(orkflowName: string): string {
		return listHref('/runs', {
			ns: namespaceLabel(namespace),
			orkflow: orkflowName,
			since: iso(result?.windowSince),
			until: iso(result?.windowUntil)
		});
	}

	const columns = $derived(result?.groupBy.length ? result.groupBy : [FailureGroupBy.ORKFLOW, ...groupBy]);
	const footer = $derived(result ? flakeFooter(result) : []);
	const cli = $derived(
		flakesCli(namespaceLabel(namespace), window === 'custom' && sinceParam ? sinceParam : window, groupBy)
	);
</script>

<details
	class="rounded-lg border border-border bg-surface shadow-xs"
	data-testid="stability"
	{open}
	ontoggle={toggled}
>
	<summary
		data-testid="stability-toggle"
		class="cursor-pointer px-4 py-3 text-sm font-medium text-fg select-none hover:bg-surface-hover"
	>
		Stability — the retries the success rate hides
	</summary>
	{#if open}
		<div class="border-t border-border-subtle px-4 py-3">
			<div class="mb-3 flex flex-wrap items-center gap-2">
				<span class="text-xs font-medium text-fg-muted">Group by</span>
				{#each FLAKE_DIMENSIONS as dim (dim)}
					<label
						class="inline-flex h-7 cursor-pointer items-center gap-1.5 rounded-full border px-2.5 text-xs font-medium transition-colors duration-150 {checked[
							dim
						]
							? 'border-brand-200 bg-brand-50 text-brand-700'
							: 'border-border bg-surface text-fg-muted hover:bg-surface-hover'}"
					>
						<input
							type="checkbox"
							class="size-3.5 accent-brand-600"
							data-testid="stability-dim-{groupBySpelling(dim)}"
							bind:checked={checked[dim]}
						/>
						{groupBySpelling(dim)}
					</label>
				{/each}
				<div class="ml-auto flex items-center gap-2">
					{#if loading}
						<span class="text-xs text-fg-muted" data-testid="stability-loading">loading…</span>
					{/if}
					<CliEquivalent command={cli} testid="stability-cli" />
				</div>
			</div>

			{#if error}
				<ErrorPanel
					testid="stability-error"
					retryTestid="stability-retry"
					subject="the stability of namespace {namespaceLabel(namespace)}"
					message={error}
					busy={loading}
					onretry={() => void load(groupBy)}
				/>
			{:else if result}
				{#if result.groups.length === 0}
					<p class="text-sm text-fg-muted" data-testid="stability-empty">
						No task was dispatched in this window.
					</p>
				{:else}
					<div class="overflow-x-auto rounded-md border border-border">
						<table class="w-full border-collapse text-xs" data-testid="stability-table">
							<thead class="bg-surface-sunken">
								<tr>
									{#each columns as dim (dim)}
										<th class="h-8 px-3 text-left font-medium text-fg-muted">{groupBySpelling(dim)}</th>
									{/each}
									<th class="h-8 px-3 text-right font-medium text-fg-muted">runs</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">retried</th>
									<th class="h-8 px-3 text-right font-medium text-fg-muted">rate</th>
									<th class="h-8 px-3 text-left font-medium text-fg-muted">attempts/run</th>
									<th class="h-8 px-3 text-left font-medium text-fg-muted">dominant cause</th>
								</tr>
							</thead>
							<tbody>
								{#each result.groups as group, i (i)}
									{@const href = rowHref(group.orkflowName)}
									{@const rate = fmtRetryRate(group)}
									{@const flaky = group.retryRateDefined && group.retryRate > 0}
									<tr
										class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
										data-testid="stability-row-{i}"
									>
										{#each columns as dim, c (dim)}
											<td class="align-middle">
												<a
													{href}
													tabindex={c === 0 ? undefined : -1}
													data-testid="stability-row-{i}-{groupBySpelling(dim)}"
													class="block px-3 py-2 text-fg {dim === FailureGroupBy.IMAGE ? 'font-mono break-all' : ''}"
													>{flakeCell(group, dim)}</a
												>
											</td>
										{/each}
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 text-right text-fg tabular-nums"
												>{group.runsObserved.toString()}</a
											>
										</td>
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 text-right text-fg tabular-nums"
												>{group.runsRetried.toString()}</a
											>
										</td>
										<td class="align-middle">
											<a
												{href}
												tabindex="-1"
												data-testid="stability-row-{i}-rate"
												data-unavailable={group.retryRateDefined ? 'false' : 'true'}
												class="block px-3 py-2 text-right whitespace-nowrap tabular-nums {!group.retryRateDefined
													? 'font-normal text-fg-muted italic'
													: flaky
														? 'font-medium text-warning-700'
														: 'text-fg'}"
											>
												{#if flaky}
													<span class="rounded-full border border-warning-200 bg-warning-50 px-2 py-0.5">{rate}</span>
												{:else}
													{rate}
												{/if}
											</a>
										</td>
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 whitespace-nowrap text-fg-muted"
												>{fmtAttemptsPerRun(group)}</a
											>
										</td>
										<td class="align-middle">
											<a
												{href}
												tabindex="-1"
												data-testid="stability-row-{i}-cause"
												class="block px-3 py-2 text-fg-muted">{dominantCauseLabel(group)}</a
											>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
				<div class="mt-2 space-y-0.5 text-xs text-fg-muted" data-testid="stability-footer">
					{#each footer as line (line)}
						<p>{line}</p>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</details>
