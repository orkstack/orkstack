<script lang="ts">
	// "Is this the same failure as the last eighteen, and is it mine or the
	// platform's?" — the question the Recent failures table stops short of.
	//
	// It is a disclosure, fetched on demand: GetFailureBreakdown is the one
	// stats read that opens run bodies (docs/STATS.md §6), so a reader who
	// never opens it never pays for it, and the dashboard itself stays one
	// GetStats call. It asks about the SAME window the page asked about, so
	// its runs are the page's failed runs, grouped.
	//
	// It renders exactly what `orkctl stats failures` renders from the same
	// response (cmd/orkctl/stats_failures.go): the helpers in $lib/stats are
	// shared spellings, not a second opinion.
	import { durationFromMs, timestampFromDate, type Timestamp } from '@bufbuild/protobuf/wkt';
	import { errorMessage } from '$lib/client';
	import { statsClient } from '$lib/clients/stats';
	import { listHref, runWindowFilters } from '$lib/browsing';
	import { namespaceLabel } from '$lib/namespace';
	import { shellQuote } from '$lib/runDetail';
	import {
		DEFAULT_FINGERPRINT,
		FINGERPRINT_DIMENSIONS,
		fingerprintCell,
		fingerprintFooter,
		fmtInstant,
		groupBySpelling,
		timestampMs
	} from '$lib/stats';
	import { FailureGroupBy, type GetFailureBreakdownResponse } from '$lib/gen/orkstack/v1/stats_pb';
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
	const STORAGE_KEY = 'orkstack.dashboard.fingerprints';
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
		Object.fromEntries(FINGERPRINT_DIMENSIONS.map((dim) => [dim, DEFAULT_FINGERPRINT.includes(dim)]))
	);
	// Enum order, whatever order the chips were ticked in: the columns and the
	// CLI's --group-by read the same way every time.
	const groupBy = $derived(FINGERPRINT_DIMENSIONS.filter((dim) => checked[dim]));
	const groupedByOrkflow = $derived(groupBy.includes(FailureGroupBy.ORKFLOW));

	let result = $state<GetFailureBreakdownResponse | null>(null);
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
			const resp = await statsClient(fetch).getFailureBreakdown({ namespace, ...windowFields(), groupBy: dims });
			if (mine !== generation) return;
			result = resp;
			error = null;
		} catch (err) {
			if (mine !== generation) return;
			// Never a silently empty table: an answer that could not be had says so.
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
		if (!open || dims.length === 0) return;
		void load(dims);
	});

	function iso(ts: Timestamp | undefined): string {
		const ms = timestampMs(ts);
		return ms === null ? '' : new Date(ms).toISOString();
	}

	// The runs list has no task, code or category filter, so a row links to
	// what it has: the namespace, the orkflow when it is a grouped dimension,
	// status failed, and the exact window the breakdown answered.
	function rowHref(orkflowName: string): string {
		return listHref(
			'/runs',
			{ ns: namespaceLabel(namespace) },
			{
				filter: runWindowFilters(
					groupedByOrkflow ? orkflowName : '',
					'FAILED',
					iso(result?.windowSince),
					iso(result?.windowUntil)
				)
			}
		);
	}

	const columns = $derived(result?.groupBy.length ? result.groupBy : groupBy);
	const footer = $derived(result ? fingerprintFooter(result) : []);
	const cli = $derived(
		`orkctl --namespace ${shellQuote(namespaceLabel(namespace))} stats failures --since ${shellQuote(window === 'custom' && sinceParam ? sinceParam : window)} --group-by ${groupBy.map(groupBySpelling).join(',')}`
	);
</script>

<details
	class="mt-4 rounded-lg border border-border bg-surface shadow-xs"
	data-testid="failure-fingerprints"
	{open}
	ontoggle={toggled}
>
	<summary
		data-testid="failure-fingerprints-toggle"
		class="cursor-pointer px-4 py-3 text-sm font-medium text-fg select-none hover:bg-surface-hover"
	>
		Fingerprints — the same failures, grouped
	</summary>
	{#if open}
		<div class="border-t border-border-subtle px-4 py-3">
			<div class="mb-3 flex flex-wrap items-center gap-2">
				<span class="text-xs font-medium text-fg-muted">Group by</span>
				{#each FINGERPRINT_DIMENSIONS as dim (dim)}
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
							data-testid="fingerprint-dim-{groupBySpelling(dim)}"
							bind:checked={checked[dim]}
						/>
						{groupBySpelling(dim)}
					</label>
				{/each}
				<div class="ml-auto flex items-center gap-2">
					{#if loading}
						<span class="text-xs text-fg-muted" data-testid="failure-fingerprints-loading">loading…</span>
					{/if}
					<CliEquivalent command={cli} testid="fingerprint-cli" />
				</div>
			</div>

			{#if groupBy.length === 0}
				<p class="text-sm text-fg-muted" data-testid="failure-fingerprints-none">
					Tick at least one dimension to group by.
				</p>
			{:else if error}
				<ErrorPanel
					testid="failure-fingerprints-error"
					retryTestid="failure-fingerprints-retry"
					subject="the failure fingerprints for namespace {namespaceLabel(namespace)}"
					message={error}
					busy={loading}
					onretry={() => void load(groupBy)}
				/>
			{:else if result}
				{#if result.groups.length === 0}
					<p class="text-sm text-fg-muted" data-testid="failure-fingerprints-empty">
						No run failed in this window.
					</p>
				{:else}
					<div class="overflow-x-auto rounded-md border border-border">
						<table class="w-full border-collapse text-xs" data-testid="fingerprint-table">
							<thead class="bg-surface-sunken">
								<tr>
									{#each columns as dim (dim)}
										<th class="h-8 px-3 text-left font-medium text-fg-muted">{groupBySpelling(dim)}</th>
									{/each}
									<th class="h-8 px-3 text-right font-medium text-fg-muted">runs</th>
									<th class="h-8 px-3 text-left font-medium text-fg-muted">first</th>
									<th class="h-8 px-3 text-left font-medium text-fg-muted">last</th>
								</tr>
							</thead>
							<tbody>
								{#each result.groups as group, i (i)}
									{@const href = rowHref(group.orkflowName)}
									<tr
										class="border-t border-border-subtle transition-colors duration-150 hover:bg-surface-hover"
										data-testid="fingerprint-row-{i}"
									>
										{#each columns as dim, c (dim)}
											{@const cell = fingerprintCell(group, dim)}
											<td class="align-middle">
												<a
													{href}
													tabindex={c === 0 ? undefined : -1}
													data-testid="fingerprint-cell-{i}-{groupBySpelling(dim)}"
													data-unavailable={cell.unavailable ? 'true' : 'false'}
													class="block px-3 py-2 {dim === FailureGroupBy.IMAGE ? 'font-mono break-all' : ''} {cell.unavailable
														? 'font-normal text-fg-muted italic'
														: 'text-fg'}">{cell.text}</a
												>
											</td>
										{/each}
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 text-right text-failed-fg tabular-nums"
												>{group.runCount.toString()}</a
											>
										</td>
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 whitespace-nowrap text-fg-muted"
												>{fmtInstant(group.firstSeen)}</a
											>
										</td>
										<td class="align-middle">
											<a {href} tabindex="-1" class="block px-3 py-2 whitespace-nowrap text-fg-muted"
												>{fmtInstant(group.lastSeen)}</a
											>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
				<div class="mt-2 space-y-0.5 text-xs text-fg-muted" data-testid="failure-fingerprints-footer">
					{#each footer as line (line)}
						<p>{line}</p>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</details>
