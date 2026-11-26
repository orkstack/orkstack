<script lang="ts">
	// Operate → Concurrency: a namespace's concurrency limits as they stand
	// (E88, design §9.4). The list is `orkctl concurrency list`; a key's own
	// page (?key=) is `orkctl concurrency get`: who holds its slots, earliest
	// granted first, and who waits, in the order they start. Both are read from
	// the limit itself when the page loads, and nothing here is a stored count:
	// Refresh asks again.
	//
	// Three things are kept apart: a limit that does not answer (its row says
	// why and has no counts), a limit reading which runs hold it (its counts
	// may still grow), and a key nothing holds or waits for, which is an answer
	// and not a failure. No write exists on this page.
	import { goto, invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import Pagination from '#lib/components/Pagination.svelte';
	import ScopeNote from '#lib/components/ScopeNote.svelte';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import { listHref, readPageStack, writePageStack } from '#lib/browsing.ts';
	import {
		concurrencyGetCli,
		concurrencyHref,
		concurrencyListCli,
		concurrencyStackKey,
		entryLevel,
		holderNote,
		limitState,
		shownOf,
		strategyName
	} from '#lib/concurrencyLimits.ts';
	import { limitHref } from '#lib/concurrency.ts';
	import { instantSeconds } from '#lib/deliveries.ts';
	import type { ConcurrencyLimit } from '#lib/gen/orkstack/v1/concurrency_service_pb.ts';
	import { ListConcurrencyLimitsRequestSchema } from '#lib/gen/orkstack/v1/concurrency_service_pb.ts';
	import { FilterOperator, filterOf, type FieldFilter } from '#lib/listFilter.ts';
	import { namespaceLabel, withNamespace } from '#lib/namespace.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const c = $derived(data.coordinates);
	const ns = $derived(namespaceLabel(data.namespace));
	const limits = $derived(data.list?.limits ?? []);
	const runHref = (id: string) => withNamespace(`/runs/${encodeURIComponent(id)}`, data.namespace);
	// Every run of the key, where the page shows only the first ones.
	const runsHref = (field: 'holding' | 'queued_on') =>
		listHref('/runs', { ns }, { filter: [filterOf(field, FilterOperator.EQUALS, [data.key])] });

	function applyFilters(filters: FieldFilter[]) {
		void goto(concurrencyHref(c, { filters, cursor: '', page: 1 }), { reset: false });
	}
	const clearHref = $derived(concurrencyHref(c, { filters: [], cursor: '', page: 1 }));

	let refreshing = $state(false);
	async function refresh() {
		refreshing = true;
		try {
			await invalidateAll();
		} finally {
			refreshing = false;
		}
	}
	// Another namespace, key or page of this route on its way: what is on
	// screen is not it.
	const loading = $derived(navigating.to !== null && navigating.to.url.pathname === page.url.pathname);

	// The cursors behind this page, per filter set, per tab (lib/browsing.ts).
	const stackKey = $derived(concurrencyStackKey(c));
	let stack = $state<string[]>([]);
	$effect(() => {
		stack = readPageStack(stackKey);
	});
	function next() {
		const ahead = Array.from({ length: Math.max(stack.length, c.page + 1) }, (_, at) => stack[at] ?? '');
		[ahead[c.page - 1], ahead[c.page]] = [c.cursor, data.list?.nextPageToken ?? ''];
		writePageStack(stackKey, ahead);
		stack = ahead;
	}
	const walked = $derived(c.page > 1 && stack[c.page - 1] === c.cursor);
	const previousHref = $derived(walked ? concurrencyHref(c, { cursor: stack[c.page - 2] ?? '', page: c.page - 1 }) : undefined);
	const nextHref = $derived(data.list?.nextPageToken ? concurrencyHref(c, { cursor: data.list.nextPageToken, page: c.page + 1 }) : undefined);
	const firstHref = $derived(concurrencyHref(c, { cursor: '', page: 1 }));
</script>

<svelte:head><title>{data.key ? `${data.key} — ` : ''}Admin — Concurrency — OrkStack</title></svelte:head>

{#snippet actions()}
	<button type="button" class="btn" data-testid="concurrency-refresh" disabled={refreshing || loading} onclick={() => void refresh()}>Refresh</button>
{/snippet}

{#snippet statePill(limit: ConcurrencyLimit)}
	{@const st = limitState(limit)}
	<span class="stamp {st.tone === 'ok' ? 'bg-ok-tint text-ok mark-square' : st.tone === 'fail' ? 'bg-fail-tint text-fail mark-tri' : 'pill-wait'}"
		>{#if st.tone === 'wait'}<span class="mark-wait" aria-hidden="true"></span>{/if}{st.word}</span
	>{#if st.detail}<span class="mt-1 block text-sm text-ink-2" data-testid="concurrency-state-detail">{st.detail}</span>{/if}
{/snippet}

{#if data.key}
	<SheetHeader title={data.key} testid="concurrency-key" mono {actions} commands={[{ label: 'This key', command: concurrencyGetCli(data.namespace, data.key) }]} />
	{#if loading}
		<p class="text-sm text-ink-muted" data-testid="concurrency-loading">Reading the limit…</p>
	{:else if data.error}
		<ErrorPanel testid="concurrency-error" retryTestid="concurrency-retry" subject="this key's limit" message={data.error} busy={refreshing} onretry={refresh} />
	{:else if !data.limit}
		<p class="text-sm text-ink-2" data-testid="concurrency-unused">
			No run holds or waits for this key now. <a class="link" href={concurrencyHref(c, { cursor: '', page: 1 })}>Every key in use</a>
		</p>
	{:else}
		{@const limit = data.limit}
		<dl class="fstrip mb-5" data-testid="concurrency-facts">
			<div><dt>State</dt><dd data-testid="concurrency-state">{@render statePill(limit)}</dd></div>
			{#if limit.answering}
				<div><dt>Holding</dt><dd class="num" data-testid="concurrency-holders-count">{limit.holdersCount}</dd></div>
				<div><dt>Waiting</dt><dd class="num" data-testid="concurrency-queued-count">{limit.queuedCount}</dd></div>
				<div><dt>Epoch</dt><dd><span class="num" data-testid="concurrency-epoch">{limit.epoch}</span> <span class="text-sm text-ink-3">{limit.epoch === 1n ? 'operator reset' : 'operator resets'}</span></dd></div>
			{/if}
		</dl>
		{#if !limit.answering}
			<p class="text-sm text-ink-2" data-testid="concurrency-unknown">Who holds this key and who waits for it is unknown, not nobody. Refresh asks the limit again.</p>
		{:else}
			{@const moreHolders = shownOf(limit.holders.length, limit.holdersCount, 'holding')}
			{@const moreWaiting = shownOf(limit.line.length, limit.queuedCount, 'waiting')}
			<section class="card" aria-labelledby="concurrency-holding-h">
				<div class="card-head">
					<h2 id="concurrency-holding-h">Holding</h2>
					<span class="card-sub">earliest granted first</span>
				</div>
				{#if limit.holders.length === 0}
					<p class="px-4 py-3 text-sm text-ink-3" data-testid="concurrency-holders-empty">No run holds a slot now.</p>
				{:else}
					<div class="relative overflow-x-auto scroll-panel">
						<table class="register register-stacked register-pairs" data-testid="concurrency-holders" aria-label="Runs holding a slot">
							<thead class="max-sm:sr-only">
								<tr><th scope="col">Run</th><th scope="col">Level</th><th scope="col">Since</th><th scope="col">Limit</th><th scope="col">On limit</th></tr>
							</thead>
							<tbody>
								{#each limit.holders as h, i (i)}
									<tr data-testid="concurrency-holder">
										<td class="max-sm:col-span-2"
											><a class="font-mono text-sm break-all" href={runHref(h.runId)}>{h.runId}</a>{#if h.stalled}<span class="severity-warning block text-sm" data-testid="concurrency-holder-stalled">{holderNote(h)}</span>{/if}</td
										>
										<td data-label="Level">{entryLevel(h.taskLevel)}</td>
										<td data-label="Since"><TimeAgo at={h.grantedAt} /></td>
										<td data-label="Limit" class="num">{h.limit}</td>
										<td data-label="On limit">{strategyName(h.strategy)}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
				{#if moreHolders}<p class="px-4 py-3 text-sm text-ink-2" data-testid="concurrency-holders-more">{moreHolders}. <a class="link" href={runsHref('holding')}>Runs holding this key</a></p>{/if}
			</section>
			<section class="card mt-4" aria-labelledby="concurrency-waiting-h">
				<div class="card-head">
					<h2 id="concurrency-waiting-h">Waiting</h2>
					<span class="card-sub">in the order they start</span>
				</div>
				{#if limit.line.length === 0}
					<p class="px-4 py-3 text-sm text-ink-3" data-testid="concurrency-line-empty">No run waits for a slot now.</p>
				{:else}
					<div class="relative overflow-x-auto scroll-panel">
						<table class="register register-stacked register-pairs" data-testid="concurrency-line" aria-label="Runs waiting for a slot, in the order they start">
							<thead class="max-sm:sr-only">
								<tr><th scope="col">Run</th><th scope="col">Place</th><th scope="col">Level</th><th scope="col">Since</th><th scope="col">Times out</th><th scope="col">Limit</th></tr>
							</thead>
							<tbody>
								{#each limit.line as e, i (i)}
									<tr data-testid="concurrency-waiting">
										<td class="max-sm:col-span-2"><a class="font-mono text-sm break-all" href={runHref(e.runId)}>{e.runId}</a></td>
										<td data-label="Place" class="num">{i + 1}</td>
										<td data-label="Level">{entryLevel(e.taskLevel)}</td>
										<td data-label="Since"><TimeAgo at={e.arrivedAt} /></td>
										<td data-label="Times out" class="max-sm:col-span-2"><span class="num whitespace-nowrap">{instantSeconds(e.timesOutAt) || '-'}</span></td>
										<td data-label="Limit" class="num">{e.limit}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
				{#if moreWaiting}<p class="px-4 py-3 text-sm text-ink-2" data-testid="concurrency-line-more">{moreWaiting}. <a class="link" href={runsHref('queued_on')}>Runs waiting for this key</a></p>{/if}
			</section>
		{/if}
	{/if}
{:else}
	<SheetHeader title="Concurrency" {actions} commands={[{ label: 'The keys in use', command: concurrencyListCli(data.namespace, c.filters) }]} />
	<ScopeNote testid="concurrency-scope">The keys runs hold or wait for, read from each limit when this page loads.</ScopeNote>
	<div class="mb-3">
		<ListFilter request={ListConcurrencyLimitsRequestSchema} applied={c.filters} testid="concurrency" onapply={applyFilters} {clearHref} />
	</div>
	{#if loading}
		<p class="text-sm text-ink-muted" data-testid="concurrency-loading">Reading the limits…</p>
	{:else if data.error}
		<ErrorPanel testid="concurrency-error" retryTestid="concurrency-retry" subject="the concurrency limits" message={data.error} busy={refreshing} onretry={refresh} />
	{:else if limits.length === 0}
		<p class="text-sm text-ink-2" data-testid="concurrency-empty">
			{#if c.filters.length > 0}No key {nextHref || c.page > 1 ? 'on this page ' : ''}matches these filters.{:else if c.page > 1 || nextHref}No key on this page is in use.{:else}No key has runs holding or waiting.{/if}
		</p>
		{#if c.page > 1 || nextHref}
			<Pagination testid="concurrency" noun="key" page={c.page} count={0} {previousHref} {nextHref} {firstHref} onnext={next} />
		{/if}
	{:else}
		<div class="card">
			<div class="relative overflow-x-auto scroll-panel">
				<table class="register register-stacked register-pairs" data-testid="concurrency-table" aria-label="Concurrency keys in use">
					<thead class="max-sm:sr-only">
						<tr><th scope="col">Key</th><th scope="col">Holding</th><th scope="col">Waiting</th><th scope="col" class="w-80">State</th></tr>
					</thead>
					<tbody>
						{#each limits as limit, i (i)}
							<tr data-testid="concurrency-row" data-key={limit.key}>
								<td class="max-sm:col-span-2"
									>{#if limit.key}<a class="font-mono text-sm break-all" href={limitHref(data.namespace, limit.key)}>{limit.key}</a>{:else}<span class="text-ink-3">Key not known</span>{/if}</td
								>
								<td data-label="Holding" class="num">{limit.answering ? limit.holdersCount : '-'}</td>
								<td data-label="Waiting" class="num">{limit.answering ? limit.queuedCount : '-'}</td>
								<td data-label="State" class="max-sm:col-span-2" data-testid="concurrency-row-state">{@render statePill(limit)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</div>
		<Pagination testid="concurrency" noun="key" page={c.page} count={limits.length} {previousHref} {nextHref} {firstHref} onnext={next} />
	{/if}
{/if}
