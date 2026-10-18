<script lang="ts">
	// Pool health: live Orker counts, narrowed rather than refused
	// (NamespaceService.ListPools). A pool this
	// read could not ask about is a THIRD state, distinct from both "healthy"
	// and "empty" — Pool.unavailable_reason — and it is rendered as its own
	// state rather than folded into a zero.
	//
	// What a deployment can run is on Admin → Activities; who is serving a
	// pool is the count below.
	import { goto, invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import ListFilter from '$lib/components/ListFilter.svelte';
	import { listHref } from '$lib/browsing';
	import type { FieldFilter } from '$lib/listFilter';
	import { shellQuote } from '$lib/runDetail';
	import { ListPoolsRequestSchema } from '$lib/gen/orkstack/v1/namespace_service_pb';
	import PoolOrkers from '$lib/components/PoolOrkers.svelte';
	import ScopeNote from '$lib/components/ScopeNote.svelte';
	import SheetHeader from '$lib/components/SheetHeader.svelte';
	import type { Pool } from '$lib/gen/orkstack/v1/namespace_pb';
	import { namespaceLabel } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// The filters narrow the pool cards (ListFilter; docs/UI_CLI_PARITY.md
	// "Filtering lists"); `ns` stays, because it scopes this view.
	function applyFilters(filters: FieldFilter[]) {
		void goto(listHref('/admin/pools', { ns: namespaceLabel(data.namespace) }, { filter: filters }), { keepFocus: true, noScroll: true });
	}
	const clearHref = $derived(listHref('/admin/pools', { ns: namespaceLabel(data.namespace) }));
	const command = $derived(['orkctl', '-n', shellQuote(namespaceLabel(data.namespace)), 'pool', 'list']);

	let retrying = $state(false);
	async function retry() {
		retrying = true;
		try {
			await invalidateAll();
		} finally {
			retrying = false;
		}
	}

	// navigatingHere is true while a navigation IN FLIGHT targets this same
	// route — the namespace switcher re-navigating `/admin/pools?ns=…`
	// while this page stays mounted, its stale data still on screen, is
	// otherwise indistinguishable from a page that finished loading. The
	// explicit Retry click above sets `retrying` itself (it calls
	// `invalidateAll` directly, not `goto`, so SvelteKit's own `navigating`
	// never turns true for it); this covers every OTHER re-entry into this
	// same URL a delayed RPC could stall on.
	const navigatingHere = $derived(
		navigating.to !== null && navigating.to.url.pathname === page.url.pathname
	);
	const busy = $derived(retrying || navigatingHere);

	// Which pool cards have their Orker list open. Orkers are deployment
	// infrastructure, so the list is a disclosure on the (namespace-scoped)
	// pool card rather than a namespace-scoped list of its own.
	let expanded = $state<Record<string, boolean>>({});

	// Nothing unavailable is ever drawn as zero: a pool the
	// cluster could not answer for has NO number at all, only the reason.
	function orkerLabel(pool: Pool): string {
		if (pool.unavailableReason) return 'unknown';
		if (pool.liveOrkerCount === 0) return '0';
		return String(pool.liveOrkerCount);
	}
</script>

<svelte:head><title>Admin — Pools — OrkStack</title></svelte:head>

<SheetHeader title="Pools" />

<ScopeNote testid="pools-scope-caption">
	Pools namespace {namespaceLabel(data.namespace)} may dispatch onto — this view honours the namespace
	selector above.
</ScopeNote>

<ListFilter request={ListPoolsRequestSchema} applied={data.applied} testid="pools" {command} onapply={applyFilters} {clearHref} />

{#if data.poolsError}
	<ErrorPanel
		testid="pools-error"
		retryTestid="pools-retry"
		subject="pool health for namespace {namespaceLabel(data.namespace)}"
		message={data.poolsError}
		busy={retrying}
		onretry={retry}
	/>
{:else if busy}
	<p class="text-sm text-ink-muted" data-testid="pools-loading">Loading pool health…</p>
{:else if data.pools.length === 0 && data.applied.length > 0}
	<p class="text-sm text-ink-muted" data-testid="pools-empty">No pools match these filters.</p>
{:else if data.pools.length === 0}
	<p class="text-sm text-ink-muted" data-testid="pools-empty">
		No pools are in scope for namespace {namespaceLabel(data.namespace)}.
	</p>
{:else}
	<!-- A register of pools, one ruled row each. A pool no Orker serves sits
		on the red tint; one the cluster could not answer for says so in the
		outlined warning mark, and has no number at all. -->
	<div class="hidden grid-cols-[minmax(10rem,1fr)_minmax(12rem,2fr)_minmax(10rem,1fr)] gap-4 border-t-2 border-b-(length:--rule-region) border-ink bg-paper-sunken px-3 py-2 md:grid" aria-hidden="true">
		<span class="caps text-ink-muted">Pool</span>
		<span class="caps text-ink-muted">Live Orkers</span>
		<span class="caps text-ink-muted">Last seen</span>
	</div>
	<ul class="border-t-2 border-ink md:border-t-0" data-testid="pool-list">
		{#each data.pools as pool (pool.name)}
			<li
				class="border-b border-rule px-3 py-3 {pool.liveOrkerCount === 0 && !pool.unavailableReason ? 'bg-red-tint' : ''}"
				data-testid="pool-card-{pool.name}"
			>
				<div class="grid gap-x-4 gap-y-1 md:grid-cols-[minmax(10rem,1fr)_minmax(12rem,2fr)_minmax(10rem,1fr)] md:items-baseline">
					<h2 class="flex items-center gap-2 font-mono text-sm font-semibold text-ink">
						{pool.name}
						{#if pool.isDefault}
							<span class="stamp border-blue font-sans text-blue" data-testid="pool-default-{pool.name}">default</span>
						{/if}
					</h2>
					{#if pool.unavailableReason}
						<p class="severity-warning text-xs" data-testid="pool-unavailable-{pool.name}">
							Could not be reached: {pool.unavailableReason}. Orker count is <strong>unknown</strong
							>, not zero — ask again rather than scaling.
						</p>
					{:else}
						<p class="text-sm text-ink-secondary">
							<span
								class="figure font-semibold {pool.liveOrkerCount === 0 ? 'text-red-strong' : 'text-ink'}"
								data-testid="pool-orker-count-{pool.name}"
							>
								{orkerLabel(pool)}
							</span>
							{pool.liveOrkerCount === 1 ? 'live Orker' : 'live Orkers'}
							{#if pool.liveOrkerCount === 0}
								<span class="ml-1 text-xs text-red-strong" data-testid="pool-no-orkers-{pool.name}"
									>— no live Orkers; a task sent here waits</span
								>
							{/if}
						</p>
					{/if}
					<p class="text-xs text-ink-muted">
						<span class="md:sr-only">Last seen: </span><span class="figure" data-testid="pool-last-seen-{pool.name}"
							>{pool.lastSeen ? fmtTimestamp(pool.lastSeen) : 'never (or gone quiet)'}</span
						>
					</p>
				</div>

				{#if !pool.unavailableReason}
					<button
						type="button"
						class="btn-disclosure mt-2 inline-flex items-center gap-1 text-xs font-semibold text-blue hover:underline"
						aria-expanded={expanded[pool.name] ?? false}
						data-testid="pool-orkers-toggle-{pool.name}"
						onclick={() => (expanded[pool.name] = !expanded[pool.name])}
					>
						{expanded[pool.name] ? 'Hide Orkers' : 'Show Orkers'}
					</button>
					{#if expanded[pool.name]}
						<PoolOrkers pool={pool.name} namespace={data.namespace} />
					{/if}
				{/if}
			</li>
		{/each}
	</ul>
	<p class="mt-4 text-xs text-ink-muted" data-testid="pools-default-note">
		A task naming no pool runs on <span class="font-mono font-semibold text-ink-secondary">{data.defaultPool}</span
		>.
	</p>
{/if}
