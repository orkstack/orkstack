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
	import ErrorPanel from '#lib/components/ErrorPanel.svelte';
	import ListFilter from '#lib/components/ListFilter.svelte';
	import { listHref, runWindowFilters } from '#lib/browsing.ts';
	import { filterFlags, type FieldFilter } from '#lib/listFilter.ts';
	import { shellQuote } from '#lib/runDetail.ts';
	import { ListPoolsRequestSchema } from '#lib/gen/orkstack/v1/namespace_service_pb.ts';
	import PoolOrkers from '#lib/components/PoolOrkers.svelte';
	import CliPopover from '#lib/components/CliPopover.svelte';
	import TimeAgo from '#lib/components/TimeAgo.svelte';
	import type { Orker } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { formatBytes } from '#lib/resources.ts';
	import type { Pool } from '#lib/gen/orkstack/v1/namespace_pb.ts';
	import { namespaceLabel } from '#lib/namespace.ts';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	// The filters narrow the pool cards (ListFilter; docs/UI_CLI_PARITY.md
	// "Filtering lists"); `ns` stays, because it scopes this view.
	function applyFilters(filters: FieldFilter[]) {
		void goto(listHref('/admin/pools', { ns: namespaceLabel(data.namespace) }, { filter: filters }), { reset: false });
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
	// Every pool's Orkers are shown, with Cordon…; a reader may fold one away.
	let expanded = $state<Record<string, boolean>>({});
	const shown = (pool: string) => expanded[pool] ?? true;

	// Nothing unavailable is ever drawn as zero: a pool the
	// cluster could not answer for has NO number at all, only the reason.
	/** What a pool's live Orkers reserved of what they declared. */
	interface PoolLoad {
		orkers: number;
		cordoned: number;
		cpu: { reserved: number; capacity: number };
		memory: { reserved: number; capacity: number };
	}
	function loadOf(pool: string, orkers: Orker[]): PoolLoad {
		const mine = orkers.filter((o) => o.pool === pool && !o.unavailableReason);
		const sum = (f: (o: Orker) => number) => mine.reduce((a, o) => a + f(o), 0);
		return {
			orkers: mine.length,
			cordoned: mine.filter((o) => o.cordoned).length,
			cpu: { reserved: sum((o) => o.reserved?.cpu ?? 0), capacity: sum((o) => o.capacity?.cpu ?? 0) },
			memory: { reserved: sum((o) => Number(o.reserved?.memoryBytes ?? 0n)), capacity: sum((o) => Number(o.capacity?.memoryBytes ?? 0n)) }
		};
	}
	/** Ready, Full or Cordoned, in words; colour only beside the word. */
	function stateOf(pool: Pool, load: PoolLoad | null): { word: string; tone: 'ok' | 'wait' | 'fail' | 'plain' } {
		if (pool.unavailableReason) return { word: 'Unknown', tone: 'plain' };
		if (pool.liveOrkerCount === 0) return { word: 'No Orkers', tone: 'fail' };
		if (load && load.orkers > 0 && load.cordoned === load.orkers) return { word: 'Cordoned', tone: 'wait' };
		if (load && isFull(load)) return { word: 'Full', tone: 'wait' };
		if (load && load.cordoned > 0) return { word: `${load.cordoned} cordoned`, tone: 'wait' };
		return { word: 'Ready', tone: 'ok' };
	}
	/** Every Orker has reserved all it declared, in a dimension their capacity bounds. */
	function isFull(load: PoolLoad): boolean {
		const at = (d: { reserved: number; capacity: number }) => d.capacity > 0 && d.reserved >= d.capacity;
		return load.orkers > 0 && (at(load.cpu) || at(load.memory));
	}
	function fullPools(pools: Pool[], orkers: Orker[]): { name: string; dimension: string }[] {
		return pools.flatMap((pool) => {
			const load = loadOf(pool.name, orkers);
			if (pool.unavailableReason || !isFull(load)) return [];
			return [{ name: pool.name, dimension: load.cpu.capacity > 0 && load.cpu.reserved >= load.cpu.capacity ? 'cpu' : 'memory' }];
		});
	}
	const cpuWords = (n: number) => (Number.isInteger(n) ? String(n) : n.toFixed(1));
	const share = (r: number, c: number) => (c > 0 ? Math.min(1, r / c) : 0);
	const cli = $derived([
		{ label: 'The pools', command: [...command, ...filterFlags(data.applied)].join(' ') },
		{ label: 'Every Orker, with its reservations', command: 'orkctl orker list' }
	]);

	function orkerLabel(pool: Pool): string {
		if (pool.unavailableReason) return 'unknown';
		if (pool.liveOrkerCount === 0) return '0';
		return String(pool.liveOrkerCount);
	}
</script>

<svelte:head><title>Admin — Pools — OrkStack</title></svelte:head>

<header class="mb-4 flex flex-wrap items-start gap-3">
	<div class="min-w-0 flex-1">
		<h1 class="page-h1">Pools</h1>
		<p class="mt-1 text-dense text-ink-2" data-testid="pools-scope-caption">
			The pools namespace {namespaceLabel(data.namespace)} may dispatch onto; this view honours the namespace selector.
		</p>
	</div>
	<button type="button" class="btn" data-testid="pools-refresh" disabled={busy} onclick={() => void retry()}>Refresh</button>
	<CliPopover testid="pools-cli" title="Command line: pools" commands={cli} />
</header>

<ListFilter request={ListPoolsRequestSchema} applied={data.applied} testid="pools" {command} onapply={applyFilters} {clearHref} cli={false} />

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
	<!-- A card per pool: its state in words, its live Orkers, and how much of
		what they declared is reserved now. A pool no Orker serves says so; one
		the cluster could not answer for has no number at all. -->
	{#await data.orkers then inv}
		{@const full = inv.error ? [] : fullPools(data.pools, inv.orkers)}
		{#if full.length}
			<!-- The capacity note: what a full pool means for what is sent to it. -->
			<div class="capline mb-4" role="status" data-testid="pools-capacity-note">
				<span class="mark-wait" aria-hidden="true"></span>
				<b class="font-semibold text-wait">{full.length === 1 ? `Pool ${full[0].name} is full` : `${full.length} pools are full`}</b>
				<span class="min-w-0 text-ink-2">{full.length === 1 ? `every Orker has reserved its whole ${full[0].dimension}.` : `${full.map((p) => p.name).join(', ')}: every Orker has reserved all it declared.`} A task sent there starts when an Orker frees room or another Orker joins.</span>
				<a class="ml-auto font-semibold whitespace-nowrap" data-testid="pools-capacity-runs" href={listHref('/runs', { ns: namespaceLabel(data.namespace) }, { filter: runWindowFilters('', 'RUNNING', '', '') })}>View running runs</a>
			</div>
		{/if}
	{/await}
	<ul class="poolgrid" data-testid="pool-list">
		{#each data.pools as pool (pool.name)}
			<li class="card flex flex-col" id="pool-{pool.name}" data-testid="pool-card-{pool.name}">
				{#await data.orkers}
					{@render card(pool, null, '')}
				{:then inv}
					{@render card(pool, inv.error ? null : loadOf(pool.name, inv.orkers), inv.error ?? '')}
				{/await}
			</li>
		{/each}
	</ul>
	<!-- What was reserved reads after the pools have drawn, with its own client. -->
	{#await import('#lib/components/PoolsUsage.svelte') then { default: PoolsUsage }}
		<PoolsUsage namespace={data.namespace} />
	{/await}
	{#each data.pools as pool (pool.name)}
		{#if shown(pool.name) && !pool.unavailableReason}
			<section class="card mt-4" aria-labelledby="orkers-{pool.name}-h" id="orkers-{pool.name}">
				<div class="card-head"><h2 id="orkers-{pool.name}-h">Orkers of {pool.name}</h2></div>
				<PoolOrkers pool={pool.name} namespace={data.namespace} />
			</section>
		{/if}
	{/each}
	<p class="mt-4 text-sm text-ink-3" data-testid="pools-default-note">
		A task naming no pool runs on <span class="font-mono font-semibold text-ink-2">{data.defaultPool}</span>.
	</p>
{/if}

{#snippet meter(label: string, reserved: number, capacity: number, words: string, full: boolean)}
	<div class="grid gap-1">
		<div class="flex justify-between text-sm"><span class="text-ink-3">{label}</span><span class="num text-ink-2">{words}</span></div>
		<svg class="meter" height="8" aria-hidden="true"
			><rect width="100%" height="8" rx="4" class="mtrack" /><rect width="{share(reserved, capacity) * 100}%" height="8" rx="4" class={full ? 'mfull' : 'mval'} /></svg
		>
	</div>
{/snippet}

{#snippet card(pool: Pool, load: PoolLoad | null, invError: string)}
	{@const st = stateOf(pool, load)}
	<div class="card-head">
		<h2>{pool.name}</h2>
		{#if pool.isDefault}<span class="pill-plain" data-testid="pool-default-{pool.name}">Default</span>{/if}
		<span class="ml-auto stamp {st.tone === 'ok' ? 'bg-ok-tint text-ok mark-square' : st.tone === 'wait' ? 'pill-wait' : st.tone === 'fail' ? 'bg-fail-tint text-fail mark-tri' : 'pill-plain'}" data-testid="pool-state-{pool.name}"
			>{#if st.tone === 'wait'}<span class="mark-wait" aria-hidden="true"></span>{/if}{st.word}</span
		>
	</div>
	<div class="grid flex-1 gap-3 px-4 py-3">
		{#if pool.unavailableReason}
			<p class="severity-warning text-sm" data-testid="pool-unavailable-{pool.name}">
				Could not be reached: {pool.unavailableReason}. Orker count is <strong>unknown</strong>, not zero — ask again rather than scaling.
			</p>
		{:else}
			<p class="text-dense text-ink-2">
				<b class="num {pool.liveOrkerCount === 0 ? 'text-fail' : 'text-ink'}" data-testid="pool-orker-count-{pool.name}">{orkerLabel(pool)}</b>
				{pool.liveOrkerCount === 1 ? 'live Orker' : 'live Orkers'}{#if load && load.cordoned > 0}, {load.cordoned} cordoned{/if}
				{#if pool.liveOrkerCount === 0}
					<span class="block text-sm text-fail" data-testid="pool-no-orkers-{pool.name}">No live Orkers; a task sent here waits.</span>
				{/if}
			</p>
			{#if load && load.cpu.capacity > 0}
				{@render meter('cpu', load.cpu.reserved, load.cpu.capacity, `${cpuWords(load.cpu.reserved)} of ${cpuWords(load.cpu.capacity)} reserved`, load.cpu.reserved >= load.cpu.capacity)}
			{/if}
			{#if load && load.memory.capacity > 0}
				{@render meter('memory', load.memory.reserved, load.memory.capacity, `${formatBytes(load.memory.reserved)} of ${formatBytes(load.memory.capacity)}`, load.memory.reserved >= load.memory.capacity)}
			{/if}
			{#if load && load.cpu.capacity === 0 && load.orkers > 0}
				<p class="text-sm text-ink-3">These Orkers declare no capacity, so nothing caps what they take.</p>
			{/if}
			{#if invError}<p class="text-sm text-ink-3">Reservations could not be read: {invError}</p>{/if}
		{/if}
	</div>
	<div class="mt-auto flex flex-wrap items-center gap-2 border-t border-line px-4 py-2.5 text-sm text-ink-3">
		<span>Last seen <span data-testid="pool-last-seen-{pool.name}">{#if pool.lastSeen}<TimeAgo at={pool.lastSeen} />{:else}never (or gone quiet){/if}</span></span>
		{#if !pool.unavailableReason}
			<button
				type="button"
				class="btn btn-sm btn-quiet ml-auto"
				aria-expanded={shown(pool.name)}
				data-testid="pool-orkers-toggle-{pool.name}"
				onclick={() => (expanded[pool.name] = !shown(pool.name))}>{shown(pool.name) ? 'Hide Orkers' : 'Show Orkers'}</button
			>
		{/if}
	</div>
{/snippet}

<style>
	.poolgrid {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 16px;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.meter {
		display: block;
		width: 100%;
	}
	.mtrack {
		fill: var(--surface-3);
	}
	.mval {
		fill: var(--meter);
	}
	.mfull {
		fill: var(--wait-mark);
	}
	@media (max-width: 1180px) {
		.poolgrid {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}
	}
	@media (max-width: 900px) {
		.poolgrid {
			grid-template-columns: minmax(0, 1fr);
		}
	}
	@media (forced-colors: active) {
		.meter {
			forced-color-adjust: none;
		}
	}
</style>
