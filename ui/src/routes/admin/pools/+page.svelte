<script lang="ts">
	// Pool health: live Orker counts, narrowed rather than refused
	// (NamespaceService.ListPools). A pool this
	// read could not ask about is a THIRD state, distinct from both "healthy"
	// and "empty" — Pool.unavailable_reason — and it is rendered as its own
	// state rather than folded into a zero.
	//
	// What a deployment can run is on Admin → Activities; who is serving a
	// pool is the count below.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
	import PoolOrkers from '$lib/components/PoolOrkers.svelte';
	import type { Pool } from '$lib/gen/orkstack/v1/namespace_pb';
	import { namespaceLabel } from '$lib/namespace';
	import { fmtTimestamp } from '$lib/status';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

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

<p class="mb-4 text-sm text-fg-muted" data-testid="pools-scope-caption">
	Pools namespace {namespaceLabel(data.namespace)} may dispatch onto — this view honours the namespace
	selector above.
</p>

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
	<p class="text-sm text-fg-muted" data-testid="pools-loading">Loading pool health…</p>
{:else if data.pools.length === 0}
	<p class="text-sm text-fg-muted" data-testid="pools-empty">
		No pools are in scope for namespace {namespaceLabel(data.namespace)}.
	</p>
{:else}
	<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3" data-testid="pool-list">
		{#each data.pools as pool (pool.name)}
			<div
				class="rounded-lg border bg-surface p-4 shadow-xs {pool.unavailableReason
					? 'border-warning-200 bg-warning-50'
					: pool.liveOrkerCount === 0
						? 'border-danger-200 bg-danger-50'
						: 'border-border'}"
				data-testid="pool-card-{pool.name}"
			>
				<div class="mb-2 flex items-center justify-between">
					<h3 class="font-medium text-fg">{pool.name}</h3>
					{#if pool.isDefault}
						<span
							class="rounded-full border border-info-200 bg-info-50 px-2 py-0.5 text-xs font-medium text-info-700"
							data-testid="pool-default-{pool.name}"
						>
							default
						</span>
					{/if}
				</div>

				{#if pool.unavailableReason}
					<p
						class="rounded-md border border-warning-200 bg-warning-50 px-2 py-1.5 text-xs text-warning-800"
						data-testid="pool-unavailable-{pool.name}"
					>
						Could not be reached: {pool.unavailableReason}. Orker count is <strong>unknown</strong
						>, not zero — ask again rather than scaling.
					</p>
				{:else}
					<p class="text-sm text-fg-secondary">
						<span
							class="font-semibold {pool.liveOrkerCount === 0
								? 'text-failed-fg'
								: 'text-succeeded-fg'}"
							data-testid="pool-orker-count-{pool.name}"
						>
							{orkerLabel(pool)}
						</span>
						{pool.liveOrkerCount === 1 ? 'live Orker' : 'live Orkers'}
						{#if pool.liveOrkerCount === 0}
							<span class="ml-1 text-xs text-failed-fg" data-testid="pool-no-orkers-{pool.name}"
								>— no live Orkers; a task sent here waits</span
							>
						{/if}
					</p>
				{/if}

				<p class="mt-2 text-xs text-fg-muted">
					Last seen: <span data-testid="pool-last-seen-{pool.name}"
						>{pool.lastSeen ? fmtTimestamp(pool.lastSeen) : 'never (or gone quiet)'}</span
					>
				</p>

				{#if !pool.unavailableReason}
					<button
						type="button"
						class="mt-2 text-xs font-medium text-brand-700 hover:underline"
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
			</div>
		{/each}
	</div>
	<p class="mt-4 text-xs text-fg-muted" data-testid="pools-default-note">
		A task naming no pool runs on <span class="font-medium text-fg-secondary">{data.defaultPool}</span
		>.
	</p>
{/if}
