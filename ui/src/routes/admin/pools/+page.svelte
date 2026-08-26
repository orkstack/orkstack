<script lang="ts">
	// Pool health (design §6.1, acceptance 3): live Orker counts and per-pool
	// plugins, narrowed rather than refused (NamespaceService.ListPools,
	// M11.3). A pool this read could not ask about is a THIRD state, distinct
	// from both "healthy" and "empty" — Pool.unavailable_reason — and it is
	// rendered as its own state rather than folded into a zero.
	import { invalidateAll } from '$app/navigation';
	import { navigating, page } from '$app/state';
	import ErrorPanel from '$lib/components/ErrorPanel.svelte';
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

	// Nothing unavailable is ever drawn as zero (acceptance 3): a pool the
	// cluster could not answer for has NO number at all, only the reason.
	function orkerLabel(pool: Pool): string {
		if (pool.unavailableReason) return 'unknown';
		if (pool.liveOrkerCount === 0) return '0';
		return String(pool.liveOrkerCount);
	}
</script>

<svelte:head><title>Admin — Pools — OrkStack</title></svelte:head>

<p class="mb-4 text-sm text-zinc-500" data-testid="pools-scope-caption">
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
	<p class="text-sm text-zinc-400" data-testid="pools-loading">Loading pool health…</p>
{:else if data.pools.length === 0}
	<p class="text-sm text-zinc-400" data-testid="pools-empty">
		No pools are in scope for namespace {namespaceLabel(data.namespace)}.
	</p>
{:else}
	<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3" data-testid="pool-list">
		{#each data.pools as pool (pool.name)}
			<div
				class="rounded-lg border p-4 {pool.unavailableReason
					? 'border-amber-500/40 bg-amber-500/5'
					: pool.liveOrkerCount === 0
						? 'border-rose-500/30 bg-rose-500/5'
						: 'border-zinc-800'}"
				data-testid="pool-card-{pool.name}"
			>
				<div class="mb-2 flex items-center justify-between">
					<h3 class="font-medium text-zinc-100">{pool.name}</h3>
					{#if pool.isDefault}
						<span
							class="rounded-full bg-sky-500/15 px-2 py-0.5 text-xs font-medium text-sky-300"
							data-testid="pool-default-{pool.name}"
						>
							default
						</span>
					{/if}
				</div>

				{#if pool.unavailableReason}
					<p
						class="rounded-md border border-amber-500/40 bg-amber-500/10 px-2 py-1.5 text-xs text-amber-200"
						data-testid="pool-unavailable-{pool.name}"
					>
						Could not be reached: {pool.unavailableReason}. Orker count is <strong>unknown</strong
						>, not zero — ask again rather than scaling.
					</p>
				{:else}
					<p class="text-sm text-zinc-300">
						<span
							class="font-semibold {pool.liveOrkerCount === 0 ? 'text-rose-300' : 'text-emerald-300'}"
							data-testid="pool-orker-count-{pool.name}"
						>
							{orkerLabel(pool)}
						</span>
						{pool.liveOrkerCount === 1 ? 'live Orker' : 'live Orkers'}
						{#if pool.liveOrkerCount === 0}
							<span class="ml-1 text-xs text-rose-400" data-testid="pool-no-orkers-{pool.name}"
								>— no live Orkers; a task sent here waits</span
							>
						{/if}
					</p>
				{/if}

			<div class="mt-2 text-xs text-zinc-400">
				<span class="text-zinc-500">Plugins:</span>
				{#if pool.unavailableReason}
					<span class="text-zinc-600" data-testid="pool-plugins-unavailable-{pool.name}"
						>unknown — this pool could not be reached, so it was never asked</span
					>
				{:else if pool.plugins.length > 0}
					<span data-testid="pool-plugins-{pool.name}">{pool.plugins.join(', ')}</span>
				{:else if pool.liveOrkerCount === 0}
					<span class="text-zinc-600" data-testid="pool-plugins-no-live-orkers-{pool.name}"
						>unknown — no live Orker to ask</span
					>
				{:else}
					<span class="text-zinc-600" data-testid="pool-plugins-unknown-{pool.name}"
						>none reported (sampled from one Orker; may be silent, not empty)</span
					>
				{/if}
			</div>

				<p class="mt-2 text-xs text-zinc-500">
					Last seen: <span data-testid="pool-last-seen-{pool.name}"
						>{pool.lastSeen ? fmtTimestamp(pool.lastSeen) : 'never (or gone quiet)'}</span
					>
				</p>
			</div>
		{/each}
	</div>
	<p class="mt-4 text-xs text-zinc-500" data-testid="pools-default-note">
		A task naming no pool runs on <span class="font-medium text-zinc-300">{data.defaultPool}</span>.
	</p>
{/if}
