<script lang="ts">
	// The Admin area's own sub-navigation (design §6.1, D12).
	//
	// Every route under here carries `?ns=` like any other page — the
	// namespace switcher stays visible on all four — but only Pools RESPECTS
	// it. Namespaces, Activities and the overview are DEPLOYMENT-WIDE, and each
	// says so in its own caption rather than hiding the switcher: hiding
	// chrome per route was reviewed and rejected (design §6.3) because a
	// switcher that disappears on some pages and not others is one more thing
	// to notice, where a sentence in the page is read once and settles it.
	import { navigating, page } from '$app/state';
	import { withNamespace } from '$lib/namespace';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();

	const link = (path: string) => withNamespace(path, data.namespace);

	// navigatingWithinAdmin is true for the whole span between clicking a
	// tab (or switching the namespace, or landing here from outside Admin
	// entirely) and the destination route's `load` resolving. This layout
	// is the shared ancestor across all four sub-routes, so it is the one
	// place that stays mounted through a CROSS-tab transition — the
	// destination `+page.svelte` does not exist yet, and the SOURCE page's
	// own loading state (see its own `navigatingHere`) only fires for a
	// re-navigation into ITSELF, not into a sibling tab. Without this, a
	// slow `load` for e.g. Pools left Overview's stale content on screen
	// with nothing on the page saying a navigation was in flight.
	const navigatingWithinAdmin = $derived(
		navigating.to !== null && navigating.to.url.pathname.startsWith('/admin')
	);

	const tabs = [
		{ id: 'overview', href: '/admin', label: 'Overview' },
		{ id: 'namespaces', href: '/admin/namespaces', label: 'Namespaces' },
		{ id: 'pools', href: '/admin/pools', label: 'Pools' },
		{ id: 'activities', href: '/admin/activities', label: 'Activities' },
		{ id: 'retention', href: '/admin/retention', label: 'Retention' }
	];

	// "overview" only ever matches the exact `/admin` path: every other tab's
	// href is a PREFIX of it (`/admin/namespaces` starts with `/admin`), so a
	// naive prefix match would have every sub-route paint Overview as active
	// too. The sub-tabs are unambiguous prefixes of each other's disjoint
	// subtrees, so a plain startsWith is correct for them.
	const current = $derived(
		page.url.pathname === '/admin'
			? 'overview'
			: (tabs.find((t) => t.id !== 'overview' && page.url.pathname.startsWith(t.href))?.id ??
					'overview')
	);
</script>

<svelte:head><title>Admin — OrkStack</title></svelte:head>

<div class="mb-6">
	<h1 class="text-2xl font-bold text-zinc-50">Admin</h1>
	<p class="mt-1 text-sm text-zinc-400">
		Manage namespaces, watch pool health, and read what this deployment is made of.
	</p>
</div>

<nav class="mb-6 flex gap-1 border-b border-zinc-800" aria-label="Admin views" data-testid="admin-nav">
	{#each tabs as tab (tab.id)}
		<a
			href={link(tab.href)}
			data-testid="admin-tab-{tab.id}"
			aria-current={current === tab.id ? 'page' : undefined}
			class="-mb-px border-b-2 px-4 py-2 text-sm {current === tab.id
				? 'border-emerald-400 font-medium text-emerald-300'
				: 'border-transparent text-zinc-400 hover:text-zinc-200'}"
		>
			{tab.label}
		</a>
	{/each}
</nav>

{#if navigatingWithinAdmin}
	<p class="mb-4 text-sm text-zinc-400" data-testid="admin-nav-loading">Loading…</p>
{/if}

{@render children()}

