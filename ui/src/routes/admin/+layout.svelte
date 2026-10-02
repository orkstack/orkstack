<script lang="ts">
	// The Admin area's own sub-navigation.
	//
	// Every route under here carries `?ns=` like any other page — the
	// namespace switcher stays visible on all of them — but only Pools RESPECTS
	// it. Namespaces, Activities, Webhooks, Retention (the deployment MAXIMUM;
	// a namespace's own run retention lives on the Namespaces page), Image
	// policy (the deployment FLOOR; a namespace's own signer lives on the
	// Namespaces page) and the overview are DEPLOYMENT-WIDE, and Audit is a third behaviour: it shows
	// every namespace by default and applies the switcher's namespace only as
	// an explicit filter chosen on the page ("All namespaces" or "Namespace
	// …"). Each says so in its own caption
	// rather than hiding the switcher: hiding chrome per route is
	// deliberately not done, because a switcher that disappears on some
	// pages and not others is one more thing to notice, where a sentence in
	// the page is read once and settles it.
	import { navigating, page } from '$app/state';
	import { withNamespace } from '$lib/namespace';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();

	const link = (path: string) => withNamespace(path, data.namespace);

	// navigatingWithinAdmin is true for the whole span between clicking a
	// tab (or switching the namespace, or landing here from outside Admin
	// entirely) and the destination route's `load` resolving. This layout
	// is the shared ancestor across all five sub-routes, so it is the one
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
		{ id: 'webhooks', href: '/admin/webhooks', label: 'Webhooks' },
		{ id: 'retention', href: '/admin/retention', label: 'Retention' },
		{ id: 'usage', href: '/admin/usage', label: 'Usage' },
		{ id: 'image-policy', href: '/admin/image-policy', label: 'Image policy' },
		{ id: 'audit', href: '/admin/audit', label: 'Audit' }
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
	<h1 class="text-xl font-semibold tracking-tight text-fg">Admin</h1>
	<p class="mt-1 text-sm text-fg-muted">
		Manage namespaces, watch pool health, and read what this deployment is made of.
	</p>
</div>

<nav class="mb-6 flex gap-1 border-b border-border" aria-label="Admin views" data-testid="admin-nav">
	{#each tabs as tab (tab.id)}
		<a
			href={link(tab.href)}
			data-testid="admin-tab-{tab.id}"
			aria-current={current === tab.id ? 'page' : undefined}
			class="-mb-px border-b-2 border-transparent px-3 py-2 text-sm font-medium text-fg-muted transition-colors duration-150 hover:text-fg current:border-brand-600 current:text-fg"
		>
			{tab.label}
		</a>
	{/each}
</nav>

{#if navigatingWithinAdmin}
	<p class="mb-4 text-sm text-fg-muted" data-testid="admin-nav-loading">Loading…</p>
{/if}

{@render children()}
