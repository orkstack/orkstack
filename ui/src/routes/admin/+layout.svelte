<script lang="ts">
	// The Admin area's own sub-navigation.
	//
	// Every route under here carries `?ns=` like any other page — the
	// namespace switcher stays visible on all of them — but only Pools and
	// Automation RESPECT it (an automation document describes exactly one
	// namespace, and the page plans, exports and applies in the switcher's). Namespaces, Releases, Activities, Webhooks, Retention (the deployment MAXIMUM;
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
	import { OPERATE_PAGES } from '$lib/navigation';
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

	const tabs = OPERATE_PAGES;

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

<div class="mb-4">
	<h1 class="text-xl font-semibold tracking-tight text-ink">Operate</h1>
	<p class="mt-1 text-sm text-ink-secondary">
		Manage namespaces, watch pool health, and read what this deployment is made of.
	</p>
</div>

<!--
	The Operate pages as the sheet's first row: tracked-caps tabs on a 1.5 px
	rule. On a narrow screen the strip scrolls inside its own frame, never the page.
-->
<nav
	class="mb-6 overflow-x-auto border-b-(length:--rule-region) border-ink"
	aria-label="Admin views"
	data-testid="admin-nav"
>
	<div class="flex w-max gap-1">
		{#each tabs as tab (tab.id)}
			<a
				href={link(tab.href)}
				data-testid="admin-tab-{tab.id}"
				aria-current={current === tab.id ? 'page' : undefined}
				class="caps -mb-px border-b-2 border-transparent px-3 py-2.5 whitespace-nowrap text-ink-muted transition-colors hover:text-ink current:border-blue current:text-ink"
			>
				{tab.label}
			</a>
		{/each}
	</div>
</nav>

{#if navigatingWithinAdmin}
	<p class="mb-4 text-sm text-ink-muted" data-testid="admin-nav-loading">Loading…</p>
{/if}

{@render children()}
