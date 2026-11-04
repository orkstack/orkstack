<script lang="ts">
	// The Operate pages share the sidebar's Operate group for their
	// navigation (Sidebar.svelte); this layout only says when a move between
	// them is still loading.
	import { navigating } from '$app/state';
	import type { LayoutProps } from './$types';

	let { children }: LayoutProps = $props();

	const navigatingWithinAdmin = $derived(navigating.to !== null && navigating.to.url.pathname.startsWith('/admin'));
</script>

{#if navigatingWithinAdmin}
	<p class="mb-4 text-sm text-ink-3" data-testid="admin-nav-loading">Loading…</p>
{/if}

{@render children()}
