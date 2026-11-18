<script lang="ts">
	// Access's tab bar (UX35): one tab per screen, drawn under each screen's
	// own title so the title stays where the other admin pages keep theirs.
	import { page } from '$app/state';
	import { ACCESS_TABS } from '#lib/navigation.ts';
</script>

<nav class="tabs mb-5" aria-label="Access views">
	{#each ACCESS_TABS as tab (tab.href)}
		<a href={tab.href} class="tab" aria-current={page.url.pathname === tab.href ? 'page' : undefined} data-testid="access-tab-{tab.label}">{tab.label}</a>
	{/each}
</nav>

<style>
	.tabs {
		display: inline-flex;
		max-width: 100%;
		gap: 2px;
		padding: 3px;
		overflow-x: auto;
		border: var(--rule-hair) solid var(--color-line);
		border-radius: var(--radius-md);
		background: var(--color-surface-2);
		scrollbar-width: none;
	}
	.tab {
		display: inline-flex;
		align-items: center;
		height: calc(var(--spacing) * 7);
		padding: 0 calc(var(--spacing) * 3);
		border-radius: var(--radius-sm);
		color: var(--color-ink-2);
		font-size: var(--text-dense);
		font-weight: var(--font-weight-semibold);
		text-decoration: none;
		white-space: nowrap;
	}
	.tab:hover {
		color: var(--color-ink);
	}
	.tab[aria-current='page'] {
		background: var(--color-surface);
		color: var(--color-navy-text);
		box-shadow: 0 0 0 var(--rule-hair) var(--color-line-2), var(--shadow-card);
	}
	.tab:focus-visible {
		outline-offset: -2px;
	}
	@media (max-width: 640px) {
		.tab {
			min-height: calc(var(--spacing) * 10);
		}
	}
	@media (forced-colors: active) {
		.tab[aria-current='page'] {
			outline: 2px solid Highlight;
		}
	}
</style>
