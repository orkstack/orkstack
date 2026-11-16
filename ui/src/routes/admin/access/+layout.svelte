<script lang="ts">
	// Access (UX35): who may do what in this deployment. One tab bar over the
	// screens; in asserted mode one panel instead, saying nothing is checked.
	import { page } from '$app/state';
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();

	const tabs = [
		{ href: '/admin/access', label: 'Bindings' },
		{ href: '/admin/access/roles', label: 'Roles' },
		{ href: '/admin/access/service-accounts', label: 'Service accounts' },
		{ href: '/admin/access/principals', label: 'People' },
		{ href: '/admin/access/explain', label: 'Explain' },
		{ href: '/admin/access/history', label: 'History' }
	];
</script>

{#if data.mode === 'asserted'}
	<SheetHeader title="Access" testid="access-title" commands={[{ label: 'Whether this deployment authenticates', command: 'orkctl admin auth status' }]} />
	<section class="card max-w-prose p-5" data-testid="access-off">
		<h2 class="text-md font-semibold">Authentication is off</h2>
		<p class="mt-2 text-ink-2">
			This deployment runs in <code>asserted</code> mode: it verifies no caller and checks no permission. Every principal it records
			was asserted by your gateway. Roles and bindings take effect once authentication is turned on; the deployment guide,
			AUTHENTICATION.md, says how.
		</p>
	</section>
{:else}
	<nav class="tabs mb-5" aria-label="Access views">
		{#each tabs as tab (tab.href)}
			<a href={tab.href} class="tab" aria-current={page.url.pathname === tab.href ? 'page' : undefined} data-testid="access-tab-{tab.label}">{tab.label}</a>
		{/each}
	</nav>
	{@render children()}
{/if}

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
