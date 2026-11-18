<script lang="ts">
	// Access (UX35): who may do what in this deployment. Each screen draws the tab
	// bar under its own title (AccessTabs); in asserted mode one panel instead, saying nothing is checked.
	import SheetHeader from '#lib/components/SheetHeader.svelte';
	import type { LayoutProps } from './$types';

	let { data, children }: LayoutProps = $props();
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
	{@render children()}
{/if}
