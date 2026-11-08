<script lang="ts">
	// "More": a page's secondary actions in one disclosure, where its header
	// has no room for them as buttons. Every item does exactly what the button
	// it stands in for does, and choosing one closes the list. A disclosure,
	// not an ARIA menu: its items are ordinary buttons and links in tab order.
	import type { Snippet } from 'svelte';
	import Icon from './Icon.svelte';
	import Popover from './Popover.svelte';

	interface Props {
		testid: string;
		label?: string;
		/** The items; each is handed the function that closes the list. */
		items: Snippet<[() => void]>;
		class?: string;
		/** The design's three dots: the label is then the button's accessible name. */
		iconOnly?: boolean;
		/** The dots at a page header's size, beside its terminal icon. */
		bordered?: boolean;
		/** Called each time the list opens, for what it reads only when asked. */
		onopen?: () => void;
	}

	let { testid, label = 'More', items, class: className = '', iconOnly = false, bordered = false, onopen }: Props = $props();

	let open = $state(false);
	const close = () => (open = false);
</script>

<div class={className}>
	<Popover {open} onOpenChange={(next) => (open = next)} testid="{testid}-list" align="end">
		{#snippet trigger()}
			<button
				type="button"
				data-testid={testid}
				aria-expanded={open}
				aria-controls="{testid}-items"
				onclick={() => {
					open = !open;
					if (open) onopen?.();
				}}
				aria-label={iconOnly ? label : undefined}
				data-tip={iconOnly ? label : undefined}
				class={iconOnly ? (bordered ? 'btn btn-icon' : 'btn btn-sm btn-quiet btn-icon') : 'btn gap-1.5'}>{#if iconOnly}<Icon name="more" />{:else}{label}<Icon name="chev" class="size-4 text-ink-muted" />{/if}</button
			>
		{/snippet}
		{#snippet content()}
			<div id="{testid}-items" class="more-items -m-3 flex min-w-52 flex-col py-1">{@render items(close)}</div>
		{/snippet}
	</Popover>
</div>

<style>
	/* Each item is a full-width row of the list: the button's words, left
	   aligned, the hover wash of a register row. */
	.more-items :global(:is(a, button)) {
		display: flex;
		align-items: center;
		min-height: calc(var(--spacing) * 9);
		padding-inline: calc(var(--spacing) * 3);
		font-size: var(--text-sm);
		line-height: var(--text-sm--line-height);
		font-weight: var(--font-weight-semibold);
		color: var(--color-ink);
		text-align: left;
		white-space: nowrap;
		transition: background-color var(--duration-fast) var(--ease-out);
	}
	.more-items :global(:is(a, button):hover) {
		background-color: var(--color-hover);
	}
</style>
