<script lang="ts" module>
	import type { Snippet } from 'svelte';
	export interface Fact {
		label: string;
		/** Plain text, or a snippet for a value with a plate, a tag or a ring. */
		value: string | Snippet;
		testid?: string;
	}
</script>

<script lang="ts">
	// The facts strip: one <dl> of label and value pairs with vertical rules,
	// the same on the run, failed run, Runs and editor headers (brief §5).
	// Two columns with no rules below 640 px, one below 360 px.
	let { facts, small = false, label }: { facts: Fact[]; small?: boolean; label?: string } = $props();
</script>

<dl class="fstrip" class:small aria-label={label}>
	{#each facts as fact (fact.label)}
		<div data-testid={fact.testid}>
			<dt>{fact.label}</dt>
			<dd>{#if typeof fact.value === 'string'}{fact.value}{:else}{@render fact.value()}{/if}</dd>
		</div>
	{/each}
</dl>

<style>
	.small :global(dd) {
		font-size: var(--text-sm);
	}
	.small > :global(div) {
		padding: 0 14px;
	}
	.small > :global(div:first-child) {
		padding-left: 14px;
	}
	@media (max-width: 640px) {
		.small > :global(div),
		.small > :global(div:first-child) {
			padding: 0;
			border-left: 0;
		}
	}
	@media (max-width: 360px) {
		.fstrip {
			grid-template-columns: minmax(0, 1fr);
		}
	}
</style>
