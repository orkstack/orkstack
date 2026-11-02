<script lang="ts">
	// An activity's icon: the image's own declared drawing when it has one
	// the UI accepts (#lib/activityIcon.ts), else the UI's drawing for the
	// kind — a first-party activity, a composed or awaited task, or the
	// default container. Always decorative: the name sits beside it.
	import { safeIcon, type DeclaredIcon, type SafeNode } from '#lib/activityIcon.ts';
	import type { ActivityKind } from '#lib/icons.ts';
	import Icon from './Icon.svelte';

	let { kind = 'default', icon, class: className = 'size-4' }: { kind?: ActivityKind; icon?: DeclaredIcon; class?: string } = $props();

	const declared = $derived(safeIcon(icon));
</script>

{#snippet draw(nodes: SafeNode[])}
	{#each nodes as node, i (i)}
		{#if node.tag === 'g'}
			<g {...node.attrs}>{@render draw(node.children)}</g>
		{:else}
			<svelte:element this={node.tag} xmlns="http://www.w3.org/2000/svg" {...node.attrs} />
		{/if}
	{/each}
{/snippet}

{#if declared}
	<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" focusable="false" class="shrink-0 {className}" {...declared.attrs}
		>{@render draw(declared.nodes)}</svg
	>
{:else}
	<Icon name="a-{kind}" class={className} />
{/if}
