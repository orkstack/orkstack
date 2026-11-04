<svelte:options namespace="svg" />

<script lang="ts">
	// One image's declared icon as a <symbol>, so a thousand tasks that run it
	// draw it with one <use> each. What it may draw is the closed allowlist of
	// #lib/activityIcon.ts, and the page names only an icon that list accepts.
	import { safeIcon, type DeclaredIcon, type SafeNode } from '#lib/activityIcon.ts';

	let { id, icon }: { id: string; icon: DeclaredIcon | undefined } = $props();

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
	<symbol {id} viewBox="0 0 16 16" fill="currentColor" {...declared.attrs}>{@render draw(declared.nodes)}</symbol>
{/if}
