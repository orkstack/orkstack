<script lang="ts">
	// Every icon of the Harbour set, drawn once as a <symbol> for the whole
	// app (the layout renders this one sprite); each Icon is a <use> of one.
	// Elements are built from #lib/icons.ts's data, never parsed from markup.
	import { ICONS, type IconName } from '#lib/icons.ts';

	const names = Object.keys(ICONS) as IconName[];
</script>

<svg class="sprite" aria-hidden="true" focusable="false">
	{#each names as name (name)}
		<symbol id="i-{name}" viewBox="0 0 16 16">
			{#each ICONS[name] as [tag, attrs], i (i)}<svelte:element this={tag} {...attrs} />{/each}
		</symbol>
	{/each}
</svg>

<style>
	.sprite {
		position: absolute;
		width: 0;
		height: 0;
		overflow: hidden;
	}
</style>
