<script lang="ts">
	// One authored icon ($lib/icons.ts), drawn in currentColor and hidden from
	// assistive technology: the control it sits in carries the name.
	import { ICONS, type IconName } from '$lib/icons';

	let { name, class: className = 'size-4' }: { name: IconName; class?: string } = $props();
</script>

<svg
	viewBox="0 0 16 16"
	fill="none"
	stroke="currentColor"
	stroke-width="1.5"
	stroke-linecap="round"
	stroke-linejoin="round"
	aria-hidden="true"
	focusable="false"
	class="shrink-0 {className}"
>
	{#each ICONS[name] as d (d)}<path {d} />{/each}
</svg>
