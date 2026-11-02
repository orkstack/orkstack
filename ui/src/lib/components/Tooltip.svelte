<script lang="ts">
	// The one shared tooltip element every `data-tip` control shows its words
	// in (#lib/tooltip.ts). The layout renders exactly one.
	import { onMount } from 'svelte';
	import { installTooltips } from '#lib/tooltip.ts';

	let tip = $state<HTMLElement>();
	onMount(() => (tip ? installTooltips(document, tip) : undefined));
</script>

<div bind:this={tip} id="tt" class="tt" role="tooltip" popover="manual"></div>
