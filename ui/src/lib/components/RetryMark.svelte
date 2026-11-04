<script lang="ts">
	// Attempts as a retry mark with the count inside, shown only from two
	// attempts on; one attempt is just "1". The words are for assistive
	// technology and the tooltip.
	import Icon from './Icon.svelte';

	let { attempts, testid }: { attempts: number; testid?: string } = $props();
	const words = $derived(`${attempts} ${attempts === 1 ? 'attempt' : 'attempts'}`);
</script>

{#if attempts >= 2}
	<span class="retry" data-tip={words} data-testid={testid}
		><Icon name="retry" class="" /><b aria-hidden="true">{attempts}</b><span class="sr">{words}</span></span
	>
{:else}
	<span class="num" data-testid={testid}><span aria-hidden="true">{attempts}</span><span class="sr">{words}</span></span>
{/if}
