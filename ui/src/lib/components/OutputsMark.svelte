<script lang="ts">
	// Whether a run's outputs are the ones its Orkflow declares: a word in the
	// quiet tag beside the Outputs heading, the asserted tag's pattern, so its
	// explanation reaches a keyboard and a screen reader and nothing rests on colour.
	let { declared }: { declared: boolean } = $props();
	const why = $derived(
		declared
			? 'This Orkflow declares its outputs, and these are their values.'
			: 'This Orkflow declares no outputs, so these are the values of the tasks no other task needs.'
	);
</script>

<!-- A tab stop on purpose: the tag's explanation must reach a keyboard (WCAG 1.4.13). -->
<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
<span class="atag" tabindex="0" data-tip={why} data-testid="run-output-mark"
	>{declared ? 'declared' : 'not declared'}<span class="sr">: {why}</span></span
>
