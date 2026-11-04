<script lang="ts">
	// SvelteKit keeps a route's component across navigations between its own
	// params, and everything a run page holds (its tasks, its selection, its live
	// state) is one run's. A link from one run to another gets a fresh page.
	import RunView from './RunView.svelte';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

{#key data.runId}
	<RunView {data} />
{/key}
