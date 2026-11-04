<script lang="ts">
	// A principal, always with its asserted tag.
	import Asserted from './Asserted.svelte';

	let { name, testid }: { name: string; testid?: string } = $props();
</script>

<span class="inline-flex flex-wrap items-center gap-1.5" data-testid={testid}><span class="min-w-0 break-all">{name}</span><Asserted /></span>
