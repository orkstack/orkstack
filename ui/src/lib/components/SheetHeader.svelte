<script lang="ts">
	// The head of every page: its title, an optional line of facts, and the
	// page's primary action top right — the one ink-filled button on the page.
	import type { Snippet } from 'svelte';

	interface Props {
		title: string;
		testid?: string;
		/** The title in the mono: a run id, a digest. */
		mono?: boolean;
		meta?: Snippet;
		actions?: Snippet;
	}

	let { title, testid = undefined, mono = false, meta = undefined, actions = undefined }: Props = $props();
</script>

<header class="mb-6 flex flex-wrap items-start justify-between gap-x-6 gap-y-3 border-b border-rule pb-4">
	<div class="min-w-0">
		<h1
			data-testid={testid}
			class="text-xl font-semibold tracking-tight text-ink {mono ? 'font-mono break-all' : ''}"
		>
			{title}
		</h1>
		{#if meta}<div class="mt-1 text-sm text-ink-secondary">{@render meta()}</div>{/if}
	</div>
	{#if actions}<div class="flex shrink-0 flex-wrap items-center gap-2">{@render actions()}</div>{/if}
</header>
