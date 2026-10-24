<script lang="ts">
	// The head of every page: its title, an optional line of facts, and the
	// page's primary action top right — the one ink-filled button on the page.
	import type { Snippet } from 'svelte';

	interface Props {
		title: string;
		testid?: string;
		/** The title in the mono: a run id, a digest. */
		mono?: boolean;
		/**
		 * The title as drawn below 640 px ("ork-…873153"), where the whole of
		 * a long id would take the first screen. The full title stays the
		 * heading for assistive technology, find-in-page and copy.
		 */
		short?: string;
		meta?: Snippet;
		actions?: Snippet;
	}

	let { title, testid = undefined, mono = false, short = undefined, meta = undefined, actions = undefined }: Props = $props();
</script>

<header class="mb-6 flex flex-wrap items-start justify-between gap-x-6 gap-y-3 border-b border-rule pb-4">
	<div class="min-w-0">
		<h1
			data-testid={testid}
			class="text-xl font-semibold tracking-tight text-ink {mono ? 'font-mono break-all' : ''} {short ? 'max-sm:sr-only' : ''}"
		>
			{title}
		</h1>
		{#if short}
			<p
				aria-hidden="true"
				title={title}
				class="text-xl font-semibold tracking-tight whitespace-nowrap text-ink sm:hidden {mono ? 'font-mono' : ''}"
			>
				{short}
			</p>
		{/if}
		{#if meta}<div class="mt-1 text-sm text-ink-secondary">{@render meta()}</div>{/if}
	</div>
	{#if actions}<div class="flex max-w-full min-w-0 flex-wrap items-center gap-2">{@render actions()}</div>{/if}
</header>
