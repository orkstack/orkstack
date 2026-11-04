<script lang="ts">
	// An image's own words (ActivityDocs): up to its limit in full, past it
	// clamped to a few lines, with More opening the rest. A missing description
	// is the caller's to leave out: this draws what it is given.
	import { docLong, docParts } from '#lib/docText.ts';

	let {
		text,
		limit,
		lines = 2,
		testid = undefined,
		subject = '',
		class: className = ''
	}: { text: string; limit: number; lines?: number; testid?: string; subject?: string; class?: string } = $props();

	const parts = $derived(docParts(text));
	const long = $derived(docLong(text, limit));
	let open = $state(false);
</script>

<span class="doc {className}" data-testid={testid}>
	<span class="doc-text" class:clamped={long && !open} style:-webkit-line-clamp={long && !open ? lines : undefined}
		>{#each parts as part, i (i)}{#if part.code}<code>{part.text}</code>{:else}{part.text}{/if}{/each}</span
	>{#if long}<button type="button" class="doc-more" aria-expanded={open} onclick={() => (open = !open)}>{open ? 'Less' : 'More'}{#if subject}<span class="sr">{' '}about {subject}</span>{/if}</button>{/if}
</span>

<style>
	.doc {
		display: block;
		min-width: 0;
	}
	.doc-text {
		overflow-wrap: anywhere;
	}
	.clamped {
		display: -webkit-box;
		overflow: hidden;
		-webkit-box-orient: vertical;
	}
	.doc-more {
		padding: 0;
		border: 0;
		background: none;
		color: var(--link);
		font: inherit;
		font-weight: var(--font-weight-medium);
		text-decoration: underline;
		text-underline-offset: var(--underline-offset);
		cursor: pointer;
	}
</style>
