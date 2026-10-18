<script lang="ts">
	// What an Operate page covers — the deployment, the namespace in the
	// picker, or every namespace — said once, as a NOTE under the page's head.
	// The testid sits on the sentence alone, so the label beside it is never
	// part of the text a reader (or a spec) quotes.
	import type { Snippet } from 'svelte';

	interface Props {
		testid: string;
		children: Snippet;
	}

	let { testid, children }: Props = $props();
</script>

<div class="note mb-6 flex max-w-prose gap-3 px-3 py-2 text-sm">
	<span class="caps shrink-0 pt-0.5 text-ink-muted">Note</span>
	<div class="min-w-0 text-ink-secondary" data-testid={testid}>{@render children()}</div>
</div>
