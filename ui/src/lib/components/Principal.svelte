<script lang="ts">
	// A principal, always with its tag: "verified" when OrkStack verified it,
	// "asserted" when a gateway only asserted it (AGENTS.md §7). `verified` is
	// required, so no caller can leave the difference unsaid.
	import { principalMark } from '#lib/principal.ts';

	let { name, verified, method = '', testid }: { name: string; verified: boolean; method?: string; testid?: string } = $props();
	const mark = $derived(principalMark(verified, method));
</script>

<!-- The tag is a tab stop on purpose: its explanation must reach a keyboard (WCAG 1.4.13). -->
<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
<span class="inline-flex flex-wrap items-center gap-1.5" data-testid={testid}
	><span class="min-w-0 break-all">{name}</span><span
		class="atag"
		class:atag-verified={mark.word === 'verified'}
		tabindex="0"
		data-tip={mark.tip}
		data-principal-mark={mark.word}>{mark.word}<span class="sr">: {mark.sr}</span></span
	></span
>
