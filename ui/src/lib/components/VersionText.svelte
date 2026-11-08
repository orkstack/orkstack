<script lang="ts">
	// A version in running text: its short form, the same as a plate's. A
	// shortened one is a tab stop, so its whole value reaches a keyboard and a
	// screen reader through the one tooltip, as a relative time's instant does.
	import { shortVersion, versionTip } from '#lib/version.ts';

	let { version, class: className = '', testid }: { version: string; class?: string; testid?: string } = $props();
	const tip = $derived(versionTip(version));
</script>

<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
<span class={className} data-testid={testid} data-tip={tip} tabindex={tip ? 0 : undefined}>{shortVersion(version)}</span>
