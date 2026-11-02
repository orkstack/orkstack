<script lang="ts">
	// A relative time ("2 min ago") that a keyboard reaches: focusable, its
	// exact instant both its tooltip and its screen-reader text (brief §5,
	// tooltip rule 8).
	import type { Timestamp } from '@bufbuild/protobuf/wkt';
	import { timestampDate } from '@bufbuild/protobuf/wkt';
	import { relativeTime, exactTime } from '#lib/when.ts';

	let { at, now, testid }: { at: Timestamp | Date | undefined; now?: number; testid?: string } = $props();
	const date = $derived(at === undefined ? undefined : at instanceof Date ? at : timestampDate(at));
</script>

{#if date}
	<!-- A relative time is a tab stop on purpose: its exact instant must reach a keyboard (brief §5, tooltip rule 8). -->
	<!-- svelte-ignore a11y_no_noninteractive_tabindex -->
	<time datetime={date.toISOString()} tabindex="0" data-tip={exactTime(date)} data-testid={testid}
		><span aria-hidden="true">{relativeTime(date, now)}</span><span class="sr">{exactTime(date)}</span></time
	>
{:else}
	<span data-testid={testid}>—</span>
{/if}
