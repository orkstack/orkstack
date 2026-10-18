<script lang="ts">
	// The task map's key, under its frame: one entry per status present, with
	// its drawn mark and its count. Pressing one moves the map's cursor to that
	// status's first cell.
	import type { Status } from '$lib/gen/orkstack/v1/common_pb';
	import { statusLabel } from '$lib/status';
	import { statusClassWord, statusMark } from '$lib/statusMarks';
	import { STATUS_ORDER } from '$lib/taskMap';

	let { counts, onpick }: { counts: Map<Status, number>; onpick: (status: Status) => void } = $props();

	const fmt = new Intl.NumberFormat('en-US');
	const present = $derived(STATUS_ORDER.filter((s) => (counts.get(s) ?? 0) > 0));
</script>

<ul data-testid="task-map-legend" class="flex flex-wrap items-center gap-x-4 gap-y-1" aria-label="Task map key">
	{#each present as status (status)}
		{@const mark = statusMark(status)}
		<li>
			<button
				type="button"
				data-testid="task-map-legend-{statusClassWord(status)}"
				onclick={() => onpick(status)}
				class="inline-flex min-h-6 items-center gap-1.5 text-xs text-ink-secondary hover:text-ink"
			>
				<span class="stamp border-transparent bg-transparent p-0 {mark.inner ? '' : mark.className}" aria-hidden="true"
					>{#if mark.inner}<span class={mark.className}></span>{/if}</span
				>
				<span class="caps">{statusLabel(status)}</span>
				<span class="figure">{fmt.format(counts.get(status) ?? 0)}</span>
			</button>
		</li>
	{/each}
</ul>
