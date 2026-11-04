<script lang="ts">
	// The task map's key, under its frame: one entry per status present, with
	// its drawn mark and its count. Pressing one moves the map's cursor to that
	// status's first cell. A status whose cells the run ended before they ever
	// started says so ("never started"), in the same words as their rows.
	import type { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import { statusLabel } from '#lib/status.ts';
	import { statusClassWord } from '#lib/statusMarks.ts';
	import { STATUS_ORDER } from '#lib/taskMap.ts';

	let {
		counts,
		neverStarted = new Map(),
		onpick,
		compact = false
	}: {
		/** A small run's key: the words alone, its cells already countable by eye. */
		compact?: boolean;
		counts: Map<Status, number>;
		/** Of each status's cells, how many never started. */
		neverStarted?: Map<Status, number>;
		onpick: (status: Status) => void;
	} = $props();

	function never(status: Status): string {
		const n = neverStarted.get(status) ?? 0;
		if (n <= 0) return '';
		return n >= (counts.get(status) ?? 0) ? 'never started' : `${fmt.format(n)} never started`;
	}

	const fmt = new Intl.NumberFormat('en-US');
	const present = $derived(STATUS_ORDER.filter((s) => (counts.get(s) ?? 0) > 0));
</script>

<ul data-testid="task-map-legend" class="flex flex-wrap items-center gap-x-4 gap-y-1" aria-label="Task map key">
	{#each present as status (status)}
		<li>
			<button
				type="button"
				data-testid="task-map-legend-{statusClassWord(status)}"
				onclick={() => onpick(status)}
				class="inline-flex min-h-6 items-center gap-1.5 text-xs text-ink-secondary hover:text-ink max-sm:min-h-10"
			>
				<span class="swatch is-{statusClassWord(status)}" aria-hidden="true"></span>
				<span class={compact ? 'lowercase' : 'font-semibold'}>{statusLabel(status)}</span>
				<span class={compact ? 'sr' : 'figure'}>{fmt.format(counts.get(status) ?? 0)}</span>
				{#if never(status)}<span class="text-ink-muted" data-testid="task-map-legend-{statusClassWord(status)}-note">· {never(status)}</span>{/if}
			</button>
		</li>
	{/each}
</ul>

<style>
	.swatch {
		display: inline-block;
		flex: none;
		width: 10px;
		height: 10px;
		border-radius: 3px;
		background: var(--surface);
		box-shadow: inset 0 0 0 1.5px var(--pend-mark);
		forced-color-adjust: none;
	}
	.is-succeeded {
		background: var(--ok-mark);
		box-shadow: none;
	}
	.is-running {
		background: var(--run);
		box-shadow: none;
		border-radius: 50%;
	}
	.is-failed {
		background: var(--fail-mark);
		box-shadow: none;
		border-radius: 0;
		clip-path: polygon(50% 0, 100% 100%, 0 100%);
	}
	.is-canceled {
		box-shadow: inset 0 0 0 1.5px var(--cancel);
		background: linear-gradient(135deg, transparent 42%, var(--cancel) 42% 58%, transparent 58%);
	}
	.is-skipped {
		box-shadow: inset 0 0 0 1.5px var(--cancel);
	}
	.is-awaiting {
		border-radius: 1px;
		rotate: 45deg;
		scale: 0.8;
	}
</style>
