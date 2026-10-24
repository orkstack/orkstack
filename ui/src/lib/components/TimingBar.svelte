<script lang="ts">
	// WAS THIS TASK SLOW, OR WAS IT WAITING — AND WHEN, IN ITS RUN?
	//
	// The task on its run's timeline, the one every row of the run shares: a
	// 4 px mark from when it was scheduled to when it finished, placed at its
	// offset in the run and as wide as its share of the run's elapsed time
	// ($lib/timing timelinePlacement), so the rows read together as when each
	// task ran. The WAIT for an Orker is the paler head of the mark, the RUN
	// on one its solid body. The numbers beside it come from $lib/timing, the
	// browser's copy of the derivation `orkctl run timing` prints, so the
	// card and the CLI say the same thing about the same run.
	//
	// A segment that cannot be known is drawn HATCHED with the word
	// `unavailable` and its reason on hover — never as a zero-width sliver,
	// which would read as "no time at all".
	import { formatQuantity, type TaskTiming, type TimelineSegment } from '$lib/timing';

	interface Props {
		taskId: string;
		timing: TaskTiming;
		/** Where the task sits on its run's timeline; null when that cannot be placed. */
		placement: TimelineSegment[] | null;
	}

	let { taskId, timing, placement }: Props = $props();

	function reason(key: 'wait' | 'run'): string | undefined {
		const q = timing[key];
		return q.kind === 'unavailable' ? q.reason : undefined;
	}

	const labels = { wait: 'Wait', run: 'Run' } as const;
	const segments = $derived([
		{ key: 'wait' as const, label: labels.wait, q: timing.wait },
		{ key: 'run' as const, label: labels.run, q: timing.run }
	]);
</script>

<div class="flex w-full flex-col gap-1 sm:w-72" data-testid="task-{taskId}-timing">
	{#if placement}
		<!-- The run's span: a hairline the task's mark sits on. -->
		<div class="relative h-1 w-full border-b border-rule-faint" aria-hidden="true">
			{#each placement as segment (segment.key)}
				<div
					data-testid="task-{taskId}-{segment.key}-segment"
					data-known={segment.known}
					class="absolute top-0 h-1 {!segment.known || reason(segment.key)
						? 'timing-hatched'
						: segment.key === 'wait'
							? 'bg-rule-strong'
							: 'bg-mark'}"
					style:left="{segment.left * 100}%"
					style:width="max({segment.width * 100}%, 2px)"
					title={reason(segment.key) ? `${labels[segment.key]} unavailable: ${reason(segment.key)}` : undefined}
				></div>
			{/each}
		</div>
	{/if}
	<div class="flex flex-wrap items-center gap-x-1.5 text-xs text-ink-muted">
		{#each segments as segment, i (segment.key)}
			{#if i > 0}<span aria-hidden="true">·</span>{/if}
			<span
				data-testid="task-{taskId}-{segment.key}"
				data-ms={segment.q.kind === 'value' ? Math.round(segment.q.ms) : undefined}
				data-reason={reason(segment.key)}
				title={reason(segment.key)}
			>
				{segment.label}
				{formatQuantity(segment.q)}{#if reason(segment.key)}
					<span class="text-ink-muted"> ({reason(segment.key)})</span>{/if}
			</span>
		{/each}
		{#if timing.preContract && timing.total.kind === 'value'}
			<span aria-hidden="true">·</span>
			<span data-testid="task-{taskId}-total" title="this run was recorded before scheduled time existed; total is dispatch to finish">
				Total {formatQuantity(timing.total)} (dispatch to finish)
			</span>
		{/if}
	</div>
</div>

<style>
	.timing-hatched {
		background-image: repeating-linear-gradient(
			45deg,
			var(--color-rule-strong) 0 3px,
			transparent 3px 6px
		);
	}
</style>
