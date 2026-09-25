<script lang="ts">
	// WAS THIS TASK SLOW, OR WAS IT WAITING?
	//
	// One bar, two segments: WAIT (scheduled → the Orker's start of the
	// attempt that decided the task) then RUN (that start → finished), each as
	// wide as its share of the two. The numbers come from $lib/timing, the
	// browser's copy of the derivation `orkctl run timing` prints, so the card
	// and the CLI say the same thing about the same run.
	//
	// A segment that cannot be known is drawn HATCHED with the word
	// `unavailable` and its reason on hover — never as a zero-width sliver,
	// which would read as "no time at all".
	import { formatQuantity, type Quantity, type TaskTiming } from '$lib/timing';

	interface Props {
		taskId: string;
		timing: TaskTiming;
	}

	let { taskId, timing }: Props = $props();

	const bothKnown = $derived(timing.wait.kind === 'value' && timing.run.kind === 'value');

	/** A segment's flex weight: its share when both are measured, half otherwise. */
	function weight(q: Quantity): number {
		if (!bothKnown || q.kind !== 'value') return 1;
		// A floor keeps a millisecond-long segment visible as a sliver rather
		// than vanishing; the text beside the bar carries the real number.
		return Math.max(q.ms, 1);
	}

	function reason(q: Quantity): string | undefined {
		return q.kind === 'unavailable' ? q.reason : undefined;
	}

	const segments = $derived([
		{ key: 'wait', label: 'Wait', q: timing.wait, color: 'bg-warning-300' },
		{ key: 'run', label: 'Run', q: timing.run, color: 'bg-brand-600' }
	]);
</script>

<div class="flex min-w-48 flex-1 flex-col gap-1" data-testid="task-{taskId}-timing">
	<div class="flex h-2 w-full max-w-md overflow-hidden rounded-full bg-surface-sunken" aria-hidden="true">
		{#each segments as segment (segment.key)}
			<div
				data-testid="task-{taskId}-{segment.key}-segment"
				class="h-full {segment.q.kind === 'value' ? segment.color : 'timing-hatched'}"
				style="flex: {weight(segment.q)} 1 0%"
				title={reason(segment.q) ? `${segment.label} unavailable: ${reason(segment.q)}` : undefined}
			></div>
		{/each}
	</div>
	<div class="flex flex-wrap items-center gap-x-1.5 text-xs text-fg-muted">
		{#each segments as segment, i (segment.key)}
			{#if i > 0}<span aria-hidden="true">·</span>{/if}
			<span
				data-testid="task-{taskId}-{segment.key}"
				data-ms={segment.q.kind === 'value' ? Math.round(segment.q.ms) : undefined}
				data-reason={reason(segment.q)}
				title={reason(segment.q)}
			>
				{segment.label}
				{formatQuantity(segment.q)}{#if reason(segment.q)}
					<span class="text-fg-muted"> ({reason(segment.q)})</span>{/if}
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
			var(--color-border-strong) 0 3px,
			transparent 3px 6px
		);
	}
</style>
