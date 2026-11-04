<script lang="ts">
	// A task's bars on its run's timeline (#lib/taskTimeline.ts). A name for
	// assistive technology and bars for the eye; the numbers themselves are in
	// the row's duration and the Item card.
	import type { TimelineBar } from '#lib/taskTimeline.ts';

	let { taskId, bars, words }: { taskId: string; bars: readonly TimelineBar[]; words: string } = $props();
	const segment = (kind: TimelineBar['kind']) => (kind === 'wait' ? 'wait' : 'run');
</script>

<span class="tl" role={bars.length ? 'img' : undefined} aria-label={bars.length ? words : undefined} data-testid="task-{taskId}-timing"
	>{#each bars as bar, i (i)}<i
			class="tl-{bar.kind}"
			data-testid="task-{taskId}-{segment(bar.kind)}-segment"
			style:left="{bar.left * 100}%"
			style:width="{bar.width * 100}%"
		></i>{/each}</span
>

<style>
	.tl {
		position: relative;
		display: block;
		min-width: 0;
		height: 22px;
	}
	.tl::before {
		content: '';
		position: absolute;
		left: 0;
		right: 0;
		top: 50%;
		border-top: 1px dashed var(--line-2);
	}
	.tl i {
		position: absolute;
		top: 6px;
		height: 10px;
		min-width: 4px;
		border-radius: 99px;
	}
	.tl-wait {
		background: repeating-linear-gradient(135deg, var(--wait-mark) 0 3px, transparent 3px 6px);
		box-shadow: inset 0 0 0 1px var(--wait-mark);
	}
	.tl-ran {
		background: var(--ok-mark);
	}
	.tl-running {
		background: var(--run);
	}
	.tl-failed {
		background: var(--surface);
		box-shadow: inset 0 0 0 2px var(--fail-mark);
	}
	@media (forced-colors: active) {
		.tl i {
			forced-color-adjust: none;
		}
	}
</style>
