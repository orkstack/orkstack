<script lang="ts">
	// A SLEEPING TASK'S CARD BODY. There is no container, so there are no logs
	// and no image to inspect, and nothing outside the run ends it early: what
	// a reader needs is how long it sleeps, when it wakes, and how long is
	// left. It has no actions — an operator's cancel of the run is the only
	// way to end a sleep before its time.
	import { sleepLengthWords, sleepRemaining, sleepStateLabel, sleepWakeAt } from '#lib/sleeps.ts';
	import { humanTimeout, type Timestamp } from '#lib/awaits.ts';
	import { Status } from '#lib/gen/orkstack/v1/common_pb.ts';
	import type { SleepSpec } from '#lib/gen/orkstack/v1/orkflow_pb.ts';
	import type { TaskSleep } from '#lib/gen/orkstack/v1/run_pb.ts';

	interface Props {
		task: {
			id: string;
			status: Status;
			sleep?: TaskSleep;
			startedAt?: Timestamp;
		};
		/** The pinned definition's sleep, for its length before the snapshot's. */
		spec?: SleepSpec;
		now: Timestamp;
	}

	let { task, spec, now }: Props = $props();

	const seconds = $derived(task.sleep?.seconds || spec?.seconds || 0);
	// The snapshot's wake-up time once read; until then TASK_SLEEPING's own
	// timestamp (the page stamps it as startedAt) plus the length — the same
	// instant, because the engine stamps both from one workflow.Now.
	const wakeAt = $derived(sleepWakeAt(task, seconds, task.startedAt));
	const sleeping = $derived(task.status === Status.SLEEPING);
	// The state line says the length while sleeping, once woken and before; a
	// failed, canceled or skipped sleep says it here.
	const lengthWords = $derived(sleepLengthWords({ status: task.status, seconds }));
	const stateLabel = $derived(
		sleepStateLabel({
			status: task.status,
			seconds,
			opened: task.startedAt !== undefined || task.sleep?.wakeAt !== undefined
		})
	);

	function fmtTime(ts: Timestamp): string {
		return new Date(Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1e6)).toLocaleString();
	}
</script>

<div class="mt-3 rounded-sm bg-sleeping-bg/40 px-3 py-2 text-sm" data-testid="task-{task.id}-sleep">
	<p class="font-medium text-ink" data-testid="task-{task.id}-sleep-state">{stateLabel}</p>
	<dl class="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-xs text-ink-muted">
		<div>
			<dt class="inline">{task.status === Status.SUCCEEDED ? 'Woke' : 'Wakes'}</dt>
			<dd class="ml-1 inline text-ink-secondary" data-testid="task-{task.id}-sleep-wakes">
				{wakeAt ? fmtTime(wakeAt) : seconds ? `${humanTimeout(seconds)} after it starts sleeping` : '—'}
			</dd>
		</div>
		{#if lengthWords}
			<div>
				<dt class="inline">Length</dt>
				<dd class="ml-1 inline text-ink-secondary" data-testid="task-{task.id}-sleep-length">{lengthWords}</dd>
			</div>
		{/if}
		{#if sleeping}
			<div>
				<dt class="inline">Remaining</dt>
				<dd class="ml-1 inline font-medium text-sleeping-fg" data-testid="task-{task.id}-sleep-remaining">
					{sleepRemaining(wakeAt, now)}
				</dd>
			</div>
		{/if}
	</dl>
	<p class="mt-2 text-xs text-ink-muted">
		A sleep runs no container and holds no Orker; nothing outside the run wakes it early. Cancelling the run ends it.
	</p>
</div>
