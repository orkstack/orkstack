<script lang="ts">
	// "This task did not succeed the first time."
	//
	// A Temporal retry is INVISIBLE in a task's status: the workflow only ever
	// sees the final outcome, so a task that failed twice and passed on its
	// third try renders exactly like one that passed immediately. This pill is
	// the whole of that difference on the overview, sitting beside the status
	// pill it qualifies — and it is drawn only when there is something to say,
	// so an ordinary run carries no extra chrome at all.
	//
	// The wording is decided in $lib/attempts, not here, because the run
	// header's chip and the CLI's table count the same tries and must agree.
	import type { TaskAttempt } from '$lib/gen/orkstack/v1/run_pb';
	import type { FlakeGroup } from '$lib/gen/orkstack/v1/stats_pb';
	import { retryBadgeLabel } from '$lib/attempts';
	import { flakeBadgeLabel, flakeHistoryTitle } from '$lib/flakes';

	interface Props {
		attempts: TaskAttempt[];
		/** Terminal tasks are described in the past tense; running ones by the try in flight. */
		terminal: boolean;
		testid: string;
		/**
		 * This task's retry history across its orkflow's recent runs, once a
		 * reader asked for it (the run page's "Retry history"). Undefined
		 * renders exactly as the badge always has.
		 */
		history?: FlakeGroup;
		/** The window the history covers, in words ("7 days"). */
		historyWindow?: string;
	}

	let { attempts, terminal, testid, history, historyWindow = '7 days' }: Props = $props();

	// The in-run label is unchanged and always wins. Only a run that did NOT
	// retry this task, of a task whose history says it has retried elsewhere,
	// gains a badge: "flaky N%".
	const inRun = $derived(retryBadgeLabel(attempts, terminal));
	const label = $derived(flakeBadgeLabel(inRun, history));
	const title = $derived(
		(inRun
			? 'Temporal retried this task. Open Inspect for the outcome and error of every attempt.'
			: 'This task did not retry in this run, but it has in others') + flakeHistoryTitle(history, historyWindow)
	);
</script>

{#if label}
	<span
		data-testid={testid}
		data-flaky={inRun ? undefined : 'true'}
		{title}
		class="inline-flex items-center rounded-full border border-warning-200 bg-warning-50 px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-warning-700 leading-4"
	>
		{label}
	</span>
{/if}
