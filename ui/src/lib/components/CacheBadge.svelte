<script lang="ts">
	// "This result was reused" — or why it was not.
	//
	// A cached task's card carries the memo decision of the attempt that
	// produced its outcome (TaskRun.cache, docs/DSL.md "Caching"): a hit links
	// to the run whose container produced the bytes; a miss says why it ran; a
	// bypass says the run asked not to look. A cached task that SUCCEEDED with
	// no decision at all ran on an Orker that predates caching. Nothing is
	// drawn for any other task. The wording is #lib/cache.ts's, shared with the
	// CLI.
	import type { CacheResult } from '#lib/gen/orkstack/v1/activity_pb.ts';
	import { cacheOutcomeLabel, cacheOutcomeWord, cacheSourceHref, missReasonSentence } from '#lib/cache.ts';

	interface Props {
		cache?: CacheResult;
		/** The pinned definition declares cache: and the task SUCCEEDED. */
		declared?: boolean;
		succeeded?: boolean;
		namespace: string;
		testid: string;
	}

	let { cache, declared = false, succeeded = false, namespace, testid }: Props = $props();

	const outcome = $derived(cache ? cacheOutcomeWord(cache.outcome) : declared && succeeded ? 'unreported' : '');
</script>

{#if outcome === 'hit'}
	<span
		data-testid={testid}
		data-outcome="hit"
		data-tip="No container ran: this task's result was reused from an identical earlier run."
	>
		{cacheOutcomeLabel(cache?.outcome)}
		from
		<a data-testid="{testid}-source" href={cacheSourceHref(namespace, cache?.source)} class="break-all">{cache?.source?.runId}</a>, no container
	</span>
{:else if outcome === 'miss'}
	<span
		data-testid={testid}
		data-outcome="miss"
		data-tip={missReasonSentence(cache?.missReason ?? '', cache?.missDetail ?? '')}
		>{cacheOutcomeLabel(cache?.outcome)}</span
	>
{:else if outcome === 'bypassed'}
	<span
		data-testid={testid}
		data-outcome="bypassed"
		data-tip="The run was submitted with Ignore cached results (--no-cache): the container ran and its memo was refreshed."
		>{cacheOutcomeLabel(cache?.outcome)}</span
	>
{:else if outcome === 'unreported'}
	<span
		data-testid={testid}
		data-outcome="unreported"
		data-tip="the Orker that ran this task predates caching"
		>Cache not reported</span
	>
{/if}
