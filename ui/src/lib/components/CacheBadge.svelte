<script lang="ts">
	// "This result was reused" — or why it was not.
	//
	// A cached task's card carries the memo decision of the attempt that
	// produced its outcome (TaskRun.cache, docs/DSL.md "Caching"): a hit links
	// to the run whose container produced the bytes; a miss says why it ran; a
	// bypass says the run asked not to look. A cached task that SUCCEEDED with
	// no decision at all ran on an Orker that predates caching. Nothing is
	// drawn for any other task. The wording is $lib/cache's, shared with the
	// CLI.
	import type { CacheResult } from '$lib/gen/orkstack/v1/activity_pb';
	import { cacheOutcomeLabel, cacheOutcomeWord, cacheSourceHref, missReasonSentence } from '$lib/cache';

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
		title="No container ran: this task's result was reused from an identical earlier run."
		class="inline-flex items-center gap-1 rounded-full border border-brand-200 bg-brand-50 px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-brand-700 leading-4"
	>
		{cacheOutcomeLabel(cache?.outcome)}
		<span class="text-fg-muted">from</span>
		<a
			data-testid="{testid}-source"
			href={cacheSourceHref(namespace, cache?.source)}
			class="font-mono hover:underline">{cache?.source?.runId}</a
		>
	</span>
{:else if outcome === 'miss'}
	<span
		data-testid={testid}
		data-outcome="miss"
		title={missReasonSentence(cache?.missReason ?? '', cache?.missDetail ?? '')}
		class="inline-flex items-center rounded-full border border-border bg-surface-sunken px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-fg-muted leading-4"
		>{cacheOutcomeLabel(cache?.outcome)}</span
	>
{:else if outcome === 'bypassed'}
	<span
		data-testid={testid}
		data-outcome="bypassed"
		title="The run was submitted with Ignore cached results (--no-cache): the container ran and its memo was refreshed."
		class="inline-flex items-center rounded-full border border-border bg-surface-sunken px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-fg-muted leading-4"
		>{cacheOutcomeLabel(cache?.outcome)}</span
	>
{:else if outcome === 'unreported'}
	<span
		data-testid={testid}
		data-outcome="unreported"
		title="the Orker that ran this task predates caching"
		class="inline-flex items-center rounded-full border border-border bg-surface-sunken px-2 py-0.5 text-[11px] font-medium whitespace-nowrap text-fg-muted leading-4"
		>Cache not reported</span
	>
{/if}
