<script lang="ts">
	// A terminal failure, said the way the system says it.
	//
	// Error is not a stringified Go error behind TASK_FAILED: it carries a
	// machine-readable `code` (the
	// §8.2 reason), a human `message`, and a `category` — which of the five
	// AGENTS.md §4 kinds of failure this was. All three are rendered, because
	// they answer three different questions and a reader needs all of them:
	// the category says WHO can fix it, the code says WHAT rule was broken,
	// and the message says which file or value broke it.
	//
	// `retryable` is shown too. It is the difference between "this will be
	// tried again" and "this run is over", and reading it off the category is
	// not safe — a task's own retry: policy can override the default.
	import { FailureCategory } from '$lib/gen/orkstack/v1/common_pb';
	import type { Error as OrkError } from '$lib/gen/orkstack/v1/common_pb';
	import {
		categoryTestid,
		failureCategoryBlurb,
		failureCategoryClasses,
		failureCategoryLabel
	} from '$lib/activity';
	import { retryStateLabel } from '$lib/attempts';
	import { RetryState } from '$lib/gen/orkstack/v1/run_pb';

	interface Props {
		error: OrkError;
		testid: string;
		/** Compact drops the explanatory blurb, for a dense list row. */
		compact?: boolean;
		/**
		 * retryState is why the platform stopped trying, when the snapshot
		 * carries it. `retryable` above says whether this failure COULD be
		 * retried; this says what actually happened to the retries — a budget
		 * that ran out reads very differently from a failure nothing would ever
		 * have tried again.
		 */
		retryState?: RetryState;
		/** How many tries the attempt timeline knows of, for the exhausted-budget phrasing. */
		attemptCount?: number;
		/** Inside the run's revision cloud: no frame of its own. */
		bare?: boolean;
	}

	let { error, testid, compact = false, retryState, attemptCount = 0, bare = false }: Props = $props();

	const retryNote = $derived(retryStateLabel(retryState, attemptCount));

	const category = $derived(error.category ?? FailureCategory.UNSPECIFIED);
</script>

<!--
	Red ink and a 1 px red rule, the code in the mono after a filled mark —
	never a tinted band. Inside the run's revision cloud (`bare`) it drops its
	own rule: the cloud is the frame.
-->
<div
	data-testid={testid}
	role="alert"
	class="{bare ? '' : 'rounded-sm border border-red bg-paper px-4 py-3'} text-sm"
>
	<div class="flex flex-wrap items-center gap-x-3 gap-y-1">
		<!--
			The badge is shown only when the wire actually carries a category.
			A run-level RUN_FAILED wrapper carries FAILURE_CATEGORY_UNSPECIFIED —
			the categorised error is the TASK's — and painting "Uncategorised"
			there would assert a classification nothing made.
		-->
		{#if category !== FailureCategory.UNSPECIFIED}
			<span
				data-testid="{testid}-category"
				data-category={categoryTestid(category)}
				title={failureCategoryBlurb(category)}
				class="stamp {failureCategoryClasses(category)}"
			>
				{failureCategoryLabel(category)}
			</span>
		{/if}
		{#if error.code}
			<span class="severity-error font-mono text-sm font-semibold" data-testid="{testid}-code">{error.code}</span>
		{/if}
		<span class="text-xs {error.retryable ? 'text-ink' : 'text-ink-muted'}" data-testid="{testid}-retryable">
			{error.retryable ? 'retryable' : 'not retryable'}
		</span>
		{#if retryNote}
			<span class="text-xs text-ink-muted" data-testid="{testid}-retry-state">{retryNote}</span>
		{/if}
	</div>

	{#if !compact && category !== FailureCategory.UNSPECIFIED}
		<p class="mt-2 text-xs text-ink-muted" data-testid="{testid}-blurb">
			{failureCategoryBlurb(category)}
		</p>
	{/if}

	{#if error.message}
		<p class="mt-1.5 max-w-prose break-words whitespace-pre-wrap text-ink" data-testid="{testid}-message">
			{error.message}
		</p>
	{/if}

	{#if error.details && !compact}
		<pre
			class="mt-2 overflow-x-auto rounded-sm border border-rule-faint bg-paper-sunken p-2 font-mono text-xs text-ink-secondary"
			data-testid="{testid}-details">{error.details}</pre>
	{/if}
</div>
