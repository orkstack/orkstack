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
	}

	let { error, testid, compact = false, retryState, attemptCount = 0 }: Props = $props();

	const retryNote = $derived(retryStateLabel(retryState, attemptCount));

	const category = $derived(error.category ?? FailureCategory.UNSPECIFIED);
</script>

<div
	data-testid={testid}
	role="alert"
	class="rounded-lg border border-failed-border bg-surface shadow-xs"
>
	<div
		class="flex flex-wrap items-center gap-2 rounded-t-lg border-b border-failed-border bg-failed-bg px-5 py-3 text-sm font-semibold text-failed-fg"
	>
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
				class="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-medium {failureCategoryClasses(
					category
				)}"
			>
				{failureCategoryLabel(category)}
			</span>
		{/if}
		{#if error.code}
			<span class="font-mono text-xs font-normal text-failed-fg/80" data-testid="{testid}-code">
				{error.code}
			</span>
		{/if}
		<span
			class="text-xs font-normal {error.retryable ? 'text-warning-700' : 'text-fg-muted'}"
			data-testid="{testid}-retryable"
		>
			{error.retryable ? 'retryable' : 'not retryable'}
		</span>
		{#if retryNote}
			<span class="text-xs font-normal text-fg-muted" data-testid="{testid}-retry-state">
				{retryNote}
			</span>
		{/if}
	</div>

	<div class="p-5 text-sm">
		{#if !compact && category !== FailureCategory.UNSPECIFIED}
			<p class="mb-1.5 text-xs text-fg-muted" data-testid="{testid}-blurb">
				{failureCategoryBlurb(category)}
			</p>
		{/if}

		{#if error.message}
			<p class="break-words whitespace-pre-wrap text-fg" data-testid="{testid}-message">
				{error.message}
			</p>
		{/if}

		{#if error.details && !compact}
			<pre
				class="mt-2 overflow-x-auto rounded-md border border-border-subtle bg-surface-sunken p-2 font-mono text-xs text-fg-secondary"
				data-testid="{testid}-details">{error.details}</pre>
		{/if}
	</div>
</div>
