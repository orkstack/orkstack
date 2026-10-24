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
	import { plainFailure } from '$lib/failureText';
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
		/**
		 * A run that ended without failing (an operator's cancel) says why in
		 * ink, as a NOTE: red is for failure and attention only.
		 */
		tone?: 'failure' | 'note';
	}

	let { error, testid, compact = false, retryState, attemptCount = 0, bare = false, tone = 'failure' }: Props = $props();

	// The sentence without the execution service's wrapper, and the reason
	// code it led with. A run's RUN_FAILED wraps its task's reason: the panel
	// leads with that reason and keeps the run's own code beside it.
	const plain = $derived(plainFailure(error.message));
	const reason = $derived(plain.code && plain.code !== error.code ? plain.code : undefined);
	const failure = $derived(tone === 'failure');

	const retryNote = $derived(retryStateLabel(retryState, attemptCount));

	const category = $derived(error.category ?? FailureCategory.UNSPECIFIED);
</script>

<!--
	A task's failure is one tinted ground with red ink, the code in the mono
	after a filled mark — no rule of its own, so the task row is the only
	other line round it. Inside the run's revision cloud (`bare`) it has no
	ground: the cloud is the frame. A NOTE (a canceled run) is ink on the
	note ground.
-->
<div
	data-testid={testid}
	role={failure ? 'alert' : 'status'}
	data-tone={tone}
	class="{bare ? '' : failure ? 'rounded-sm bg-red-tint px-3 py-2' : 'note px-3 py-2'} text-sm"
>
	<div class="flex flex-wrap items-center gap-x-3 gap-y-1">
		{#if !failure}<span class="caps text-ink-muted">Note</span>{/if}
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
		{#if reason}
			<span class="severity-error font-mono text-sm font-semibold" data-testid="{testid}-reason">{reason}</span>
		{/if}
		{#if error.code}
			<span
				class="font-mono {reason ? 'text-xs text-ink-secondary' : failure ? 'severity-error text-sm font-semibold' : 'text-sm font-semibold text-ink'}"
				data-testid="{testid}-code">{error.code}</span
			>
		{/if}
		<span class="text-xs {error.retryable ? 'text-ink' : 'text-ink-secondary'}" data-testid="{testid}-retryable">
			{error.retryable ? 'retryable' : 'not retryable'}
		</span>
		{#if retryNote}
			<span class="text-xs text-ink-secondary" data-testid="{testid}-retry-state">{retryNote}</span>
		{/if}
	</div>

	{#if !compact && category !== FailureCategory.UNSPECIFIED}
		<p class="mt-2 text-xs text-ink-secondary" data-testid="{testid}-blurb">
			{failureCategoryBlurb(category)}
		</p>
	{/if}

	{#if plain.text}
		<p class="mt-1.5 max-w-prose break-words whitespace-pre-wrap text-ink" data-testid="{testid}-message">
			{plain.text}
		</p>
	{/if}

	{#if error.details && !compact && !plain.text.includes(error.details)}
		<p class="mt-1.5 font-mono text-xs break-words text-ink-secondary" data-testid="{testid}-details">{error.details}</p>
	{/if}
</div>
