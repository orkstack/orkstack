<script lang="ts">
	// A terminal failure, said the way the system says it (design §8.3).
	//
	// The engine stopped flattening everything into TASK_FAILED with a
	// stringified Go error: Error now carries a machine-readable `code` (the
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

	interface Props {
		error: OrkError;
		testid: string;
		/** Compact drops the explanatory blurb, for a dense list row. */
		compact?: boolean;
	}

	let { error, testid, compact = false }: Props = $props();

	const category = $derived(error.category ?? FailureCategory.UNSPECIFIED);
</script>

<div
	data-testid={testid}
	role="alert"
	class="rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
>
	<div class="mb-1.5 flex flex-wrap items-center gap-2">
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
				class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ring-1 {failureCategoryClasses(
					category
				)}"
			>
				{failureCategoryLabel(category)}
			</span>
		{/if}
		{#if error.code}
			<span class="font-mono text-xs text-rose-200/80" data-testid="{testid}-code">
				{error.code}
			</span>
		{/if}
		<span
			class="text-xs {error.retryable ? 'text-amber-300/80' : 'text-zinc-400'}"
			data-testid="{testid}-retryable"
		>
			{error.retryable ? 'retryable' : 'not retryable'}
		</span>
	</div>

	{#if !compact && category !== FailureCategory.UNSPECIFIED}
		<p class="mb-1.5 text-xs text-rose-200/70" data-testid="{testid}-blurb">
			{failureCategoryBlurb(category)}
		</p>
	{/if}

	{#if error.message}
		<p class="break-words whitespace-pre-wrap" data-testid="{testid}-message">{error.message}</p>
	{/if}

	{#if error.details && !compact}
		<pre
			class="mt-2 overflow-x-auto rounded bg-zinc-950/60 p-2 text-xs text-zinc-400"
			data-testid="{testid}-details">{error.details}</pre>
	{/if}
</div>
