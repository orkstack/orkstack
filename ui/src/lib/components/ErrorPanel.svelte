<script lang="ts">
	// A failed request states what failed and offers the way back: the retry
	// re-issues the request that failed, in place, without a page reload.
	//
	// The retry is not optional decoration. A view that renders an error string
	// and nothing else leaves the user with the browser's reload button as the
	// only recovery, which re-runs every loader on the page — including the ones
	// that succeeded — and loses whatever they had typed.
	interface Props {
		message: string;
		/** What was being read, in the user's words: "the versions of deploy". */
		subject?: string;
		testid: string;
		retryTestid?: string;
		retryLabel?: string;
		busy?: boolean;
		onretry?: () => void;
	}

	let {
		message,
		subject = '',
		testid,
		retryTestid = '',
		retryLabel = 'Retry',
		busy = false,
		onretry
	}: Props = $props();
</script>

<div
	data-testid={testid}
	role="alert"
	class="flex flex-wrap items-start gap-3 rounded-md border border-danger-200 bg-danger-50 px-4 py-3 text-sm"
>
	<div class="min-w-0 flex-1">
		{#if subject}
			<p class="font-medium text-danger-800">Could not load {subject}.</p>
		{/if}
		<p class="break-words whitespace-pre-wrap text-danger-700">{message}</p>
	</div>
	{#if onretry}
		<button
			type="button"
			data-testid={retryTestid || `${testid}-retry`}
			disabled={busy}
			onclick={() => onretry?.()}
			class="inline-flex h-8 shrink-0 items-center gap-1.5 rounded-md border border-danger-200 bg-surface px-3 text-xs font-medium text-danger-700 shadow-xs transition-colors duration-150 hover:border-danger-300 hover:bg-danger-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-danger-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas disabled:pointer-events-none disabled:opacity-50"
		>
			{busy ? 'Retrying…' : retryLabel}
		</button>
	{/if}
</div>
