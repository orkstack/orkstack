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
	class="flex flex-wrap items-start gap-3 rounded-md border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-300"
>
	<div class="min-w-0 flex-1">
		{#if subject}
			<p class="font-medium text-rose-200">Could not load {subject}.</p>
		{/if}
		<p class="break-words whitespace-pre-wrap">{message}</p>
	</div>
	{#if onretry}
		<button
			type="button"
			data-testid={retryTestid || `${testid}-retry`}
			disabled={busy}
			onclick={() => onretry?.()}
			class="shrink-0 rounded-md border border-rose-400/50 px-3 py-1.5 text-sm font-medium text-rose-100 hover:bg-rose-500/20 disabled:opacity-50"
		>
			{busy ? 'Retrying…' : retryLabel}
		</button>
	{/if}
</div>
