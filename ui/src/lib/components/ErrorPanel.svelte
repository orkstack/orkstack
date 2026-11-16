<script lang="ts">
	import { permissionRefusal } from '#lib/permissions.ts';
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

	// A refusal for a permission the caller lacks is not a failure: it says
	// what to ask for, calmly, and Retry still reads again once it is granted.
	const refusal = $derived(permissionRefusal(message));
	const where = $derived(refusal?.scope === 'deployment' ? 'this deployment' : refusal?.scope.replace(':', ' '));
</script>

{#if refusal}
	<div data-testid={testid} data-refusal="permission" role="status" class="flex flex-wrap items-start gap-3 border border-line bg-surface-2 px-4 py-3 text-sm">
		<div class="min-w-0 flex-1">
			<p class="font-semibold text-ink">{subject ? `You may not see ${subject}.` : 'You may not do this.'}</p>
			<p class="text-ink-2">You need <code>{refusal.permission}</code> on {where}. An administrator can bind a role that holds it to you.</p>
		</div>
		{#if onretry}
			<button type="button" data-testid={retryTestid || `${testid}-retry`} disabled={busy} onclick={() => onretry?.()} class="btn btn-sm shrink-0">
				{busy ? 'Retrying…' : retryLabel}
			</button>
		{/if}
	</div>
{:else}
	<div
		data-testid={testid}
		role="alert"
		class="flex flex-wrap items-start gap-3 border border-red bg-red-tint px-4 py-3 text-sm"
	>
		<div class="min-w-0 flex-1">
			{#if subject}
				<p class="severity-error font-semibold">Could not load {subject}.</p>
			{/if}
			<p class="break-words whitespace-pre-wrap text-red-strong">{message}</p>
		</div>
		{#if onretry}
			<button
				type="button"
				data-testid={retryTestid || `${testid}-retry`}
				disabled={busy}
				onclick={() => onretry?.()}
				class="btn btn-sm shrink-0"
			>
				{busy ? 'Retrying…' : retryLabel}
			</button>
		{/if}
	</div>
{/if}
