<script lang="ts">
	// The confirmation step in front of an action that changes what runs.
	//
	// It names both ends of the change — from this version, to that one —
	// because "are you sure?" over an unnamed subject is a click-through, and
	// the mistake this guards against is promoting the row above the one you
	// meant.
	//
	// It takes the focus when it opens and keeps Tab inside (focusTrap): a
	// destructive one starts on Cancel, so the Enter that opened it cannot
	// also confirm it; any other starts on its first control.
	import type { Snippet } from 'svelte';
	import { focusTrap } from '$lib/focusTrap';

	interface Props {
		title: string;
		testid: string;
		acceptLabel?: string;
		acceptTestid: string;
		cancelTestid: string;
		busy?: boolean;
		error?: string;
		/** Swaps the accept button to the destructive recipe, for delete/cancel actions. */
		destructive?: boolean;
		/** Holds the accept button disabled, for a dialog whose body collects a required value. */
		acceptDisabled?: boolean;
		body: Snippet;
		onaccept: () => void;
		oncancel: () => void;
	}

	let {
		title,
		testid,
		acceptLabel = 'Confirm',
		acceptTestid,
		cancelTestid,
		busy = false,
		error = '',
		destructive = false,
		acceptDisabled = false,
		body,
		onaccept,
		oncancel
	}: Props = $props();
</script>

<div class="fixed inset-0 z-50 flex items-center justify-center bg-scrim p-4">
	<div
		data-testid={testid}
		data-overlay
		role="dialog"
		aria-modal="true"
		aria-label={title}
		tabindex="-1"
		use:focusTrap={{ initial: destructive ? '[data-confirm-cancel]' : undefined }}
		class="w-full max-w-lg rounded-md border-(length:--rule-region) border-ink bg-sheet p-6 text-ink shadow-overlay animate-overlay-in focus:outline-none motion-reduce:animate-none"
	>
		<h2 class="border-b border-rule pb-3 text-lg font-semibold text-ink">{title}</h2>
		<div class="mt-3 text-sm text-ink-secondary">{@render body()}</div>
		{#if error}
			<p
				data-testid="{testid}-error"
				class="severity-error mt-4 border border-red bg-red-tint px-3 py-2 text-sm"
			>
				{error}
			</p>
		{/if}
		<div class="mt-6 flex justify-end gap-3">
			<button
				type="button"
				data-testid={cancelTestid}
				data-confirm-cancel
				disabled={busy}
				onclick={() => oncancel()}
				class="btn"
			>
				Cancel
			</button>
			<button
				type="button"
				data-testid={acceptTestid}
				disabled={busy || acceptDisabled}
				onclick={() => onaccept()}
				class="btn {destructive ? 'btn-danger' : 'btn-primary'}"
			>
				{busy ? 'Working…' : acceptLabel}
			</button>
		</div>
	</div>
</div>
