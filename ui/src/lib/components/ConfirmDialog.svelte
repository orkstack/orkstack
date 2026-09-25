<script lang="ts">
	// The confirmation step in front of an action that changes what runs.
	//
	// It names both ends of the change — from this version, to that one —
	// because "are you sure?" over an unnamed subject is a click-through, and
	// the mistake this guards against is promoting the row above the one you
	// meant.
	import type { Snippet } from 'svelte';

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

<div
	class="fixed inset-0 z-50 flex items-center justify-center bg-gray-950/40 p-4 animate-fade-in motion-reduce:animate-none"
>
	<div
		data-testid={testid}
		role="dialog"
		aria-modal="true"
		aria-label={title}
		class="w-full max-w-lg rounded-xl border border-border bg-surface-raised p-6 shadow-lg animate-dialog-in motion-reduce:animate-none"
	>
		<h2 class="text-base font-semibold text-fg">{title}</h2>
		<div class="mt-2 text-sm text-fg-secondary">{@render body()}</div>
		{#if error}
			<p
				data-testid="{testid}-error"
				class="mt-4 rounded-md border border-danger-200 bg-danger-50 px-3 py-2 text-sm text-danger-700"
			>
				{error}
			</p>
		{/if}
		<div class="mt-6 flex justify-end gap-3">
			<button
				type="button"
				data-testid={cancelTestid}
				disabled={busy}
				onclick={() => oncancel()}
				class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md border border-border bg-surface px-3.5 text-sm font-medium text-fg shadow-xs transition-colors duration-150 hover:border-border-strong hover:bg-surface-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas disabled:pointer-events-none disabled:opacity-50"
			>
				Cancel
			</button>
			<button
				type="button"
				data-testid={acceptTestid}
				disabled={busy || acceptDisabled}
				onclick={() => onaccept()}
				class="inline-flex h-9 items-center justify-center gap-1.5 rounded-md px-3.5 text-sm font-medium text-white shadow-xs transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas disabled:pointer-events-none disabled:opacity-50 {destructive
					? 'bg-danger-600 hover:bg-danger-700 focus-visible:ring-danger-500/40 active:bg-danger-800'
					: 'bg-brand-600 hover:bg-brand-700 focus-visible:ring-brand-500/40 active:bg-brand-800'}"
			>
				{busy ? 'Working…' : acceptLabel}
			</button>
		</div>
	</div>
</div>
