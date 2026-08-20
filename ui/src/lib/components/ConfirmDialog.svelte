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
		body,
		onaccept,
		oncancel
	}: Props = $props();
</script>

<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
	<div
		data-testid={testid}
		role="dialog"
		aria-modal="true"
		aria-label={title}
		class="w-full max-w-lg rounded-lg border border-zinc-700 bg-zinc-900 p-5 shadow-xl"
	>
		<h2 class="text-lg font-semibold text-zinc-100">{title}</h2>
		<div class="mt-2 text-sm text-zinc-300">{@render body()}</div>
		{#if error}
			<p
				data-testid="{testid}-error"
				class="mt-3 rounded-md border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-sm text-rose-300"
			>
				{error}
			</p>
		{/if}
		<div class="mt-5 flex justify-end gap-3">
			<button
				type="button"
				data-testid={cancelTestid}
				disabled={busy}
				onclick={() => oncancel()}
				class="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
			>
				Cancel
			</button>
			<button
				type="button"
				data-testid={acceptTestid}
				disabled={busy}
				onclick={() => onaccept()}
				class="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
			>
				{busy ? 'Working…' : acceptLabel}
			</button>
		</div>
	</div>
</div>
