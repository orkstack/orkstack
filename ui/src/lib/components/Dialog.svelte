<script lang="ts">
	// The modal shell: a scrim, a panel, a title, a way out.
	//
	// It is the same scrim, radius, shadow and motion ConfirmDialog already
	// uses, because two modals that arrive differently read as two different
	// applications. What it is NOT is a confirmation: ConfirmDialog exists for
	// the question with two answers and keeps its own accept/cancel footer,
	// error line and busy state. This one holds whatever the caller renders.
	//
	// Like ConfirmDialog, it does not toggle itself — the caller renders it
	// inside `{#if}` and owns the state that decides. A component that hid
	// itself would need the caller to keep a second copy of the same fact.
	import type { Snippet } from 'svelte';
	import { dismissOnOutsideOrEscape } from '$lib/dismiss';

	interface Props {
		title: string;
		testid: string;
		onclose: () => void;
		body: Snippet;
		/** Wider than ConfirmDialog's question when the body is a form. */
		widthClass?: string;
	}

	let { title, testid, onclose, body, widthClass = 'max-w-lg' }: Props = $props();

	let panel = $state<HTMLDivElement | undefined>(undefined);

	// The panel takes the focus when it opens. Two reasons, both about the
	// keyboard: Tab starts inside the dialog rather than continuing down the
	// page behind it, and Escape is answered by THIS dialog rather than by
	// whatever the reader's focus was left on when it opened.
	$effect(() => {
		panel?.focus();
	});
</script>

<div
	class="fixed inset-0 z-50 flex items-center justify-center bg-gray-950/40 p-4 animate-fade-in motion-reduce:animate-none"
>
	<div
		bind:this={panel}
		data-testid={testid}
		role="dialog"
		aria-modal="true"
		aria-label={title}
		tabindex="-1"
		use:dismissOnOutsideOrEscape={onclose}
		class="flex max-h-[85vh] w-full {widthClass} flex-col rounded-xl border border-border bg-surface-raised p-6 shadow-lg animate-dialog-in focus:outline-none motion-reduce:animate-none"
	>
		<div class="flex shrink-0 items-start justify-between gap-4">
			<h2 class="text-base font-semibold text-fg">{title}</h2>
			<button
				type="button"
				data-testid="{testid}-close"
				aria-label="Close"
				onclick={() => onclose()}
				class="-mt-1 -mr-1 inline-flex h-8 w-8 items-center justify-center rounded-md text-lg leading-none text-fg-muted transition-colors duration-150 hover:bg-surface-hover hover:text-fg focus-visible:ring-2 focus-visible:ring-brand-500/40 focus-visible:outline-none"
			>
				×
			</button>
		</div>
		<!--
			The panel's own scroll, not the page's: a fixed, centered overlay has
			no other way to reach content taller than the viewport (a short
			window, or a form whose advanced options and CLI popover are both
			open). shrink-0 above keeps the header pinned while this scrolls.
		-->
		<div class="mt-2 overflow-y-auto text-sm text-fg-secondary">{@render body()}</div>
	</div>
</div>
