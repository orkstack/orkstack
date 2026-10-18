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
	import { focusTrap } from '$lib/focusTrap';
	import Icon from './Icon.svelte';

	interface Props {
		title: string;
		testid: string;
		onclose: () => void;
		body: Snippet;
		/** Wider than ConfirmDialog's question when the body is a form. */
		widthClass?: string;
	}

	let { title, testid, onclose, body, widthClass = 'max-w-lg' }: Props = $props();

	// The panel takes the focus when it opens (focusTrap). Two reasons, both
	// about the keyboard: Tab starts inside the dialog rather than continuing
	// down the page behind it, and Escape is answered by THIS dialog rather
	// than by whatever the reader's focus was left on when it opened. The trap
	// then keeps Tab inside and hands the focus back to the opener on close.
</script>

<div class="fixed inset-0 z-50 flex items-center justify-center bg-scrim p-4">
	<div
		data-testid={testid}
		data-overlay
		role="dialog"
		aria-modal="true"
		aria-label={title}
		tabindex="-1"
		use:dismissOnOutsideOrEscape={onclose}
		use:focusTrap={{ focusContainer: true }}
		class="flex max-h-[85vh] w-full {widthClass} flex-col rounded-md border-(length:--rule-region) border-ink bg-sheet p-6 text-ink shadow-overlay animate-overlay-in focus:outline-none motion-reduce:animate-none"
	>
		<div class="flex shrink-0 items-start justify-between gap-4 border-b border-rule pb-3">
			<h2 class="text-lg font-semibold text-ink">{title}</h2>
			<button
				type="button"
				data-testid="{testid}-close"
				aria-label="Close"
				onclick={() => onclose()}
				class="btn btn-icon -mt-1 -mr-2"
			>
				<Icon name="close" />
			</button>
		</div>
		<!--
			The panel's own scroll, not the page's: a fixed, centered overlay has
			no other way to reach content taller than the viewport (a short
			window, or a form whose advanced options and CLI popover are both
			open). shrink-0 above keeps the header pinned while this scrolls.
		-->
		<div class="mt-3 overflow-y-auto text-sm text-ink-secondary">{@render body()}</div>
	</div>
</div>
