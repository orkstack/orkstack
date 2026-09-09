<script lang="ts">
	// A panel that hangs off whatever opened it, and nothing more.
	//
	// It owns position and dismissal; it does not own `open`. The caller holds
	// that state and is told when the popover wants it changed, which is what
	// lets one caller open a popover from a button, another from a menu row,
	// and a third close it as a side effect of the action it was showing.
	import type { Snippet } from 'svelte';
	import { dismissOnOutsideOrEscape } from '$lib/dismiss';

	interface Props {
		open: boolean;
		onOpenChange: (open: boolean) => void;
		trigger: Snippet;
		content: Snippet;
		testid?: string;
		/** Which edge the panel shares with its trigger. 'end' keeps a wide panel on screen near the right margin. */
		align?: 'start' | 'end';
	}

	let {
		open,
		onOpenChange,
		trigger,
		content,
		testid = undefined,
		align = 'start'
	}: Props = $props();

	// The trigger lives INSIDE the element the dismissal watches, so a press on
	// it is never "outside": the trigger's own click is left to toggle, instead
	// of being closed here and reopened a moment later by the click that
	// followed the same press.
	function dismiss(): void {
		if (open) onOpenChange(false);
	}

	// Escape is also handled locally, on the bubble path, so it stops here
	// instead of reaching a Dialog (or another Popover) further up the tree —
	// the same reason Select answers Escape itself rather than leaving it to
	// dismissOnOutsideOrEscape's own document-level listener. Without this, a
	// CliEquivalent popover opened inside a Dialog closed BOTH on one Escape.
	//
	// An action, not an `onkeydown` template attribute: the wrapping element
	// is a plain positioning `<div>` with no interactive role of its own (the
	// real interactive elements are inside `trigger`/`content`), and Svelte's
	// a11y check flags a static element with a declarative key handler. An
	// imperative listener isn't statically checkable that way, and behaves
	// identically — it still runs before the bubble reaches `document`.
	function stopEscapeHere(node: HTMLElement): { destroy(): void } {
		function onKeyDown(event: KeyboardEvent): void {
			if (event.key !== 'Escape' || !open) return;
			event.stopPropagation();
			onOpenChange(false);
		}
		node.addEventListener('keydown', onKeyDown);
		return { destroy: () => node.removeEventListener('keydown', onKeyDown) };
	}
</script>

<div class="relative inline-block" use:dismissOnOutsideOrEscape={dismiss} use:stopEscapeHere>
	{@render trigger()}
	{#if open}
		<div
			data-testid={testid}
			class="absolute z-10 mt-1 rounded-md border border-border bg-surface p-3 shadow-md {align ===
			'end'
				? 'right-0'
				: 'left-0'}"
		>
			{@render content()}
		</div>
	{/if}
</div>
