<script lang="ts">
	// A panel that hangs off whatever opened it, and nothing more.
	//
	// It owns position and dismissal; it does not own `open`. The caller holds
	// that state and is told when the popover wants it changed, which is what
	// lets one caller open a popover from a button, another from a menu row,
	// and a third close it as a side effect of the action it was showing.
	import { untrack, type Snippet } from 'svelte';
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
	// An imperative listener, not an `onkeydown` template attribute: the
	// wrapping element is a plain positioning `<div>` with no interactive role
	// of its own (the real interactive elements are inside `trigger`/
	// `content`), and Svelte's a11y check flags a static element with a
	// declarative key handler. It still runs before the bubble reaches
	// `document`.
	function stopEscapeHere(event: KeyboardEvent): void {
		if (event.key !== 'Escape' || !open) return;
		event.stopPropagation();
		onOpenChange(false);
	}

	// Every listener is installed only while the panel is open. A closed
	// popover listens to nothing: a run page holds one per task card, and at
	// 4,000 cards two document listeners each were the largest single cost of
	// drawing the page.
	let root = $state<HTMLDivElement | undefined>(undefined);
	// The panel hangs from its trigger's edge, but never past the viewport's:
	// opened from a trigger near the other edge (a header's actions wrapped to
	// the left on a phone), it is moved back inside, 8 px clear of the edge.
	let panel = $state<HTMLDivElement | undefined>(undefined);
	let shift = $state(0);
	$effect(() => {
		if (!open || !panel) {
			shift = 0;
			return;
		}
		const r = panel.getBoundingClientRect();
		const current = untrack(() => shift);
		const left = r.left - current;
		const right = r.right - current;
		const room = document.documentElement.clientWidth;
		shift = left < 8 ? 8 - left : right > room - 8 ? Math.max(room - 8 - right, 8 - left) : 0;
	});
	$effect(() => {
		if (!open || !root) return;
		const node = root;
		const outside = dismissOnOutsideOrEscape(node, dismiss);
		node.addEventListener('keydown', stopEscapeHere);
		return () => {
			outside.destroy();
			node.removeEventListener('keydown', stopEscapeHere);
		};
	});
</script>

<div class="relative inline-block" bind:this={root}>
	{@render trigger()}
	{#if open}
		<div
			bind:this={panel}
			data-testid={testid}
			data-overlay
			style:translate={shift ? `${shift}px 0` : undefined}
			class="absolute z-10 mt-1 rounded-md border border-ink bg-sheet p-3 text-ink shadow-overlay animate-overlay-in motion-reduce:animate-none {align ===
			'end'
				? 'right-0'
				: 'left-0'}"
		>
			{@render content()}
		</div>
	{/if}
</div>
