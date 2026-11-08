<script lang="ts">
	// A panel that hangs off whatever opened it, and nothing more.
	//
	// It owns position and dismissal; it does not own `open`. The caller holds
	// that state and is told when the popover wants it changed, which is what
	// lets one caller open a popover from a button, another from a menu row,
	// and a third close it as a side effect of the action it was showing.
	import { untrack, type Snippet } from 'svelte';
	import { dismissOnOutsideOrEscape } from '#lib/dismiss.ts';
	import { placePanel, type PanelPlacement } from '#lib/placement.ts';

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
		// Both: stopPropagation keeps it from the document's listeners, and
		// preventDefault keeps it from a native <dialog> around this panel, which
		// answers Escape by closing unless the keydown was cancelled.
		event.stopPropagation();
		event.preventDefault();
		onOpenChange(false);
	}

	// Every listener is installed only while the panel is open. A closed
	// popover listens to nothing: a run page holds one per task card, and at
	// 4,000 cards two document listeners each were the largest single cost of
	// drawing the page.
	let root = $state<HTMLDivElement | undefined>(undefined);
	// The panel is a manual popover, in the top layer, as the tooltip is: inside
	// a table's scrolling frame or a modal's body it would otherwise be clipped
	// to whatever that frame shows. It stays a descendant of `root`, so the
	// dismissal and Escape handling below see it as inside.
	//
	// offsetWidth, not the bounding box: the opening animation scales the panel,
	// and a placement measured mid-animation would stick.
	let panel = $state<HTMLDivElement | undefined>(undefined);
	let placed = $state<PanelPlacement | undefined>(undefined);
	function place(): void {
		if (!root || !panel) return;
		// The full height the content asks for, not what the last placement allowed.
		const height = panel.scrollHeight + panel.offsetHeight - panel.clientHeight;
		placed = placePanel(
			root.getBoundingClientRect(),
			{ width: panel.offsetWidth, height },
			{ width: document.documentElement.clientWidth, height: document.documentElement.clientHeight },
			align
		);
	}
	$effect(() => {
		if (!open || !panel) {
			placed = undefined;
			return;
		}
		panel.showPopover();
		untrack(place);
		// Fixed in the viewport, so it follows its trigger through any scroll,
		// and its own content changing size can move it across an edge.
		const sized = new ResizeObserver(() => place());
		sized.observe(panel);
		const options = { capture: true, passive: true };
		window.addEventListener('scroll', place, options);
		window.addEventListener('resize', place);
		return () => {
			sized.disconnect();
			window.removeEventListener('scroll', place, options);
			window.removeEventListener('resize', place);
		};
	});
	$effect(() => {
		if (!open || !root) return;
		const node = root;
		const outside = dismissOnOutsideOrEscape(node, dismiss);
		node.addEventListener('keydown', stopEscapeHere);
		// Focus leaving the trigger and its panel closes it, as Escape does.
		// Checked a frame later: a copy that falls back to a hidden textarea
		// moves the focus away for a moment and back.
		const leave = () => {
			requestAnimationFrame(() => {
				const now = document.activeElement;
				if (now && now !== document.body && !node.contains(now)) dismiss();
			});
		};
		node.addEventListener('focusout', leave);
		return () => {
			outside.destroy();
			node.removeEventListener('keydown', stopEscapeHere);
			node.removeEventListener('focusout', leave);
		};
	});
</script>

<div class="relative inline-block" bind:this={root}>
	{@render trigger()}
	{#if open}
		<div
			bind:this={panel}
			popover="manual"
			data-testid={testid}
			data-overlay
			style:left={placed ? `${placed.left}px` : '0'}
			style:top={placed ? `${placed.top}px` : '0'}
			style:max-height={placed ? `${placed.maxHeight}px` : undefined}
			class="fixed right-auto bottom-auto m-0 overflow-y-auto rounded-md border border-line-2 bg-surface p-3.5 text-ink shadow-overlay animate-overlay-in motion-reduce:animate-none"
		>
			{@render content()}
		</div>
	{/if}
</div>
