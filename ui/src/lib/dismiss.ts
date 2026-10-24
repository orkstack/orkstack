// The one dismissal rule every floating surface in this UI obeys: a pointer
// press anywhere outside it, or Escape, puts it away.
//
// It lives here rather than in each of Select/Popover/Dialog because three
// copies of "did that click land inside me" is three chances to answer it
// differently — and a dropdown that stays open under a dialog, or a dialog
// that closes when a dropdown inside it is dismissed, is exactly the kind of
// thing three copies produce.
//
// Two deliberate choices in the listeners below:
//
//   * pointerdown, in the CAPTURE phase, not click. A click fires after the
//     press, on the element the press ended over, and a handler further down
//     the tree can stop it before it ever reaches the document. Capturing the
//     press means the panel is closed by the same gesture that started the
//     next interaction, in the order a reader expects.
//
//   * keydown in the BUBBLE phase, so an element that handles Escape itself
//     can stop the event and keep this from acting a second time. Select does
//     exactly that: it closes without committing, and a dialog around it stays
//     open, because the press never reaches the document.
export function dismissOnOutsideOrEscape(
	node: HTMLElement,
	onDismiss: () => void
): { destroy(): void; update(onDismiss: () => void): void } {
	// Held in a variable rather than closed over, so `update` can swap the
	// callback when Svelte re-evaluates the action's argument.
	let dismiss = onDismiss;

	function onPointerDown(event: PointerEvent): void {
		const target = event.target;
		if (target instanceof Node && node.contains(target)) return;
		dismiss();
	}

	function onKeyDown(event: KeyboardEvent): void {
		if (event.key !== 'Escape') return;
		const focused = document.activeElement;
		// Escape acts on this node when the focus is inside it, or when
		// nothing in particular holds the focus — the ordinary state of a page
		// behind a dialog that was opened by a click. A press aimed at some
		// other focused control (a text field being edited elsewhere) is not
		// this node's to answer.
		const unfocused =
			focused === null || focused === document.body || focused === document.documentElement;
		if (!unfocused && !node.contains(focused)) return;
		dismiss();
	}

	document.addEventListener('pointerdown', onPointerDown, true);
	document.addEventListener('keydown', onKeyDown);

	return {
		update(next: () => void): void {
			dismiss = next;
		},
		destroy(): void {
			document.removeEventListener('pointerdown', onPointerDown, true);
			document.removeEventListener('keydown', onKeyDown);
		}
	};
}
