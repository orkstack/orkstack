// The keyboard contract of a modal: aria-modal="true" PROMISES that nothing
// behind the dialog is reachable, and this keeps the promise.
//
//   * on open, the focus moves into the dialog — onto `initial` when the
//     dialog names one (a destructive confirmation names Cancel, so a stray
//     Enter never deletes), else the first focusable control, else the panel;
//   * Tab and Shift+Tab cycle among the dialog's controls and never leave it;
//   * on close, the focus goes back to whatever held it before the dialog
//     opened (the button that opened it), if that element is still on the page.
//
// One action shared by Dialog and ConfirmDialog, for dismiss.ts's reason: two
// copies of "where does Tab go" would answer it differently.

export const FOCUSABLE =
	'a[href], button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"]), [contenteditable="true"]';

export interface FocusTrapOptions {
	/** A selector inside the dialog naming what takes the focus on open. */
	initial?: string;
	/**
	 * Focus the dialog panel itself on open rather than its first control
	 * (the panel must carry tabindex="-1"). Tab still starts inside it.
	 */
	focusContainer?: boolean;
}

/** The dialog's controls a Tab can land on, in document order, visible only. */
export function focusables(node: HTMLElement): HTMLElement[] {
	return Array.from(node.querySelectorAll<HTMLElement>(FOCUSABLE)).filter(
		(element) => element.getClientRects().length > 0
	);
}

/**
 * nextTabStop answers where a Tab (or Shift+Tab) goes from `active`, or null
 * when the browser's own move stays inside the dialog and needs no help.
 */
export function nextTabStop<T>(stops: T[], active: unknown, backwards: boolean): T | null {
	if (stops.length === 0) return null;
	const first = stops[0];
	const last = stops[stops.length - 1];
	const at = stops.indexOf(active as T);
	if (at < 0) return backwards ? last : first;
	if (backwards && at === 0) return last;
	if (!backwards && at === stops.length - 1) return first;
	return null;
}

export function focusTrap(
	node: HTMLElement,
	options: FocusTrapOptions = {}
): { destroy(): void; update(next: FocusTrapOptions): void } {
	const previous = node.ownerDocument.activeElement as HTMLElement | null;

	function onKeyDown(event: KeyboardEvent): void {
		// An inner trap (a confirmation opened over a dialog) answered first.
		if (event.key !== 'Tab' || event.defaultPrevented) return;
		const stops = focusables(node);
		if (stops.length === 0) {
			event.preventDefault();
			node.focus();
			return;
		}
		const next = nextTabStop(stops, node.ownerDocument.activeElement, event.shiftKey);
		if (next) {
			event.preventDefault();
			next.focus();
		}
	}

	// A disabled Cancel (a busy dialog) cannot take the focus: fall back.
	const initial = options.initial ? node.querySelector<HTMLElement>(options.initial) : null;
	if (initial && focusables(node).includes(initial)) {
		initial.focus();
	} else if (options.focusContainer) {
		node.focus();
	} else {
		(focusables(node)[0] ?? node).focus();
	}
	node.addEventListener('keydown', onKeyDown);

	return {
		update(next: FocusTrapOptions) {
			options = next;
		},
		destroy() {
			node.removeEventListener('keydown', onKeyDown);
			// Back to the opener — unless the dialog's own action removed it
			// (a deleted row's Delete button), where there is nothing to go
			// back to and the browser's default is the honest answer.
			if (previous?.isConnected && typeof previous.focus === 'function') {
				previous.focus();
			}
		}
	};
}
