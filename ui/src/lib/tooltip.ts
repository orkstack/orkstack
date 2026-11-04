// The one tooltip. Any element with `data-tip` shows its text in a single
// shared element (Tooltip.svelte's #tt, role="tooltip"): after 300 ms of
// hover, or at once on keyboard focus. There is no native `title` anywhere.
//
// The rules, each a WCAG 1.4.13 or a review finding:
//   1. a bridge covers the gap, so the pointer can cross onto the tip (CSS);
//   2. every show or hide timer is cleared before it is set; hide waits 200 ms;
//   3. opening a tip first closes the previous one and releases its
//      aria-describedby;
//   4. a tip opened by hover closes on pointer-out; one opened by focus closes
//      on blur, never because the pointer moved;
//   5. the tip joins aria-describedby only when the control's name, text or
//      existing description does not already say the same words;
//   6. Escape is caught first and closes only the tip; the next Escape
//      reaches the popover or dialog beneath it;
//   7. any click closes the tip, so it never sits over what the click opened;
//   8. (TimeAgo.svelte) a relative time is focusable and carries its instant
//      both as the tip and as screen-reader text;
//   9. a tip dismissed by Escape or a click does not re-arm until the pointer
//      has left its control.
// It sits 8 px below its control, flips above when there is no room, and is
// clamped 12 px inside the viewport, so it never covers its control.

export const SHOW_DELAY_MS = 300;
export const HIDE_DELAY_MS = 200;
export const TIP_GAP = 8;
export const TIP_EDGE = 12;

export interface Box {
	left: number;
	top: number;
	width: number;
	height: number;
}

export interface Placement {
	left: number;
	top: number;
	/** True when the tip sits above its control. */
	up: boolean;
}

/** Where a tip of `tip`'s size goes for a control at `anchor`, in a viewport of `view`. */
export function placeTip(anchor: Box, tip: { width: number; height: number }, view: { width: number; height: number }): Placement {
	const below = anchor.top + anchor.height + TIP_GAP;
	const above = anchor.top - TIP_GAP - tip.height;
	const fitsBelow = below + tip.height <= view.height - TIP_EDGE;
	const up = !fitsBelow && above >= TIP_EDGE;
	const centred = anchor.left + anchor.width / 2 - tip.width / 2;
	const maxLeft = Math.max(TIP_EDGE, view.width - TIP_EDGE - tip.width);
	return { left: Math.round(Math.min(Math.max(centred, TIP_EDGE), maxLeft)), top: Math.round(up ? above : below), up };
}

const words = (s: string | null | undefined) => (s ?? '').replace(/\s+/g, ' ').trim().toLowerCase();

/**
 * Whether a tip should join its control's description (rule 5): not when the
 * control's accessible name, its text or its existing description already
 * says the tip's words.
 */
export function shouldDescribe(tip: string, control: { name?: string | null; text?: string | null; description?: string | null }): boolean {
	const t = words(tip);
	if (!t) return false;
	return ![control.name, control.text, control.description].some((s) => {
		const w = words(s);
		return w !== '' && (w === t || w.includes(t));
	});
}

const TIP_ID = 'tt';

/** The control's accessible name, text and existing description, as plain words. */
function said(el: Element): { name: string; text: string; description: string } {
	const byIds = (attr: string) =>
		(el.getAttribute(attr) ?? '')
			.split(/\s+/)
			.filter((id) => id && id !== TIP_ID)
			.map((id) => el.ownerDocument.getElementById(id)?.textContent ?? '')
			.join(' ');
	return {
		name: el.getAttribute('aria-label') ?? byIds('aria-labelledby'),
		text: el.textContent ?? '',
		description: byIds('aria-describedby')
	};
}

/**
 * Installs the tooltip behaviour on a document for one shared tip element.
 * Returns the function that removes it.
 */
export function installTooltips(doc: Document, tip: HTMLElement): () => void {
	const win = doc.defaultView;
	if (!win) return () => {};
	let current: HTMLElement | null = null;
	let open = false;
	let openedBy: 'hover' | 'focus' | null = null;
	let described = false;
	let showTimer: ReturnType<typeof setTimeout> | undefined;
	let hideTimer: ReturnType<typeof setTimeout> | undefined;
	// Rule 9: the control a dismissal disarmed, until the pointer leaves it.
	let disarmed: Element | null = null;

	const clearTimers = () => {
		clearTimeout(showTimer);
		clearTimeout(hideTimer);
		showTimer = hideTimer = undefined;
	};

	function release(): void {
		if (current && described) {
			const ids = (current.getAttribute('aria-describedby') ?? '').split(/\s+/).filter((id) => id && id !== TIP_ID);
			if (ids.length) current.setAttribute('aria-describedby', ids.join(' '));
			else current.removeAttribute('aria-describedby');
		}
		described = false;
	}

	/** keepShow: a pending hide of the old tip never cancels the next tip's pending show. */
	function hide(keepShow = false): void {
		if (keepShow) clearTimeout(hideTimer);
		else clearTimers();
		release();
		current = null;
		openedBy = null;
		open = false;
		if (tip.matches(':popover-open')) tip.hidePopover();
		tip.textContent = '';
	}

	function show(el: HTMLElement, by: 'hover' | 'focus'): void {
		const text = el.dataset.tip ?? '';
		if (!text) return;
		clearTimers();
		if (current && current !== el) hide();
		current = el;
		openedBy = by;
		tip.textContent = text;
		// The tip is a manual popover, in the top layer: above a modal dialog
		// too. Shown again on each open, so it is above whatever opened since.
		if (tip.matches(':popover-open')) tip.hidePopover();
		tip.showPopover();
		open = true;
		const r = el.getBoundingClientRect();
		const t = tip.getBoundingClientRect();
		const p = placeTip(
			{ left: r.left, top: r.top, width: r.width, height: r.height },
			{ width: t.width, height: t.height },
			{ width: win!.innerWidth, height: win!.innerHeight }
		);
		tip.style.left = `${p.left}px`;
		tip.style.top = `${p.top}px`;
		tip.classList.toggle('up', p.up);
		if (!described && shouldDescribe(text, said(el))) {
			const ids = (el.getAttribute('aria-describedby') ?? '').split(/\s+/).filter(Boolean);
			el.setAttribute('aria-describedby', [...ids, TIP_ID].join(' '));
			described = true;
		}
	}

	const tipOf = (target: EventTarget | null): HTMLElement | null =>
		target instanceof Element ? (target.closest('[data-tip]') as HTMLElement | null) : null;

	function onPointerOver(e: PointerEvent): void {
		if (e.pointerType === 'touch') return;
		if (tip.contains(e.target as Node)) {
			clearTimeout(hideTimer);
			return;
		}
		const el = tipOf(e.target);
		if (!el || el === disarmed) return;
		if (el === current) {
			clearTimeout(hideTimer);
			return;
		}
		if (openedBy === 'focus') return;
		// Only a pending show is replaced; a shown tip's pending hide still runs.
		clearTimeout(showTimer);
		showTimer = setTimeout(() => show(el, 'hover'), SHOW_DELAY_MS);
	}

	function onPointerOut(e: PointerEvent): void {
		const from = tip.contains(e.target as Node) ? current : tipOf(e.target);
		const to = e.relatedTarget as Node | null;
		if (disarmed && from === disarmed && !(to && disarmed.contains(to))) disarmed = null;
		if (!from) return;
		if (to && (from.contains(to) || tip.contains(to))) return;
		clearTimeout(showTimer);
		if (from === current && openedBy === 'hover') {
			clearTimeout(hideTimer);
			hideTimer = setTimeout(() => hide(true), HIDE_DELAY_MS);
		}
	}

	function onFocusIn(e: FocusEvent): void {
		const el = tipOf(e.target);
		if (!el || !(e.target instanceof Element) || e.target !== el) return;
		if (!el.matches(':focus-visible')) return;
		show(el, 'focus');
	}

	function onFocusOut(e: FocusEvent): void {
		if (current && e.target === current && openedBy === 'focus') hide();
	}

	function onKey(e: KeyboardEvent): void {
		if (e.key !== 'Escape' || !open) return;
		// Rule 6: this Escape is the tip's alone.
		e.stopPropagation();
		e.preventDefault();
		disarmed = current;
		hide();
	}

	function onClick(): void {
		if (!open && !showTimer) return;
		disarmed = current ?? disarmed;
		hide();
	}

	function onScroll(): void {
		if (open && openedBy === 'hover') hide();
	}

	doc.addEventListener('pointerover', onPointerOver);
	doc.addEventListener('pointerout', onPointerOut);
	doc.addEventListener('focusin', onFocusIn);
	doc.addEventListener('focusout', onFocusOut);
	win.addEventListener('keydown', onKey, { capture: true });
	doc.addEventListener('click', onClick, { capture: true });
	win.addEventListener('scroll', onScroll, { capture: true, passive: true });
	return () => {
		hide();
		doc.removeEventListener('pointerover', onPointerOver);
		doc.removeEventListener('pointerout', onPointerOut);
		doc.removeEventListener('focusin', onFocusIn);
		doc.removeEventListener('focusout', onFocusOut);
		win.removeEventListener('keydown', onKey, { capture: true });
		doc.removeEventListener('click', onClick, { capture: true });
		win.removeEventListener('scroll', onScroll, { capture: true });
	};
}
