// The command palette's shortcut: ⌘K on Apple platforms, Ctrl+K everywhere
// else — one shortcut per platform, never both.
//
// On macOS Ctrl+K is a text-editing key (kill to the end of the line) in every
// text field, so taking it there would break typing; elsewhere Meta+K is the
// operating system's or nothing. The hint the top bar shows is decided by the
// same predicate, so the key a person reads is the key that works.

/** True for a platform string of macOS, iOS or iPadOS. */
export function isApplePlatform(platform: string): boolean {
	// navigator.platform says "MacIntel"; navigator.userAgentData.platform says "macOS".
	return /mac|iphone|ipad|ipod/i.test(platform);
}

/** The platform string the browser reports, or '' where there is none (SSR). */
export function currentPlatform(): string {
	const nav = globalThis.navigator as
		| (Navigator & { userAgentData?: { platform?: string } })
		| undefined;
	return nav?.userAgentData?.platform || nav?.platform || '';
}

/** The shortcut as the hint prints it. */
export function shortcutHint(apple: boolean): string {
	return apple ? '⌘K' : 'Ctrl K';
}

/** The shortcut as aria-keyshortcuts names it. */
export function shortcutAria(apple: boolean): string {
	return apple ? 'Meta+K' : 'Control+K';
}

export interface KeyLike {
	key: string;
	metaKey: boolean;
	ctrlKey: boolean;
	altKey: boolean;
	shiftKey: boolean;
}

/**
 * What a keydown means for the palette:
 *   'toggle'  — the platform's shortcut: open the palette, or close it;
 *   'consume' — the shortcut while another overlay is open: swallow it
 *               (so the browser's own Ctrl+K does not fire) and open nothing,
 *               because a press on a palette option would dismiss that
 *               overlay — and a dialog may hold a half-filled form;
 *   'ignore'  — anything else.
 * `overlayOpen` is read from the DOM by the caller; the palette itself is
 * never counted as one, so the shortcut still closes it.
 */
export function paletteKey(event: KeyLike, apple: boolean, overlayOpen: boolean): 'toggle' | 'consume' | 'ignore' {
	if (event.key.toLowerCase() !== 'k' || event.altKey || event.shiftKey) return 'ignore';
	const ours = apple ? event.metaKey && !event.ctrlKey : event.ctrlKey && !event.metaKey;
	if (!ours) return 'ignore';
	return overlayOpen ? 'consume' : 'toggle';
}

/** The selector every floating surface carries only while it is open. */
export const OVERLAY_SELECTOR = '[data-overlay]';
