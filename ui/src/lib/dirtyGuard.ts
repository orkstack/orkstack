// "You have unsaved changes" — one rule for every form that can lose typing.
//
// guardUnsaved is called once while a component initialises. It answers every
// way of leaving the page through SvelteKit's beforeNavigate:
//
//   * an in-app navigation (a link, the namespace switcher, back/forward)
//     asks `message` with the browser's confirm(), and stays when refused;
//   * leaving the document (reload, closing the tab, typing a new address)
//     is a 'leave' navigation, where SvelteKit turns cancel() into the
//     browser's own beforeunload prompt — a page may not show a dialog of its
//     own there, so the browser's is the only one there can be.
//
// isDirty is read at the moment of leaving, never captured: a form that was
// saved (or reset) a second ago must leave without a question. It is handed
// the navigation, so a page whose form survives a navigation to its own route
// (a filter, a page of the list, the namespace switcher) can say so.
import { beforeNavigate } from '$app/navigation';
import type { BeforeNavigate } from '@sveltejs/kit';

export type UnsavedVerdict = 'proceed' | 'cancel';

/** The decision guardUnsaved makes, separated so it can be tested. */
export function unsavedVerdict(dirty: boolean, type: string, ask: () => boolean): UnsavedVerdict {
	if (!dirty) return 'proceed';
	if (type === 'leave') return 'cancel';
	return ask() ? 'proceed' : 'cancel';
}

export function guardUnsaved(
	isDirty: (navigation: BeforeNavigate) => boolean,
	message: string,
	ask: (message: string) => boolean = (text) => confirm(text)
): void {
	beforeNavigate((navigation) => {
		if (unsavedVerdict(isDirty(navigation), navigation.type, () => ask(message)) === 'cancel') navigation.cancel();
	});
}

/** True when a navigation takes the reader off the route they are on. */
export function leavesRoute(navigation: Pick<BeforeNavigate, 'from' | 'to'>): boolean {
	return !navigation.to || navigation.to.route.id !== navigation.from?.route.id;
}
