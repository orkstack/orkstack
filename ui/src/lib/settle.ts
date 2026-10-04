// A re-read the page starts on its own — a live event, a timer — must never
// cost the reader a click.
//
// SvelteKit hands the router to whichever began last: an invalidation started
// while a navigation is still loading takes the navigation's token, and the
// navigation's result is then discarded when it arrives (client.js
// `_invalidate`: "the invalidation should take over and win"). So a run page
// that re-read itself the moment its run finished swallowed a link clicked in
// that same instant: the destination's data was fetched and thrown away, and
// the reader was left on the run page with nothing to say why. Observed in
// e2e/run-origin.spec.ts — the trigger link, clicked as RUN_SUCCEEDED
// arrived, never left /runs/<id>.
//
// afterNavigation waits out every navigation in flight first, then re-reads
// only if the page that asked is still mounted: a navigation that left it has
// already loaded everything the reader now sees, and re-reading the new page
// on the old one's behalf would only repeat that work. A navigation that
// stayed on the page (a query-parameter link, a shallow history entry) gets
// the re-read it would otherwise have lost.
//
// The router is injected rather than imported so this runs under `bun test`:
// the call sites pass `() => navigating.complete` from $app/state.

/** The promise of the navigation in flight, or null when there is none. */
export type PendingNavigation = () => Promise<void> | null;

/**
 * Runs `reread` once no navigation is in flight, if `mounted()` still holds
 * then. Resolves true when it re-read, false when the page had gone.
 */
export async function afterNavigation(
	pending: PendingNavigation,
	mounted: () => boolean,
	reread: () => Promise<void>
): Promise<boolean> {
	for (let navigation = pending(); navigation; navigation = pending()) {
		try {
			await navigation;
		} catch {
			// An aborted or failed navigation is over all the same; whether the
			// page survived it is mounted()'s question, below.
		}
	}
	if (!mounted()) return false;
	await reread();
	return true;
}
