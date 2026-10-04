import { namespaceFrom } from '$lib/namespace';
import type { PageLoad } from './$types';

// Automation RESPECTS the namespace switcher, as Pools does: a document
// describes exactly one namespace, and the page plans, exports and applies
// in the switcher's. Nothing is read until the operator asks — an export and
// a plan are each one button — because a plan needs a document first.
export const load: PageLoad = async ({ url }) => {
	return { namespace: namespaceFrom(url) };
};
