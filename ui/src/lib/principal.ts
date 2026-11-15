// How a principal is marked (AGENTS.md §7, "Principal"): every rendering says
// whether OrkStack verified it or a gateway only asserted it, and an asserted
// principal is never shown as a verified one, nor a verified one as asserted.

export interface PrincipalMark {
	/** The tag's word beside the name. */
	word: 'verified' | 'asserted';
	/** The tag's explanation, shown as its tip. */
	tip: string;
	/** The explanation a screen reader hears after the word. */
	sr: string;
}

/**
 * The mark for a principal. Only `verified === true` reads as verified: a
 * record that does not say, or says false, is asserted. `method` is how it
 * was verified (Run origin's auth_method), named in the tip when given.
 */
export function principalMark(verified: boolean, method = ''): PrincipalMark {
	if (verified === true) {
		const how = method.trim();
		const by = how ? `by OrkStack (${how})` : 'by OrkStack';
		return { word: 'verified', tip: `Verified ${by}`, sr: `verified ${by}` };
	}
	return {
		word: 'asserted',
		tip: 'Asserted by your gateway, not verified by OrkStack',
		sr: 'asserted by your gateway, not verified by OrkStack'
	};
}
