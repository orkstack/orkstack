// What a task's row says about where it ran, when a pin asks for that or the
// reader asked for Orkers on every row. A plain task has one Orker; a fan-out's
// items can land on several, so its row counts them and names them in a tip.
// Pure: nothing here is read unless the row is going to say it.

export interface RowOrkers {
	word: string;
	/** Every Orker's name, when the word only counts them. */
	tip?: string;
}

/** The ids may be empty, for what has reached no Orker; none at all is no words. */
export function rowOrkers(ids: readonly string[]): RowOrkers | null {
	const distinct = [...new Set(ids.filter(Boolean))].sort();
	if (distinct.length === 0) return null;
	if (distinct.length === 1) return { word: distinct[0] };
	return { word: `${distinct.length} Orkers`, tip: distinct.join(', ') };
}
