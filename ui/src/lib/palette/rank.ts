// Ranking the palette's commands against what was typed. Pure, so the order
// a person sees is a tested function of the query and nothing else.
//
// A match is graded, best first: the label IS the query; it starts with it;
// a word in it starts with it; it contains it; its letters appear in order
// (a shorter span wins). Case is ignored. Ties go to the group's order, then
// to how recently the command was used, then to the label.

export const MATCH_EXACT = 5;
export const MATCH_PREFIX = 4;
export const MATCH_WORD = 3;
export const MATCH_SUBSTRING = 2;
export const MATCH_SUBSEQUENCE = 1;

export interface MatchScore {
	grade: number;
	/** For a subsequence: the span it covered (smaller is better); 0 otherwise. */
	span: number;
}

export function match(label: string, query: string): MatchScore | null {
	const l = label.toLowerCase();
	const q = query.trim().toLowerCase();
	if (q === '') return { grade: MATCH_SUBSEQUENCE, span: 0 };
	if (l === q) return { grade: MATCH_EXACT, span: 0 };
	if (l.startsWith(q)) return { grade: MATCH_PREFIX, span: 0 };
	const at = l.indexOf(q);
	if (at > 0 && /[^a-z0-9]/.test(l[at - 1])) return { grade: MATCH_WORD, span: 0 };
	if (at >= 0) {
		// A later word start may still exist after a mid-word hit.
		for (let i = l.indexOf(q, at + 1); i > 0; i = l.indexOf(q, i + 1)) {
			if (/[^a-z0-9]/.test(l[i - 1])) return { grade: MATCH_WORD, span: 0 };
		}
		return { grade: MATCH_SUBSTRING, span: 0 };
	}
	// Ordered subsequence: the shortest window holding every letter in order.
	let best = Infinity;
	for (let start = l.indexOf(q[0]); start >= 0; start = l.indexOf(q[0], start + 1)) {
		let j = 1;
		let i = start + 1;
		for (; i < l.length && j < q.length; i++) if (l[i] === q[j]) j++;
		if (j === q.length) best = Math.min(best, i - start);
	}
	return best === Infinity ? null : { grade: MATCH_SUBSEQUENCE, span: best };
}

export interface Rankable {
	id: string;
	label: string;
	group: string;
	/** Extra words that also match (a namespace, a digest); never shown as the label. */
	keywords?: string;
}

export const PER_GROUP = 8;
export const TOTAL = 40;

/**
 * rank returns the commands that match, best first, at most PER_GROUP per
 * group and TOTAL in all. `groupOrder` lists the groups in display order;
 * `recency` maps a command id to when it was last used (larger is newer).
 */
export function rank<T extends Rankable>(
	commands: readonly T[],
	query: string,
	groupOrder: readonly string[],
	recency: ReadonlyMap<string, number> = new Map()
): T[] {
	const scored: { c: T; s: MatchScore; g: number; i: number }[] = [];
	for (const [i, c] of commands.entries()) {
		const own = match(c.label, query);
		const extra = c.keywords ? match(c.keywords, query) : null;
		const s = own && extra ? (better(own, extra) ? own : extra) : (own ?? extra);
		if (!s) continue;
		const g = groupOrder.indexOf(c.group);
		scored.push({ c, s, g: g < 0 ? groupOrder.length : g, i });
	}
	scored.sort(
		(a, b) =>
			b.s.grade - a.s.grade ||
			a.s.span - b.s.span ||
			a.g - b.g ||
			(recency.get(b.c.id) ?? 0) - (recency.get(a.c.id) ?? 0) ||
			a.i - b.i
	);
	const perGroup = new Map<string, number>();
	const out: T[] = [];
	for (const { c } of scored) {
		const n = perGroup.get(c.group) ?? 0;
		if (n >= PER_GROUP) continue;
		perGroup.set(c.group, n + 1);
		out.push(c);
		if (out.length >= TOTAL) break;
	}
	return out;
}

function better(a: MatchScore, b: MatchScore): boolean {
	return a.grade > b.grade || (a.grade === b.grade && a.span <= b.span);
}

/**
 * Commands in display order: grouped under headings, each group placed where
 * its best match ranked, so the first option shown is always the best match.
 */
export function grouped<T extends Rankable>(ranked: readonly T[]): { group: string; items: T[] }[] {
	const groups: { group: string; items: T[] }[] = [];
	const at = new Map<string, number>();
	for (const c of ranked) {
		let i = at.get(c.group);
		if (i === undefined) {
			i = groups.length;
			at.set(c.group, i);
			groups.push({ group: c.group, items: [] });
		}
		groups[i].items.push(c);
	}
	return groups;
}
