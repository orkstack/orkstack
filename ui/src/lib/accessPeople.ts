// Access → People, from what the engine serves: the principals seen to sign
// in over a window (ListPrincipals, read from the audit trail and the live
// sign-ins), the sign-ins it holds (ListSignins), the principals the document
// disables, and the literal principals its bindings name.
import type { Timestamp } from '@bufbuild/protobuf/wkt';
import type { AccessDocument, SeenPrincipal, Signin } from '#lib/gen/orkstack/v1/access_pb.ts';

export interface PersonRow {
	id: string;
	/** A name or email, for people, as the latest sign-in gave it; empty when none is known. */
	display: string;
	issuer: string;
	/** The last sign-in in the window read; undefined when none was. */
	lastSeen: Timestamp | undefined;
	signins: Signin[];
	disabled: boolean;
	/** The bindings that name this principal itself, not a group of it. */
	bindings: { name: string; role: string; scope: string }[];
}

const PRINCIPAL = /^(user|service|serviceaccount|cert):/;

/** The windows People reads sign-ins over, as the URL names them, with their seconds. */
export const PEOPLE_WINDOWS = { '30d': 30 * 86400, '7d': 7 * 86400, '1d': 86400 } as const;
export type PeopleWindow = keyof typeof PEOPLE_WINDOWS;
export const DEFAULT_PEOPLE_WINDOW: PeopleWindow = '30d';

export function peopleWindow(value: string | null): PeopleWindow {
	return value !== null && Object.hasOwn(PEOPLE_WINDOWS, value) ? (value as PeopleWindow) : DEFAULT_PEOPLE_WINDOW;
}

/** `orkctl admin principal list` for a window of so many hours. */
export function peopleCommand(w: PeopleWindow | number): string {
	return `orkctl admin principal list --window ${typeof w === 'number' ? w : PEOPLE_WINDOWS[w] / 3600}h`;
}

/** The windows, longest first: the order People steps down through. */
export const PEOPLE_WINDOW_ORDER: PeopleWindow[] = ['30d', '7d', '1d'];
/** A window as a choice ("30 days") and inside a sentence ("the last 30 days"). */
export const PEOPLE_WINDOW_LABEL: Record<PeopleWindow, string> = { '30d': '30 days', '7d': '7 days', '1d': '24 hours' };
export const PEOPLE_WINDOW_PHRASE: Record<PeopleWindow, string> = { '30d': 'the last 30 days', '7d': 'the last 7 days', '1d': 'the last 24 hours' };

/** One window's read: who was seen, and whether the list stopped short of its last page. */
export interface SeenPage {
	seen: SeenPrincipal[];
	truncated: boolean;
}

/** What People shows of who signed in, and over which window. */
export interface SeenRead {
	/** The window asked for. */
	asked: PeopleWindow;
	/** The window shown: the one asked for, or a shorter one the engine could read. Null when none could be. */
	shown: PeopleWindow | null;
	seen: SeenPrincipal[];
	truncated: boolean;
	/** Why nothing is shown: every window refused for its size, or a read that failed otherwise. */
	error: string | null;
	/** Whether the error is the size refusal at the shortest window. */
	tooLong: boolean;
}

/**
 * readSeen reads the window asked for and, while the engine refuses one as
 * examining more audit records than one request may, the next shorter one,
 * so the default works on a busy deployment. Any other failure stops it: a
 * shorter window would not mend it.
 */
export async function readSeen(
	asked: PeopleWindow,
	read: (w: PeopleWindow) => Promise<SeenPage>,
	tooLarge: (err: unknown) => boolean,
	words: (err: unknown) => string
): Promise<SeenRead> {
	let last: unknown = null;
	for (const w of PEOPLE_WINDOW_ORDER.slice(PEOPLE_WINDOW_ORDER.indexOf(asked))) {
		try {
			const page = await read(w);
			return { asked, shown: w, seen: page.seen, truncated: page.truncated, error: null, tooLong: false };
		} catch (err) {
			if (!tooLarge(err)) return { asked, shown: null, seen: [], truncated: false, error: words(err), tooLong: false };
			last = err;
		}
	}
	return { asked, shown: null, seen: [], truncated: false, error: words(last), tooLong: true };
}

/** The sentence saying which window People shows, when it is not the one asked for. */
export function steppedDownSentence(r: SeenRead): string {
	if (r.shown === null || r.shown === r.asked) return '';
	return `Sign-ins of ${PEOPLE_WINDOW_PHRASE[r.shown]}; ${PEOPLE_WINDOW_LABEL[r.asked]} examines more audit records than one request may.`;
}

const seconds = (t: Timestamp | undefined) => (t ? Number(t.seconds) + t.nanos / 1e9 : -Infinity);

/** Everyone the engine knows of here: seen most recently first, then the rest by id. */
export function people(doc: AccessDocument, signins: Signin[], seen: SeenPrincipal[] = []): PersonRow[] {
	const rows = new Map<string, PersonRow>();
	const row = (id: string) => {
		let r = rows.get(id);
		if (!r) {
			r = { id, display: '', issuer: '', lastSeen: undefined, signins: [], disabled: false, bindings: [] };
			rows.set(id, r);
		}
		return r;
	};
	for (const p of seen) {
		const r = row(p.id);
		r.display = p.display;
		r.issuer = p.issuer;
		r.lastSeen = p.lastSeen;
	}
	for (const s of signins) row(s.principalId).signins.push(s);
	for (const id of doc.disabled) row(id).disabled = true;
	for (const b of [...doc.configuredBindings, ...doc.bindings]) {
		for (const subject of b.subjects) {
			if (!PRINCIPAL.test(subject) || subject.endsWith('*')) continue;
			row(subject).bindings.push({ name: b.name, role: b.role, scope: b.scope });
		}
	}
	// Never seen reads as -Infinity: two such rows compare as NaN, which falls
	// through to the id.
	return [...rows.values()].sort((a, b) => seconds(b.lastSeen) - seconds(a.lastSeen) || a.id.localeCompare(b.id));
}
