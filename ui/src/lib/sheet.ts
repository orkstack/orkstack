// The title block's state: what the page you are on says about itself, in
// the persistent row along the bottom edge (ORKFLOW, VERSION, DIGEST,
// NAMESPACE, STATUS on a run; PAGE and NAMESPACE where a page says nothing).
//
// A page registers a GETTER, not a value, so a field the page holds in
// $state (a run's live status) updates the block with no further call. The
// registration's cleanup clears the holder only if it still holds THIS
// registration: on a client navigation the new page's init may run before the
// old page's teardown, and an unguarded cleanup would erase the new block.
import type { Status } from './gen/orkstack/v1/common_pb';

export interface TitleField {
	/** The tracked-caps label: ORKFLOW, VERSION, … Also the testid suffix, lower-cased. */
	label: string;
	value: string;
	/** A digest or an id: set in the mono. */
	mono?: boolean;
	href?: string;
	/** STATUS: drawn with its mark, in its status ink. */
	status?: Status;
	/** Full text for a shortened value (a digest's whole hash). */
	title?: string;
}

export type TitleGetter = () => TitleField[];

export class TitleBlockHolder {
	#token = 0;
	#current: { token: number; get: TitleGetter } | null = null;
	#notify: () => void;

	constructor(notify: () => void = () => {}) {
		this.#notify = notify;
	}

	register(get: TitleGetter): () => void {
		const token = ++this.#token;
		this.#current = { token, get };
		this.#notify();
		return () => {
			if (this.#current?.token === token) {
				this.#current = null;
				this.#notify();
			}
		};
	}

	/** The page's fields, or null when no page registered any. */
	fields(): TitleField[] | null {
		return this.#current ? this.#current.get() : null;
	}
}

/** The testid of a cell: `title-block-{label}`, lower-cased with spaces as '-'. */
export function titleCellTestid(label: string): string {
	return `title-block-${label.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
}
