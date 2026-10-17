// How each status is DRAWN: its mark (the shape before the word on a stamp,
// and later the cell of the task map) and its fill. Statuses differ by shape,
// never by hue alone (WCAG 1.4.1): only RUNNING/AWAITING (blue) and FAILED
// (red) carry colour at all, and each status's word is always written beside
// its mark. The CSS for every mark is in app.css's `.mark-*` primitives.
import { Status } from './gen/orkstack/v1/common_pb';
import { statusClasses } from './status';

export type MarkShape = 'square' | 'dot' | 'ring' | 'cross' | 'strike' | 'hatch' | 'hollow' | 'dotted';
/** live: the drafting blue; alarm: the revision red; the rest are ink. */
export type MarkFill = 'solid' | 'live' | 'live-outline' | 'alarm' | 'outline' | 'hatched' | 'none';

export interface StatusMark {
	shape: MarkShape;
	fill: MarkFill;
	/** The class that draws it: on the stamp itself for ::before marks, on the inner span for dot/ring. */
	className: string;
	/** True when the mark is the stamp's one inner element rather than its ::before. */
	inner: boolean;
}

export const STATUS_MARKS: Record<Status, StatusMark> = {
	[Status.UNSPECIFIED]: { shape: 'dotted', fill: 'none', className: 'mark-dotted', inner: false },
	[Status.PENDING]: { shape: 'hollow', fill: 'outline', className: 'mark-hollow', inner: false },
	[Status.RUNNING]: { shape: 'dot', fill: 'live', className: 'mark-dot', inner: true },
	[Status.SUCCEEDED]: { shape: 'square', fill: 'solid', className: 'mark-square', inner: false },
	[Status.FAILED]: { shape: 'cross', fill: 'alarm', className: 'mark-cross', inner: false },
	[Status.CANCELED]: { shape: 'strike', fill: 'outline', className: 'mark-strike', inner: false },
	[Status.SKIPPED]: { shape: 'hatch', fill: 'hatched', className: 'mark-hatch', inner: false },
	[Status.AWAITING]: { shape: 'ring', fill: 'live-outline', className: 'mark-ring', inner: true }
};

export function statusMark(status: Status | undefined): StatusMark {
	return STATUS_MARKS[status ?? Status.UNSPECIFIED] ?? STATUS_MARKS[Status.UNSPECIFIED];
}

const STAMP_CLASS = new Map<Status, string>(
	(Object.keys(STATUS_MARKS).map(Number) as Status[]).map((s) => {
		const mark = STATUS_MARKS[s];
		return [s, `stamp ${statusClasses(s)}${mark.inner ? '' : ` ${mark.className}`}`];
	})
);

/** The whole class of a status stamp, computed once per status. */
export function stampClass(status: Status | undefined): string {
	return STAMP_CLASS.get(status ?? Status.UNSPECIFIED) ?? STAMP_CLASS.get(Status.UNSPECIFIED)!;
}
