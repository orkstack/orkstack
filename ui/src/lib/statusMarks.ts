// How each status is DRAWN: its mark (the shape before the word on a stamp,
// and later the cell of the task map) and its fill. Statuses differ by shape,
// never by hue alone (WCAG 1.4.1): only RUNNING (green), AWAITING (blue) and
// FAILED (red) carry colour at all, and each status's word is always written
// beside its mark. The CSS for every mark is in app.css's `.mark-*` primitives.
import { Status } from './gen/orkstack/v1/common_pb';
import { statusClasses } from './status';

export type MarkShape = 'square' | 'dot' | 'diamond' | 'triangle' | 'strike' | 'dash' | 'hollow' | 'dotted';
/** live: running blue; live-outline: awaiting violet; alarm: failed red; the rest are ink. */
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
	[Status.FAILED]: { shape: 'triangle', fill: 'alarm', className: 'mark-tri', inner: false },
	[Status.CANCELED]: { shape: 'strike', fill: 'outline', className: 'mark-strike', inner: false },
	[Status.SKIPPED]: { shape: 'dash', fill: 'hatched', className: 'mark-dash', inner: false },
	[Status.AWAITING]: { shape: 'diamond', fill: 'live-outline', className: 'mark-diamond', inner: true }
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

/** One SVG element of a drawn mark: never a <rect> (a DAG node holds exactly one). */
export type SvgMarkPart =
	| { el: 'path'; d: string; cls: string; dash?: string }
	| { el: 'circle'; cx: number; cy: number; r: number; cls: string };

/**
 * The same marks again, as SVG for a DAG node: drawing geometry in user
 * units at (x, y) and `size` square. Classes name what each part is
 * (`mark-fill-…`, `mark-line-…`); DagGraph's style colours them from the
 * tokens.
 */
export function svgMark(status: Status | undefined, x: number, y: number, size: number): SvgMarkPart[] {
	const s = size;
	const inset = `M ${x + 0.75} ${y + 0.75} h ${s - 1.5} v ${s - 1.5} h ${-(s - 1.5)} Z`;
	const cx = x + s / 2;
	const cy = y + s / 2;
	const word = statusClassWord(status);
	switch (statusMark(status).shape) {
		case 'square':
			return [{ el: 'path', d: `M ${x + 2} ${y} h ${s - 4} a 2 2 0 0 1 2 2 v ${s - 4} a 2 2 0 0 1 -2 2 h ${-(s - 4)} a 2 2 0 0 1 -2 -2 v ${-(s - 4)} a 2 2 0 0 1 2 -2 Z`, cls: `mark-fill-${word}` }];
		case 'dot':
			return [{ el: 'circle', cx, cy, r: s / 2, cls: `mark-fill-${word}` }];
		case 'diamond':
			return [{ el: 'path', d: `M ${cx} ${y + 1} L ${x + s - 1} ${cy} L ${cx} ${y + s - 1} L ${x + 1} ${cy} Z`, cls: `mark-line-${word}` }];
		case 'triangle':
			return [{ el: 'path', d: `M ${cx} ${y} L ${x + s} ${y + s} L ${x} ${y + s} Z`, cls: `mark-fill-${word}` }];
		case 'strike':
			return [
				{ el: 'path', d: inset, cls: `mark-line-${word}` },
				{ el: 'path', d: `M ${x + s - 1} ${y + 1} L ${x + 1} ${y + s - 1}`, cls: `mark-line-${word}` }
			];
		case 'dash':
			return [
				{ el: 'path', d: inset, cls: `mark-line-${word}` },
				{ el: 'path', d: `M ${x + 2} ${cy} L ${x + s - 2} ${cy}`, cls: `mark-line-${word}` }
			];
		case 'hollow':
			return [{ el: 'path', d: inset, cls: `mark-line-${word}` }];
		case 'dotted':
			return [{ el: 'path', d: inset, cls: `mark-line-${word}`, dash: '1 1.5' }];
	}
}

const CLASS_WORD: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'unspecified',
	[Status.PENDING]: 'pending',
	[Status.RUNNING]: 'running',
	[Status.SUCCEEDED]: 'succeeded',
	[Status.FAILED]: 'failed',
	[Status.CANCELED]: 'canceled',
	[Status.SKIPPED]: 'skipped',
	[Status.AWAITING]: 'awaiting'
};

/** The status's word as the token families spell it (`--color-failed-solid`). */
export function statusClassWord(status: Status | undefined): string {
	return CLASS_WORD[status ?? Status.UNSPECIFIED] ?? 'unspecified';
}
