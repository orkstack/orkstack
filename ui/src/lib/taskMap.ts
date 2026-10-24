// The task map: one cell per task and per fan-out item, drawn by status,
// readable at the task ceiling. A <canvas>, never DOM — the run page already
// holds a card per task — painted in an idle callback after the cards are on
// screen, so nothing here is on the path that draws the header and the cards.
//
// This module is the map's pure half (model, geometry, keyboard, painting
// onto a context it is handed) and the small DOM half the run page needs to
// bring a card into view. TaskMap.svelte only wires them to the page.
import { Status } from './gen/orkstack/v1/common_pb';
import { layerOrder as defaultLayerOrder, type Needs } from './dag';
import { statusMark } from './statusMarks';

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/** A cell is a task, a fan-out's own cell (a stack), or one of its items. */
export const KIND_TASK = 0;
export const KIND_STACK = 1;
export const KIND_ITEM = 2;

export interface MapItem {
	index: number;
	status: Status;
}

/** What the map needs of a task: its id, status and needs, and its items if it fans out. */
export interface MapTask {
	id: string;
	status: Status;
	needs?: readonly string[];
	/** Present for a fan-out (an empty list until its array was counted). */
	items?: readonly MapItem[];
}

export interface MapCell {
	taskId: string;
	item?: number;
}

export interface Segment {
	start: number;
	count: number;
	/** A fan-out's section carries a label band above its first row. */
	labelled: boolean;
	taskId?: string;
}

export interface MapModel {
	order: 'dag' | 'run';
	cells: MapCell[];
	kinds: Uint8Array;
	segments: Segment[];
	/** The cell of a task, or of one of its items; -1 when it has none. */
	indexOf(taskId: string, item?: number): number;
}

const cellKey = (taskId: string, item?: number) => (item === undefined ? taskId : `${taskId}\u0000${item}`);

/**
 * The cells, in reading order: every task in DAG order (layer, then
 * declaration — layerOrder's order) or, with no definition to order by, the
 * run's own order; then, for each fan-out with items, a section of its items
 * in index order.
 */
export function buildModel(
	tasks: readonly MapTask[],
	definition: boolean,
	layerOrder: (t: readonly Needs[]) => { layer: Int32Array; order: Int32Array } = defaultLayerOrder
): MapModel {
	let ordered: readonly MapTask[] = tasks;
	if (definition) {
		const { order } = layerOrder(tasks.map((t) => ({ id: t.id, needs: t.needs ?? [] })));
		ordered = Array.from(order, (i) => tasks[i]);
	}
	const cells: MapCell[] = [];
	const kinds: number[] = [];
	for (const t of ordered) {
		cells.push({ taskId: t.id });
		kinds.push(t.items !== undefined ? KIND_STACK : KIND_TASK);
	}
	const segments: Segment[] = [{ start: 0, count: ordered.length, labelled: false }];
	for (const t of ordered) {
		if (!t.items || t.items.length === 0) continue;
		const start = cells.length;
		for (const item of t.items) {
			cells.push({ taskId: t.id, item: item.index });
			kinds.push(KIND_ITEM);
		}
		segments.push({ start, count: t.items.length, labelled: true, taskId: t.id });
	}
	const index = new Map<string, number>();
	cells.forEach((c, i) => index.set(cellKey(c.taskId, c.item), i));
	return {
		order: definition ? 'dag' : 'run',
		cells,
		kinds: Uint8Array.from(kinds),
		segments,
		indexOf: (taskId, item) => index.get(cellKey(taskId, item)) ?? -1
	};
}

// Worst first: what a person looking at a fan-out's one cell most needs to see.
const SEVERITY: Record<Status, number> = {
	[Status.FAILED]: 7,
	[Status.RUNNING]: 6,
	[Status.AWAITING]: 5,
	[Status.PENDING]: 4,
	[Status.UNSPECIFIED]: 3,
	[Status.CANCELED]: 2,
	[Status.SKIPPED]: 1,
	[Status.SUCCEEDED]: 0
};

/** The worst status among a fan-out's items, or undefined for none. */
export function worstStatus(statuses: readonly Status[]): Status | undefined {
	let worst: Status | undefined;
	for (const s of statuses) if (worst === undefined || SEVERITY[s] > SEVERITY[worst]) worst = s;
	return worst;
}

/**
 * Writes every cell's status into `out` (one byte per cell) and returns the
 * cells whose byte changed — the only ones a live batch repaints. A stack
 * cell shows its worst item, or the task's own status before it has any.
 */
export function fillStatuses(model: MapModel, tasks: readonly MapTask[], out: Uint8Array): number[] {
	const dirty: number[] = [];
	const set = (i: number, s: Status) => {
		if (i >= 0 && out[i] !== s) {
			out[i] = s;
			dirty.push(i);
		}
	};
	for (const t of tasks) {
		if (t.items && t.items.length > 0) {
			set(model.indexOf(t.id), worstStatus(t.items.map((i) => i.status)) ?? t.status);
			for (const item of t.items) set(model.indexOf(t.id, item.index), item.status);
		} else {
			set(model.indexOf(t.id), t.status);
		}
	}
	return dirty;
}

/** Cells per status — tasks and items; a stack is its items, not counted twice. */
export function countStatuses(statuses: Uint8Array, kinds: Uint8Array): Map<Status, number> {
	const counts = new Map<Status, number>();
	for (let i = 0; i < statuses.length; i++) {
		if (kinds[i] === KIND_STACK) continue;
		const s = statuses[i] as Status;
		counts.set(s, (counts.get(s) ?? 0) + 1);
	}
	return counts;
}

/** The order statuses are named in: the summary, the legend. */
export const STATUS_ORDER: readonly Status[] = [
	Status.SUCCEEDED,
	Status.RUNNING,
	Status.AWAITING,
	Status.FAILED,
	Status.CANCELED,
	Status.SKIPPED,
	Status.PENDING,
	Status.UNSPECIFIED
];

const STATUS_WORD: Record<Status, string> = {
	[Status.UNSPECIFIED]: 'unspecified',
	[Status.PENDING]: 'pending',
	[Status.RUNNING]: 'running',
	[Status.SUCCEEDED]: 'succeeded',
	[Status.FAILED]: 'failed',
	[Status.CANCELED]: 'canceled',
	[Status.SKIPPED]: 'skipped',
	[Status.AWAITING]: 'awaiting'
};

const fmt = new Intl.NumberFormat('en-US');

/** The map's description, read by a screen reader with the map. */
export function mapSummary(counts: Map<Status, number>, _total: number, order: 'dag' | 'run'): string {
	const parts = STATUS_ORDER.filter((s) => (counts.get(s) ?? 0) > 0).map((s) => `${fmt.format(counts.get(s)!)} ${STATUS_WORD[s]}`);
	let text = `${parts.join(', ')}; arrows move, Enter selects, F jumps to the next failed task.`;
	if (order === 'run') text += " Cells are in run order: this run's Orkflow definition could not be read.";
	return text;
}

// ---------------------------------------------------------------------------
// Geometry
// ---------------------------------------------------------------------------

/** The cell pitches, largest first; a cell is the pitch less the gap. */
export const PITCHES = [16, 14, 12, 10, 8] as const;
export const CELL_GAP = 2;
/** The band above a fan-out section where its label sits (DOM, not canvas). */
export const LABEL_H = 20;

export interface SegmentGeometry {
	start: number;
	count: number;
	rows: number;
	/** The top of the segment's first row of cells (below its label band). */
	y: number;
	labelled: boolean;
}

export interface MapGeometry {
	pitch: number;
	columns: number;
	/** The map's whole height in CSS pixels. */
	height: number;
	/** How far the map area scrolls (0 when it fits). */
	scroll: number;
	total: number;
	segments: SegmentGeometry[];
}

function layout(segments: readonly { count: number; labelled: boolean }[], pitch: number, width: number): MapGeometry {
	const columns = Math.max(1, Math.floor(width / pitch));
	let y = 0;
	let start = 0;
	const out: SegmentGeometry[] = [];
	for (const s of segments) {
		if (s.labelled) y += LABEL_H;
		const rows = Math.ceil(s.count / columns);
		out.push({ start, count: s.count, rows, y, labelled: s.labelled });
		y += rows * pitch;
		start += s.count;
	}
	return { pitch, columns, height: y, scroll: 0, total: start, segments: out };
}

/**
 * The largest pitch of PITCHES whose rows fit the map area; when even the
 * smallest does not, the smallest, and the area scrolls (inside its frame,
 * never the page).
 */
export function mapGeometry(segments: readonly { count: number; labelled: boolean }[], width: number, height: number): MapGeometry {
	for (const pitch of PITCHES) {
		const g = layout(segments, pitch, width);
		if (g.height <= height) return g;
	}
	const g = layout(segments, PITCHES[PITCHES.length - 1], width);
	return { ...g, scroll: Math.max(0, g.height - height) };
}

function segmentOf(g: MapGeometry, i: number): SegmentGeometry | undefined {
	for (const s of g.segments) if (i >= s.start && i < s.start + s.count) return s;
	return undefined;
}

export interface Rect {
	x: number;
	y: number;
	w: number;
	h: number;
}

export function rectOf(g: MapGeometry, i: number): Rect {
	const s = segmentOf(g, i);
	if (!s) return { x: 0, y: 0, w: 0, h: 0 };
	const local = i - s.start;
	const size = g.pitch - CELL_GAP;
	return { x: (local % g.columns) * g.pitch, y: s.y + Math.floor(local / g.columns) * g.pitch, w: size, h: size };
}

/** The cell under a point of the map, or -1 (a gap, a label band, past the end). */
export function cellAt(g: MapGeometry, x: number, y: number): number {
	if (x < 0 || y < 0) return -1;
	const col = Math.floor(x / g.pitch);
	if (col >= g.columns || x - col * g.pitch >= g.pitch - CELL_GAP) return -1;
	for (const s of g.segments) {
		if (y < s.y || y >= s.y + s.rows * g.pitch) continue;
		const row = Math.floor((y - s.y) / g.pitch);
		if (y - s.y - row * g.pitch >= g.pitch - CELL_GAP) return -1;
		const local = row * g.columns + col;
		return local < s.count ? s.start + local : -1;
	}
	return -1;
}

/** The scroll position that keeps the cursor's row inside the area. */
export function scrollForCursor(g: MapGeometry, cursor: number, scrollTop: number, areaHeight: number): number {
	const r = rectOf(g, cursor);
	if (r.y < scrollTop) return r.y;
	if (r.y + g.pitch > scrollTop + areaHeight) return r.y + g.pitch - areaHeight;
	return scrollTop;
}

/** Where the readout sits: beside its cell, flipped at the frame's edges, never outside it. */
export function readoutPosition(
	cell: Rect,
	size: { w: number; h: number },
	frame: { w: number; h: number },
	offset = 8
): { left: number; top: number } {
	let left = cell.x + cell.w + offset;
	if (left + size.w > frame.w) left = cell.x - offset - size.w;
	let top = cell.y + cell.h + offset;
	if (top + size.h > frame.h) top = cell.y - offset - size.h;
	return { left: Math.max(0, left), top: Math.max(0, top) };
}

// ---------------------------------------------------------------------------
// Keyboard
// ---------------------------------------------------------------------------

export interface MapKey {
	key: string;
	ctrlKey?: boolean;
	metaKey?: boolean;
	shiftKey?: boolean;
}

function down(g: MapGeometry, c: number): number {
	const s = segmentOf(g, c);
	if (!s) return c;
	const local = c - s.start;
	const row = Math.floor(local / g.columns);
	const col = local % g.columns;
	if (row + 1 < s.rows) return s.start + Math.min(local + g.columns, s.count - 1);
	const next = g.segments[g.segments.indexOf(s) + 1];
	return next && next.count > 0 ? next.start + Math.min(col, next.count - 1) : c;
}

function up(g: MapGeometry, c: number): number {
	const s = segmentOf(g, c);
	if (!s) return c;
	const local = c - s.start;
	const row = Math.floor(local / g.columns);
	const col = local % g.columns;
	if (row > 0) return c - g.columns;
	const prev = g.segments[g.segments.indexOf(s) - 1];
	if (!prev || prev.count === 0) return c;
	const lastRow = Math.floor((prev.count - 1) / g.columns) * g.columns;
	return prev.start + Math.min(lastRow + col, prev.count - 1);
}

function nextFailed(statuses: Uint8Array, from: number, step: 1 | -1): number {
	const n = statuses.length;
	for (let k = 1; k <= n; k++) {
		const i = (((from + step * k) % n) + n) % n;
		if (statuses[i] === Status.FAILED) return i;
	}
	return from;
}

/**
 * The map's keys: ←/→ the previous/next cell; ↑/↓ the cell a row above or
 * below; Home/End the row's ends; Ctrl+Home/End the first and last cell;
 * PageUp/PageDown by the visible rows; F / Shift+F the next/previous failed
 * cell, wrapping; Enter/Space select. null for a key the map leaves alone.
 */
export function mapKeydown(
	g: MapGeometry,
	cursor: number,
	ev: MapKey,
	opts: { statuses: Uint8Array; visibleRows: number }
): { cursor: number; select?: boolean } | null {
	const last = g.total - 1;
	if (last < 0) return null;
	const c = Math.min(Math.max(cursor, 0), last);
	const ctrl = ev.ctrlKey || ev.metaKey;
	const s = segmentOf(g, c)!;
	const rowStart = s.start + Math.floor((c - s.start) / g.columns) * g.columns;
	switch (ev.key) {
		case 'ArrowLeft':
			return { cursor: Math.max(0, c - 1) };
		case 'ArrowRight':
			return { cursor: Math.min(last, c + 1) };
		case 'ArrowDown':
			return { cursor: down(g, c) };
		case 'ArrowUp':
			return { cursor: up(g, c) };
		case 'Home':
			return { cursor: ctrl ? 0 : rowStart };
		case 'End':
			return { cursor: ctrl ? last : Math.min(rowStart + g.columns - 1, s.start + s.count - 1) };
		case 'PageDown':
		case 'PageUp': {
			let at = c;
			for (let k = 0; k < Math.max(1, opts.visibleRows); k++) at = ev.key === 'PageDown' ? down(g, at) : up(g, at);
			return { cursor: at };
		}
		case 'f':
		case 'F':
			if (ctrl) return null;
			return { cursor: nextFailed(opts.statuses, c, ev.shiftKey ? -1 : 1) };
		case 'Enter':
		case ' ':
			return { cursor: c, select: true };
	}
	return null;
}

// ---------------------------------------------------------------------------
// Painting
// ---------------------------------------------------------------------------

export interface MapColors {
	paper: string;
	ink: string;
	focus: string;
	status: Record<Status, { bg: string; solid: string }>;
}

/** Reads the map's colours from the tokens once (and again on a theme change). */
export function readColors(root: Element = document.documentElement): MapColors {
	if (typeof matchMedia === 'function' && matchMedia('(forced-colors: active)').matches) {
		const status = {} as MapColors['status'];
		for (const s of STATUS_ORDER) status[s] = { bg: 'Canvas', solid: 'CanvasText' };
		return { paper: 'Canvas', ink: 'CanvasText', focus: 'Highlight', status };
	}
	const css = getComputedStyle(root);
	const v = (name: string) => css.getPropertyValue(name).trim();
	const status = {} as MapColors['status'];
	for (const s of STATUS_ORDER) {
		const word = STATUS_WORD[s];
		status[s] = { bg: v(`--color-${word}-bg`), solid: v(`--color-${word}-solid`) };
	}
	return { paper: v('--color-paper'), ink: v('--color-ink'), focus: v('--color-focus'), status };
}

function line(ctx: CanvasRenderingContext2D, x1: number, y1: number, x2: number, y2: number): void {
	ctx.beginPath();
	ctx.moveTo(x1, y1);
	ctx.lineTo(x2, y2);
	ctx.stroke();
}

/**
 * Draws one cell's mark — the canvas rendering of the same table the CSS
 * stamps draw from ($lib/statusMarks): filled square, live dot, waiting ring,
 * crossed, struck, hatched, hollow, dotted.
 */
function drawMark(ctx: CanvasRenderingContext2D, r: Rect, status: Status, kind: number, colors: MapColors): void {
	const { x, y, w: s } = r;
	const c = colors.status[status] ?? colors.status[Status.UNSPECIFIED];
	ctx.lineWidth = 1;
	switch (statusMark(status).shape) {
		case 'square':
			ctx.fillStyle = c.solid;
			ctx.fillRect(x, y, s, s);
			break;
		case 'dot': {
			ctx.fillStyle = c.solid;
			ctx.fillRect(x, y, s, s);
			const d = Math.max(2, Math.round(s / 3));
			ctx.fillStyle = colors.paper;
			ctx.fillRect(x + (s - d) / 2, y + (s - d) / 2, d, d);
			break;
		}
		case 'ring':
			ctx.fillStyle = c.bg;
			ctx.fillRect(x, y, s, s);
			ctx.strokeStyle = c.solid;
			ctx.strokeRect(x + 0.5, y + 0.5, s - 1, s - 1);
			break;
		case 'cross':
			ctx.fillStyle = c.solid;
			ctx.fillRect(x, y, s, s);
			ctx.strokeStyle = colors.paper;
			line(ctx, x + 1, y + 1, x + s - 1, y + s - 1);
			line(ctx, x + s - 1, y + 1, x + 1, y + s - 1);
			break;
		case 'strike':
			ctx.fillStyle = c.bg;
			ctx.fillRect(x, y, s, s);
			ctx.strokeStyle = c.solid;
			ctx.strokeRect(x + 0.5, y + 0.5, s - 1, s - 1);
			line(ctx, x + s - 1, y + 1, x + 1, y + s - 1);
			break;
		case 'hatch':
			ctx.strokeStyle = c.solid;
			line(ctx, x, y + s / 2, x + s / 2, y);
			line(ctx, x + s / 2, y + s, x + s, y + s / 2);
			line(ctx, x, y + s, x + s, y);
			break;
		case 'hollow':
			ctx.strokeStyle = c.solid;
			ctx.strokeRect(x + 0.5, y + 0.5, s - 1, s - 1);
			break;
		case 'dotted':
			ctx.fillStyle = c.solid;
			for (const [dx, dy] of [
				[0, 0],
				[s - 1, 0],
				[0, s - 1],
				[s - 1, s - 1],
				[Math.floor(s / 2), 0],
				[Math.floor(s / 2), s - 1]
			])
				ctx.fillRect(x + dx, y + dy, 1, 1);
			break;
	}
	if (kind === KIND_STACK) {
		// A fan-out's own cell: its top-right corner notched in paper.
		const n = Math.max(2, Math.round(s / 3));
		ctx.fillStyle = colors.paper;
		ctx.beginPath();
		ctx.moveTo(x + s - n, y);
		ctx.lineTo(x + s, y);
		ctx.lineTo(x + s, y + n);
		ctx.closePath();
		ctx.fill();
	}
}

/**
 * Paints cells onto a context already scaled to CSS pixels: every cell, or
 * only `only` (a live batch's dirty cells). No computed style is read here —
 * the colours are handed in.
 */
export function paintCells(
	ctx: CanvasRenderingContext2D,
	g: MapGeometry,
	statuses: Uint8Array,
	kinds: Uint8Array,
	colors: MapColors,
	only?: Iterable<number>
): void {
	const paintOne = (i: number) => {
		const r = rectOf(g, i);
		ctx.fillStyle = colors.paper;
		ctx.fillRect(r.x, r.y, r.w, r.h);
		drawMark(ctx, r, statuses[i] as Status, kinds[i], colors);
	};
	if (only) {
		for (const i of only) if (i >= 0 && i < g.total) paintOne(i);
		return;
	}
	ctx.clearRect(0, 0, g.columns * g.pitch, g.height);
	for (let i = 0; i < g.total; i++) paintOne(i);
}

/** The overlay: the selected cell's ink frame (a paper gap inside it) and the keyboard cursor. */
export function paintOverlay(
	ctx: CanvasRenderingContext2D,
	g: MapGeometry,
	colors: MapColors,
	selected: number,
	cursor: number,
	showCursor: boolean
): void {
	ctx.clearRect(0, 0, g.columns * g.pitch + 4, g.height + 4);
	ctx.lineWidth = 2;
	if (selected >= 0) {
		const r = rectOf(g, selected);
		ctx.strokeStyle = colors.ink;
		ctx.strokeRect(r.x - 1, r.y - 1, r.w + 2, r.h + 2);
	}
	if (showCursor && cursor >= 0) {
		const r = rectOf(g, cursor);
		ctx.strokeStyle = colors.focus;
		ctx.strokeRect(r.x - 2, r.y - 2, r.w + 4, r.h + 4);
	}
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

export interface TaskMapDeps {
	layerOrder: (t: readonly Needs[]) => { layer: Int32Array; order: Int32Array };
	/** Paints the freshly built model (the component's painter). */
	paint: (model: MapModel, statuses: Uint8Array) => void;
	/** Runs fn when the browser is idle; returns its cancellation. */
	idle: (fn: () => void) => () => void;
}

/** The browser's idle scheduling: requestIdleCallback, else a task after the next frame. */
export function browserIdle(fn: () => void): () => void {
	if (typeof requestIdleCallback === 'function') {
		const h = requestIdleCallback(() => fn(), { timeout: 500 });
		return () => cancelIdleCallback(h);
	}
	let t: ReturnType<typeof setTimeout> | undefined;
	const f = requestAnimationFrame(() => (t = setTimeout(fn, 0)));
	return () => {
		cancelAnimationFrame(f);
		if (t !== undefined) clearTimeout(t);
	};
}

/**
 * The map's lifecycle. Creating it builds nothing; `schedule` asks for one
 * idle callback, which builds the model (layerOrder, or the run's order),
 * fills the status bytes and issues the first paint.
 */
export function createTaskMap(deps: TaskMapDeps) {
	let model: MapModel | null = null;
	let statuses = new Uint8Array(0);
	let cancel: (() => void) | null = null;
	return {
		schedule(input: () => { tasks: readonly MapTask[]; definition: boolean }): () => void {
			cancel?.();
			cancel = deps.idle(() => {
				cancel = null;
				const { tasks, definition } = input();
				model = buildModel(tasks, definition, deps.layerOrder);
				statuses = new Uint8Array(model.cells.length);
				fillStatuses(model, tasks, statuses);
				deps.paint(model, statuses);
			});
			return () => cancel?.();
		},
		model: () => model,
		statuses: () => statuses
	};
}

// ---------------------------------------------------------------------------
// Bringing a card into view
// ---------------------------------------------------------------------------

export interface ScrollEnv {
	/** The card's box, relative to the viewport. */
	rect(): { top: number; bottom: number } | null;
	scroll(): void;
	/** The band between the top bar and the title block. */
	viewport(): { top: number; bottom: number };
	/** Resolves after the next two animation frames. */
	frames(): Promise<void>;
}

/**
 * Scrolls a task card into view, exactly. The cards are content-visibility:
 * auto with a 7 rem estimate, so the first scroll to a card thousands down
 * lands where estimated cards put it, and the cards then drawn around it take
 * their real heights and move it. So: scroll, wait two frames, look again —
 * at most three passes, a bound, never a loop. Returns the passes made.
 */
export async function scrollToCard(env: ScrollEnv): Promise<number> {
	for (let passes = 0; ; passes++) {
		const r = env.rect();
		if (!r) return passes;
		const v = env.viewport();
		if (r.top >= v.top && r.bottom <= v.bottom) return passes;
		if (passes === 3) return passes;
		env.scroll();
		await env.frames();
	}
}

/** Resolves after two animation frames (the DOM half of ScrollEnv.frames). */
export function twoFrames(): Promise<void> {
	return new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
}
