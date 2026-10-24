import type { Status } from './gen/orkstack/v1/common_pb';
import type { Task } from './gen/orkstack/v1/orkflow_pb';

/** What the run page says about a DAG node: its status and the line under its id. */
export interface NodeState {
	status: Status;
	/** "ok · 0.4 s", "CONTAINER_TIMEOUT · 120 s", "never started". */
	line: string;
	/** The task never reached an Orker: drawn faded, with its solid outline. */
	neverStarted?: boolean;
}

export interface DagNode {
	task: Task;
	layer: number;
	index: number;
	x: number;
	y: number;
}

export interface DagEdge {
	fromId: string;
	toId: string;
	/** Where the edge leaves its source (the source's port). */
	x1: number;
	y1: number;
	/** Where it reaches its target (the target's port). */
	x2: number;
	y2: number;
	/**
	 * The orthogonal route, port to port: across a gap on the edge's own
	 * track, and through the lane it was given in every layer it passes.
	 */
	points: [number, number][];
}

/**
 * Which way the drawing reads: layers left to right (LR, a node's needs to
 * its left) or top to bottom (TB, its needs above it). Task ids are always
 * horizontal text.
 */
export type DagDirection = 'LR' | 'TB';

export interface DagLayout {
	nodes: DagNode[];
	edges: DagEdge[];
	width: number;
	height: number;
	direction: DagDirection;
}

// Drawing geometry, not tokens: user units of the SVG the DAG is drawn in.
// LAYER_GAP is the room an edge needs between two layers — its elbow and
// its arrowhead — and no more, so a five-layer Orkflow is drawn whole in
// the run page's frame at 1440 px (DagGraph scales the drawing down to the
// frame, never below DAG_MIN_SCALE).
export const DAG_NODE_W = 190;
export const DAG_NODE_H = 58;
const LAYER_GAP = 56;
const ROW_GAP = 34;
const PAD = 24;
// Top to bottom, a layer gap holds a vertical elbow and its arrowhead, and
// nodes side by side are a column gutter apart.
const TB_LAYER_GAP = 48;
const TB_COL_GAP = 24;
// A long edge's lane through a layer it passes (a dummy node), and the gap
// beside a lane: narrow, because it carries a line, not a box.
const LANE = 12;
const LANE_GAP = 10;
// A gap's tracks: the clear margin after a source and before a target (the
// arrowhead's room), the pitch between two tracks, and the widest a gap may
// grow to give its tracks that pitch.
const TRACK_LEAD = 10;
const TRACK_TAIL = 14;
const TRACK_PITCH = 8;
const GAP_MAX = 216;
// Barycentre sweeps: each round is one sweep down and one up.
const SWEEP_ROUNDS = 6;

// The smallest a drawing is scaled to fit its frame: below it the node
// text is too small to read, and the frame scrolls instead.
export const DAG_MIN_SCALE = 0.6;

// The dash of a failure handler's outline and of every edge into it — ISO
// 128's hidden line, meaning only "a handler". It is set as the SVG
// ATTRIBUTE, never the CSS property, because join-policy.spec.ts reads the
// attribute and the attribute must be the dash drawn.
export const DAG_DASH = '5 4';

// How many characters a node's second line holds: the node's width less its
// two text insets, over the mono face's advance (Spline Sans Mono, about
// 0.64 em, at --text-2xs, 11 px). The face is monospaced, so a count of
// characters is a width.
export const DAG_LINE_CHARS = 23;

/**
 * fitDagLine fits a node's second line into `room` characters: it drops
 * trailing " · " parts first (a duration after an error code), so what
 * names the outcome stays whole, and only then cuts the end with an
 * ellipsis. The full line stays in the node's accessible name and title.
 */
export function fitDagLine(line: string, room: number = DAG_LINE_CHARS): string {
	let fitted = line;
	while (fitted.length > room) {
		const cut = fitted.lastIndexOf(' · ');
		if (cut <= 0) break;
		fitted = fitted.slice(0, cut);
	}
	if (fitted.length <= room) return fitted;
	return `${fitted.slice(0, Math.max(room - 1, 0))}…`;
}

/**
 * fitDagRef fits a composed task's line, `orkflow <name>@<version>`, into
 * `room` characters: the name gives way (cut with an ellipsis) and the
 * version stays whole, because which version a run pins is the point. The
 * full reference stays in the node's title.
 */
export function fitDagRef(name: string, version: string, room: number = DAG_LINE_CHARS): string {
	const whole = `orkflow ${name}@${version}`;
	if (whole.length <= room) return whole;
	const keep = room - `orkflow …@${version}`.length;
	if (keep < 1) return fitDagLine(whole, room);
	return `orkflow ${name.slice(0, keep)}…@${version}`;
}

// How many tasks a legible DAG holds. Up to DAG_DRAW_LIMIT it is drawn;
// beyond, on request up to DAG_LEGIBLE_LIMIT; beyond that never laid out at
// all (each node is a link, and 4,000 of them is no drawing a person reads).
export const DAG_DRAW_LIMIT = 80;
export const DAG_LEGIBLE_LIMIT = 300;

export type DagBound = 'draw' | 'on-request' | 'too-large';

export function dagBound(tasks: number): DagBound {
	if (tasks <= DAG_DRAW_LIMIT) return 'draw';
	if (tasks <= DAG_LEGIBLE_LIMIT) return 'on-request';
	return 'too-large';
}

/** The fields layerOrder reads: a task's id and what it needs. */
export interface Needs {
	id: string;
	needs: readonly string[];
}

/**
 * layerOrder gives every task its layer — one more than the largest layer
 * among its needs — and the order the DAG is read in: layer by layer, and
 * declaration order within a layer (which is layoutDag's row order).
 *
 * It is iterative (Kahn's algorithm) and allocation-light: typed arrays
 * indexed by declaration position, a queue and a compressed dependents list,
 * O(tasks + needs), with no recursion and no edge objects — so the task map
 * can order a 4,000-task run of 200,000 needs in an idle callback. Unknown
 * needs are ignored, as layoutDag ignores them. A task Kahn cannot place (a
 * cycle, which validation forbids) takes the layer after the last, so every
 * task is ordered exactly once and the function always ends.
 */
export function layerOrder(tasks: readonly Needs[]): { layer: Int32Array; order: Int32Array } {
	const n = tasks.length;
	const pos = new Map<string, number>();
	for (let i = 0; i < n; i++) pos.set(tasks[i].id, i);

	// Each task's count of known needs, and each task's count of dependents
	// (shifted by one, so a prefix sum turns it into start offsets).
	const indegree = new Int32Array(n);
	const start = new Int32Array(n + 1);
	for (let i = 0; i < n; i++) {
		for (const need of tasks[i].needs) {
			const from = pos.get(need);
			if (from === undefined) continue;
			indegree[i]++;
			start[from + 1]++;
		}
	}
	// Compressed dependents: those of task u are out[start[u] .. start[u + 1]).
	for (let i = 0; i < n; i++) start[i + 1] += start[i];
	const fill = start.slice(0, n);
	const out = new Int32Array(start[n]);
	for (let i = 0; i < n; i++) {
		for (const need of tasks[i].needs) {
			const from = pos.get(need);
			if (from !== undefined) out[fill[from]++] = i;
		}
	}

	const layer = new Int32Array(n);
	const placed = new Uint8Array(n);
	const queue = new Int32Array(n);
	let head = 0;
	let tail = 0;
	for (let i = 0; i < n; i++) if (indegree[i] === 0) queue[tail++] = i;
	let maxLayer = 0;
	while (head < tail) {
		const u = queue[head++];
		placed[u] = 1;
		if (layer[u] > maxLayer) maxLayer = layer[u];
		for (let k = start[u]; k < start[u + 1]; k++) {
			const v = out[k];
			if (layer[u] + 1 > layer[v]) layer[v] = layer[u] + 1;
			if (--indegree[v] === 0) queue[tail++] = v;
		}
	}
	if (tail < n) {
		maxLayer += 1;
		for (let i = 0; i < n; i++) if (!placed[i]) layer[i] = maxLayer;
	}

	// Counting sort by layer, stable in declaration order.
	const at = new Int32Array(maxLayer + 2);
	for (let i = 0; i < n; i++) at[layer[i] + 1]++;
	for (let l = 0; l <= maxLayer; l++) at[l + 1] += at[l];
	const order = new Int32Array(n);
	for (let i = 0; i < n; i++) order[at[layer[i]]++] = i;
	return { layer, order };
}

// layoutDag draws the DAG as a layered drawing, the way a drafter lays out a
// flow sheet, in four steps:
//
//   1. LAYERS. Every task takes its layerOrder layer (one past its deepest
//      need). An edge that skips layers is given a LANE in each layer it
//      passes — a dummy node, a narrow slot of its own — so it is drawn
//      between the boxes of those layers, never through one.
//   2. ORDER. Within each layer the boxes and lanes are ordered to cross as
//      few edges as possible: barycentre sweeps, down and up, starting from
//      declaration order; the order with the fewest crossings seen is kept.
//      Every tie falls back to the order already held, so the same Orkflow
//      is always drawn the same way.
//   3. PLACE. Each layer is stacked from the top (top to bottom: from the
//      left) in that order: a box, a gutter, a lane, a narrow gap.
//   4. ROUTE. Between two layers each edge runs across, along a track of
//      the gap, and across again into its target. Edges that share a source
//      share its track (a split); otherwise edges that share a target share
//      its track (a join) — whichever of the two needs fewer tracks — and
//      the tracks are ordered to cross as little as they can. A gap widens,
//      up to a limit, to keep its tracks apart.
//
// Coordinates are worked in two axes — along the layers and across them —
// and only then turned into x and y, so left-to-right and top-to-bottom are
// one algorithm. A node keeps its size (190 × 58) and its horizontal text in
// both. It allocates per node, lane and edge, so it runs only for a DAG that
// is drawn (at most DAG_LEGIBLE_LIMIT tasks); the task map orders a run of
// any size with layerOrder alone.
export function layoutDag(tasks: Task[], direction: DagDirection = 'LR'): DagLayout {
	const lr = direction === 'LR';
	const along = lr ? DAG_NODE_W : DAG_NODE_H;
	const across = lr ? DAG_NODE_H : DAG_NODE_W;
	const gutter = lr ? ROW_GAP : TB_COL_GAP;
	const baseGap = lr ? LAYER_GAP : TB_LAYER_GAP;

	const { layer, order } = layerOrder(tasks);
	const n = tasks.length;
	const pos = new Map<string, number>();
	for (let i = 0; i < n; i++) pos.set(tasks[i].id, i);

	// ---- 1. Layers, with a lane for every layer a long edge passes ----
	const vLayer: number[] = Array.from(layer);
	const succ: number[][] = Array.from({ length: n }, () => []);
	const pred: number[][] = Array.from({ length: n }, () => []);
	const lane = (l: number): number => {
		vLayer.push(l);
		succ.push([]);
		pred.push([]);
		return vLayer.length - 1;
	};
	const link = (u: number, v: number) => {
		succ[u].push(v);
		pred[v].push(u);
	};
	interface Route {
		from: number;
		to: number;
		chain: number[];
	}
	const routes: Route[] = [];
	for (let i = 0; i < n; i++) {
		for (const need of new Set(tasks[i].needs)) {
			const j = pos.get(need);
			if (j === undefined || j === i) continue;
			const chain = [j];
			// A need in a later or the same layer exists only in a cycle,
			// which validation refuses: it is drawn without lanes.
			if (layer[i] > layer[j]) {
				let prev = j;
				for (let l = layer[j] + 1; l < layer[i]; l++) {
					const d = lane(l);
					link(prev, d);
					chain.push(d);
					prev = d;
				}
				link(prev, i);
			}
			chain.push(i);
			routes.push({ from: j, to: i, chain });
		}
	}
	const vCount = vLayer.length;
	const isLane = (v: number) => v >= n;
	const layers = n ? Math.max(...vLayer) + 1 : 1;
	const rows: number[][] = Array.from({ length: layers }, () => []);
	for (const i of order) rows[layer[i]].push(i);
	for (let v = n; v < vCount; v++) rows[vLayer[v]].push(v);

	// ---- 2. Order: barycentre sweeps, keeping the fewest crossings ----
	const rank = new Int32Array(vCount);
	const reRank = () => {
		for (const row of rows) row.forEach((v, k) => (rank[v] = k));
	};
	reRank();
	const sweep = (l: number, side: number[][]) => {
		const row = rows[l];
		const key = new Map<number, number>();
		for (const v of row) {
			const nb = side[v];
			let sum = 0;
			for (const u of nb) sum += rank[u];
			key.set(v, nb.length ? sum / nb.length : rank[v]);
		}
		row.sort((a, b) => key.get(a)! - key.get(b)! || rank[a] - rank[b]);
		row.forEach((v, k) => (rank[v] = k));
	};
	const crossings = () => {
		let total = 0;
		for (let l = 0; l + 1 < layers; l++) total += crossingsBetween(rows[l], succ, rank);
		return total;
	};
	let best = rows.map((r) => [...r]);
	let fewest = crossings();
	for (let round = 0; round < SWEEP_ROUNDS && fewest > 0; round++) {
		for (let l = 1; l < layers; l++) sweep(l, pred);
		for (let l = layers - 2; l >= 0; l--) sweep(l, succ);
		const c = crossings();
		if (c < fewest) {
			fewest = c;
			best = rows.map((r) => [...r]);
		}
	}
	for (let l = 0; l < layers; l++) rows[l] = best[l];
	reRank();

	// ---- 3. Place: stack each layer from the top ----
	// A lane is also kept level with the hop before it where the stack
	// allows (it moves down, never up), so a long edge runs as one straight
	// bypass instead of stepping at every layer it passes.
	const c0 = new Float64Array(vCount);
	const size = (v: number) => (isLane(v) ? LANE : across);
	const mid = (v: number) => c0[v] + size(v) / 2;
	let extent = 0;
	for (const row of rows) {
		let c = PAD;
		row.forEach((v, k) => {
			if (k > 0) c += isLane(v) || isLane(row[k - 1]) ? LANE_GAP : gutter;
			if (isLane(v)) c = Math.max(c, mid(pred[v][0]) - LANE / 2);
			c0[v] = c;
			c += size(v);
		});
		extent = Math.max(extent, c);
	}

	// ---- 4. Route: tracks in each gap ----
	interface Hop {
		u: number;
		v: number;
		group: number;
	}
	const trackOf = new Map<string, number>();
	const gapWidth: number[] = [];
	const hopsByGap: Hop[][] = [];
	for (let l = 0; l + 1 < layers; l++) {
		const hops: Hop[] = [];
		for (const u of rows[l]) for (const v of succ[u]) hops.push({ u, v, group: -1 });
		const sources = new Set(hops.map((h) => h.u)).size;
		const targets = new Set(hops.map((h) => h.v)).size;
		const bySource = sources <= targets;
		const groups = new Map<number, Hop[]>();
		for (const h of hops) {
			const k = bySource ? h.u : h.v;
			if (!groups.has(k)) groups.set(k, []);
			groups.get(k)!.push(h);
		}
		// A group's direction and where it starts, to order the tracks.
		const keys = [...groups.keys()];
		const meanOf = (hs: Hop[], end: 'u' | 'v') => hs.reduce((s, h) => s + mid(h[end]), 0) / hs.length;
		const info = new Map(keys.map((k) => [k, { from: meanOf(groups.get(k)!, 'u'), to: meanOf(groups.get(k)!, 'v') }]));
		keys.sort((a, b) => {
			const A = info.get(a)!;
			const B = info.get(b)!;
			const upA = A.to < A.from ? 0 : 1;
			const upB = B.to < B.from ? 0 : 1;
			if (upA !== upB) return upA - upB;
			// Rising: the higher start first. Falling: the lower start first.
			return (upA === 0 ? A.from - B.from : B.from - A.from) || a - b;
		});
		improveTracks(keys, groups, mid);
		keys.forEach((k, t) => groups.get(k)!.forEach((h) => (h.group = t)));
		const tracks = keys.length;
		gapWidth.push(Math.min(GAP_MAX, Math.max(baseGap, TRACK_LEAD + TRACK_TAIL + TRACK_PITCH * (tracks - 1))));
		hopsByGap.push(hops);
	}
	const a0: number[] = [PAD];
	for (let l = 1; l < layers; l++) a0.push(a0[l - 1] + along + gapWidth[l - 1]);
	for (let l = 0; l + 1 < layers; l++) {
		const hops = hopsByGap[l];
		const tracks = hops.reduce((m, h) => Math.max(m, h.group + 1), 0);
		const left = a0[l] + along;
		const width = gapWidth[l];
		const pitch = tracks > 1 ? Math.min(TRACK_PITCH, (width - TRACK_LEAD - TRACK_TAIL) / (tracks - 1)) : 0;
		const first = tracks > 1 ? left + (width - pitch * (tracks - 1)) / 2 : left + width / 2;
		for (const h of hops) trackOf.set(`${h.u}>${h.v}`, first + pitch * h.group);
	}

	// ---- Out: nodes, edges, size — turned into x and y ----
	const xy = (a: number, c: number): [number, number] => (lr ? [a, c] : [c, a]);
	const nodes: DagNode[] = [];
	for (const i of order) {
		const [x, y] = xy(a0[layer[i]], c0[i]);
		nodes.push({ task: tasks[i], layer: layer[i], index: rank[i], x, y });
	}
	const edges: DagEdge[] = routes.map(({ from, to, chain }) => {
		const pts: [number, number][] = [[a0[vLayer[from]] + along, mid(from)]];
		for (let k = 0; k + 1 < chain.length; k++) {
			const u = chain[k];
			const v = chain[k + 1];
			const t = trackOf.get(`${u}>${v}`);
			if (t === undefined) {
				// A cycle's edge: straight from port to port, one elbow.
				const ax = (pts[pts.length - 1][0] + a0[vLayer[v]]) / 2;
				pts.push([ax, mid(u)], [ax, mid(v)]);
			} else {
				pts.push([t, mid(u)], [t, mid(v)]);
			}
			pts.push([a0[vLayer[v]], mid(v)]);
			if (isLane(v)) pts.push([a0[vLayer[v]] + along, mid(v)]);
		}
		const points = straighten(pts).map(([a, c]) => xy(a, c));
		const [x1, y1] = points[0];
		const [x2, y2] = points[points.length - 1];
		return { fromId: tasks[from].id, toId: tasks[to].id, x1, y1, x2, y2, points };
	});
	const lengthAlong = a0[layers - 1] + along + PAD;
	const lengthAcross = Math.max(extent, PAD + across) + PAD;
	return {
		nodes,
		edges,
		width: lr ? lengthAlong : lengthAcross,
		height: lr ? lengthAcross : lengthAlong,
		direction
	};
}

/**
 * The crossings between one layer's out-edges, counted as inversions of
 * their targets' ranks once sorted by their sources' (Barth, Jünger and
 * Mutzel's accumulator: O(edges · log layer)).
 */
function crossingsBetween(row: readonly number[], succ: readonly number[][], rank: Int32Array): number {
	const ends: number[] = [];
	let width = 0;
	for (const u of row) {
		const vs = succ[u].map((v) => rank[v]).sort((a, b) => a - b);
		for (const r of vs) {
			ends.push(r);
			if (r + 1 > width) width = r + 1;
		}
	}
	const tree = new Int32Array(width + 1);
	let crossings = 0;
	for (let k = ends.length - 1; k >= 0; k--) {
		// Ends already seen that sit strictly below this one cross it.
		for (let i = ends[k]; i > 0; i -= i & -i) crossings += tree[i];
		for (let i = ends[k] + 1; i <= width; i += i & -i) tree[i]++;
	}
	return crossings;
}

/**
 * Swaps neighbouring tracks of one gap while that removes crossings between
 * their routes (across, along the track, across): a few passes over at most
 * a few dozen tracks.
 */
function improveTracks<H extends { u: number; v: number }>(
	keys: number[],
	groups: Map<number, H[]>,
	mid: (v: number) => number
): void {
	if (keys.length < 2) return;
	// Two groups on tracks t < t′ cross where a horizontal of one meets the
	// vertical of the other; the count depends only on their order.
	const cross = (left: H[], right: H[]): number => {
		let c = 0;
		for (const a of left) {
			const [a1, a2] = [mid(a.u), mid(a.v)];
			for (const b of right) {
				const [b1, b2] = [mid(b.u), mid(b.v)];
				// a's exit (to the right of its track) meets b's vertical.
				if (Math.min(b1, b2) < a2 && a2 < Math.max(b1, b2)) c++;
				// b's entry (to the left of its track) meets a's vertical.
				if (Math.min(a1, a2) < b1 && b1 < Math.max(a1, a2)) c++;
			}
		}
		return c;
	};
	for (let pass = 0; pass < 4; pass++) {
		let swapped = false;
		for (let k = 0; k + 1 < keys.length; k++) {
			const A = groups.get(keys[k])!;
			const B = groups.get(keys[k + 1])!;
			if (cross(B, A) < cross(A, B)) {
				[keys[k], keys[k + 1]] = [keys[k + 1], keys[k]];
				swapped = true;
			}
		}
		if (!swapped) break;
	}
}

/** Drops a point that lies on the straight line between its neighbours, and repeats. */
function straighten(points: [number, number][]): [number, number][] {
	const out: [number, number][] = [];
	for (const p of points) {
		const last = out[out.length - 1];
		if (last && last[0] === p[0] && last[1] === p[1]) continue;
		if (out.length >= 2) {
			const [ax, ay] = out[out.length - 2];
			const [bx, by] = last;
			if ((ax === bx && bx === p[0]) || (ay === by && by === p[1])) out.pop();
		}
		out.push(p);
	}
	return out;
}

/**
 * An edge as an SVG path. A routed edge follows its points, its end drawn
 * `inset` units short of the target so the arrowhead's tip meets the
 * outline; an unrouted one (two ports only) goes across, down, across.
 */
export function edgePath(
	e: { x1: number; y1: number; x2: number; y2: number; points?: [number, number][] },
	inset = 0
): string {
	if (!e.points?.length) {
		const mx = (e.x1 + e.x2) / 2;
		return `M ${e.x1} ${e.y1} H ${mx} V ${e.y2} H ${e.x2 - inset}`;
	}
	const pts = e.points.map(([x, y]) => [x, y] as [number, number]);
	if (inset && pts.length >= 2) {
		const [px, py] = pts[pts.length - 2];
		const [ex, ey] = pts[pts.length - 1];
		const len = Math.hypot(ex - px, ey - py) || 1;
		pts[pts.length - 1] = [ex - ((ex - px) / len) * inset, ey - ((ey - py) / len) * inset];
	}
	return pts.map(([x, y], k) => `${k ? 'L' : 'M'} ${round(x)} ${round(y)}`).join(' ');
}

const round = (v: number) => Math.round(v * 100) / 100;

/**
 * The one-hop neighbourhood of a task: its needs, itself and its dependents,
 * in declaration order, each with its needs trimmed to what is drawn so no
 * edge points off the drawing. What the run page draws past
 * DAG_LEGIBLE_LIMIT once a task is selected.
 */
export function neighbourhood(tasks: readonly Task[], id: string): Task[] {
	const self = tasks.find((t) => t.id === id);
	if (!self) return [];
	const keep = new Set<string>([id, ...self.needs]);
	for (const t of tasks) if (t.needs.includes(id)) keep.add(t.id);
	return tasks.filter((t) => keep.has(t.id)).map((t) => ({ ...t, needs: t.needs.filter((need) => keep.has(need)) }));
}

/**
 * Which way a DAG reads best in a frame of this size: the direction whose
 * drawing — scaled to the frame's width, never below DAG_MIN_SCALE — costs
 * a reader least to take in. The cost is the frames of it to scroll through,
 * a frame of sideways scrolling counting as two downward (a wheel and a page
 * move down), divided by the square of the scale it is drawn at (text drawn
 * at 0.6 is 8 px, and every step below full size costs more than the last). Left to right wins a tie, and anything within a tenth:
 * it is the drawing's usual direction, and a reader should not see it turn
 * for a sliver. So wide parallel stages stay left to right everywhere, and a
 * long chain — or a small DAG on a phone — turns top to bottom.
 */
export function chooseDagDirection(tasks: Task[], frame: { width: number; height: number }): DagDirection {
	if (frame.width <= 0 || frame.height <= 0 || tasks.length === 0) return 'LR';
	const cost = (l: DagLayout) => {
		const s = Math.max(DAG_MIN_SCALE, Math.min(1, frame.width / l.width));
		const wide = Math.max(1, (l.width * s) / frame.width);
		const tall = Math.max(1, (l.height * s) / frame.height);
		return (tall + 2 * (wide - 1)) / (s * s);
	};
	const lr = cost(layoutDag(tasks, 'LR'));
	const tb = cost(layoutDag(tasks, 'TB'));
	return tb < lr * 0.9 ? 'TB' : 'LR';
}

/**
 * The drawing's direction for a page: `auto` (chooseDagDirection, for the
 * frame it is drawn in) unless the address asks for one — `dag=lr` or
 * `dag=tb`, a reader's override that a shared link carries.
 */
export function dagDirectionParam(url: URL): DagDirection | 'auto' {
	const asked = url.searchParams.get('dag');
	return asked === 'lr' ? 'LR' : asked === 'tb' ? 'TB' : 'auto';
}
