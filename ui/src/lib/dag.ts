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
	x1: number;
	y1: number;
	x2: number;
	y2: number;
}

export interface DagLayout {
	nodes: DagNode[];
	edges: DagEdge[];
	width: number;
	height: number;
}

// Drawing geometry, not tokens: user units of the SVG the DAG is drawn in.
export const DAG_NODE_W = 190;
export const DAG_NODE_H = 58;
const LAYER_GAP = 110;
const ROW_GAP = 34;
const PAD = 24;

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

// layoutDag assigns each task its layer (layerOrder) and a row within the
// layer, producing coordinates for an SVG rendering of the DAG. It allocates
// a node per task and an edge per known need, so it runs only for a DAG that
// is drawn (at most DAG_LEGIBLE_LIMIT tasks); the task map orders a run of any
// size with layerOrder alone.
export function layoutDag(tasks: Task[]): DagLayout {
	const { layer, order } = layerOrder(tasks);

	const nodes: DagNode[] = [];
	let maxLayer = 0;
	let maxRows = 0;
	let currentLayer = -1;
	let index = 0;
	for (const i of order) {
		const l = layer[i];
		if (l !== currentLayer) {
			currentLayer = l;
			index = 0;
		}
		maxLayer = Math.max(maxLayer, l);
		maxRows = Math.max(maxRows, index + 1);
		nodes.push({
			task: tasks[i],
			layer: l,
			index,
			x: PAD + l * (DAG_NODE_W + LAYER_GAP),
			y: PAD + index * (DAG_NODE_H + ROW_GAP)
		});
		index++;
	}

	const nodeById = new Map(nodes.map((n) => [n.task.id, n]));
	const edges: DagEdge[] = [];
	for (const node of nodes) {
		for (const dep of node.task.needs) {
			const from = nodeById.get(dep);
			if (!from) continue;
			edges.push({
				fromId: from.task.id,
				toId: node.task.id,
				x1: from.x + DAG_NODE_W,
				y1: from.y + DAG_NODE_H / 2,
				x2: node.x,
				y2: node.y + DAG_NODE_H / 2
			});
		}
	}

	return {
		nodes,
		edges,
		width: PAD * 2 + (maxLayer + 1) * DAG_NODE_W + maxLayer * LAYER_GAP,
		height: PAD * 2 + Math.max(maxRows, 1) * DAG_NODE_H + Math.max(maxRows - 1, 0) * ROW_GAP
	};
}

/** An edge routed the way a drafter routes a leader: across, down, across. */
export function edgePath(e: { x1: number; y1: number; x2: number; y2: number }): string {
	const mid = (e.x1 + e.x2) / 2;
	return `M ${e.x1} ${e.y1} H ${mid} V ${e.y2} H ${e.x2}`;
}

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
