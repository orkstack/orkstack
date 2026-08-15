import type { Task } from './gen/orkstack/v1/workflow_pb';

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

export const DAG_NODE_W = 190;
export const DAG_NODE_H = 58;
const LAYER_GAP = 110;
const ROW_GAP = 34;
const PAD = 24;

// layoutDag assigns each task a layer from its transitive `needs` depth and a
// row within the layer, producing coordinates for an SVG rendering of the DAG.
export function layoutDag(tasks: Task[]): DagLayout {
	const byId = new Map(tasks.map((t) => [t.id, t]));
	const depthMemo = new Map<string, number>();
	const visiting = new Set<string>();

	const depth = (id: string): number => {
		const memo = depthMemo.get(id);
		if (memo !== undefined) return memo;
		if (visiting.has(id)) return 0; // guard: DSL validation forbids cycles
		visiting.add(id);
		const task = byId.get(id);
		let d = 0;
		if (task) {
			for (const dep of task.needs) {
				if (byId.has(dep)) d = Math.max(d, depth(dep) + 1);
			}
		}
		visiting.delete(id);
		depthMemo.set(id, d);
		return d;
	};

	const layers = new Map<number, Task[]>();
	for (const task of tasks) {
		const d = depth(task.id);
		const layer = layers.get(d) ?? [];
		layer.push(task);
		layers.set(d, layer);
	}

	const nodes: DagNode[] = [];
	let maxLayer = 0;
	let maxRows = 0;
	for (const [layer, layerTasks] of [...layers.entries()].sort((a, b) => a[0] - b[0])) {
		maxLayer = Math.max(maxLayer, layer);
		maxRows = Math.max(maxRows, layerTasks.length);
		layerTasks.forEach((task, index) => {
			nodes.push({
				task,
				layer,
				index,
				x: PAD + layer * (DAG_NODE_W + LAYER_GAP),
				y: PAD + index * (DAG_NODE_H + ROW_GAP)
			});
		});
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
