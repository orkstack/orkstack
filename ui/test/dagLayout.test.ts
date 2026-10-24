import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { JoinPolicy } from '../src/lib/gen/orkstack/v1/activity_pb';
import { TaskSchema, type Task } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import { DAG_NODE_H, DAG_NODE_W, chooseDagDirection, dagDirectionParam, layoutDag, type DagDirection, type DagLayout } from '../src/lib/dag';
import { buildModel } from '../src/lib/taskMap';
import { DAG_STRESS, PLANAR, type StressTask } from './dagStress';

// What a drawn DAG must be, checked on every stress shape (test/dagStress.ts)
// left to right and top to bottom: every task drawn once and every need
// drawn once; no box over another; every edge orthogonal, from its source's
// port to its target's, never through a box, always forward; the drawing
// holds all of it; the same Orkflow always drawn the same way; and a shape
// that can be drawn without crossings drawn without them.

const JOIN: Record<string, JoinPolicy> = {
	none_failed: JoinPolicy.NONE_FAILED,
	any_failed: JoinPolicy.ANY_FAILED,
	all_done: JoinPolicy.ALL_DONE
};

export function stressTasks(spec: StressTask[]): Task[] {
	return spec.map((s) =>
		create(TaskSchema, {
			id: s.id,
			needs: s.needs,
			pin: s.pin ?? '',
			join: s.join ? JOIN[s.join] : JoinPolicy.UNSPECIFIED,
			each: s.each ? { over: s.each.over, maxItems: s.each.maxItems } : undefined,
			orkflow: s.orkflow,
			await: s.await ? {} : undefined
		})
	);
}

type Seg = { x1: number; y1: number; x2: number; y2: number };

function segments(points: [number, number][]): Seg[] {
	const out: Seg[] = [];
	for (let k = 0; k + 1 < points.length; k++) {
		const [x1, y1] = points[k];
		const [x2, y2] = points[k + 1];
		out.push({ x1, y1, x2, y2 });
	}
	return out;
}

const horizontal = (s: Seg) => s.y1 === s.y2;
const between = (v: number, a: number, b: number) => Math.min(a, b) < v && v < Math.max(a, b);

/** A horizontal and a vertical segment meeting inside both. */
function crosses(a: Seg, b: Seg): boolean {
	if (horizontal(a) === horizontal(b)) return false;
	const [h, v] = horizontal(a) ? [a, b] : [b, a];
	return between(v.x1, h.x1, h.x2) && between(h.y1, v.y1, v.y2);
}

/** Edge crossings in the drawing; edges sharing an end merge on purpose and are not counted. */
export function drawnCrossings(layout: DagLayout): number {
	let count = 0;
	const segs = layout.edges.map((e) => segments(e.points));
	for (let i = 0; i < layout.edges.length; i++) {
		for (let j = i + 1; j < layout.edges.length; j++) {
			const a = layout.edges[i];
			const b = layout.edges[j];
			if (a.fromId === b.fromId || a.toId === b.toId) continue;
			for (const s of segs[i]) for (const t of segs[j]) if (crosses(s, t)) count++;
		}
	}
	return count;
}

/** Does a segment pass through a box's inside? */
function throughBox(s: Seg, x: number, y: number): boolean {
	const [l, r, t, b] = [x + 0.5, x + DAG_NODE_W - 0.5, y + 0.5, y + DAG_NODE_H - 0.5];
	if (horizontal(s)) return s.y1 > t && s.y1 < b && Math.max(s.x1, s.x2) > l && Math.min(s.x1, s.x2) < r;
	return s.x1 > l && s.x1 < r && Math.max(s.y1, s.y2) > t && Math.min(s.y1, s.y2) < b;
}

const DIRECTIONS: DagDirection[] = ['LR', 'TB'];

for (const [name, spec] of Object.entries(DAG_STRESS)) {
	for (const direction of DIRECTIONS) {
		test(`${name} (${direction}): every task and need drawn once, no box over another, every edge clear and forward`, () => {
			const tasks = stressTasks(spec);
			const layout = layoutDag(tasks, direction);
			const ids = tasks.map((t) => t.id);

			// Every task exactly once; every known need exactly once.
			expect(layout.nodes.map((n) => n.task.id).sort()).toEqual([...ids].sort());
			const needs = tasks.flatMap((t) => [...new Set(t.needs)].filter((n) => ids.includes(n)).map((n) => `${n}>${t.id}`));
			expect(layout.edges.map((e) => `${e.fromId}>${e.toId}`).sort()).toEqual(needs.sort());

			// No two boxes touch.
			const at = new Map(layout.nodes.map((n) => [n.task.id, n]));
			const boxes = layout.nodes;
			for (let i = 0; i < boxes.length; i++) {
				for (let j = i + 1; j < boxes.length; j++) {
					const a = boxes[i];
					const b = boxes[j];
					const apart =
						a.x + DAG_NODE_W <= b.x || b.x + DAG_NODE_W <= a.x || a.y + DAG_NODE_H <= b.y || b.y + DAG_NODE_H <= a.y;
					expect(apart, `${a.task.id} and ${b.task.id} overlap`).toBe(true);
				}
			}

			// The drawing holds every box and every route.
			for (const n of boxes) {
				expect(n.x >= 0 && n.y >= 0 && n.x + DAG_NODE_W <= layout.width && n.y + DAG_NODE_H <= layout.height).toBe(true);
			}

			for (const e of layout.edges) {
				const from = at.get(e.fromId)!;
				const to = at.get(e.toId)!;
				const label = `${e.fromId}→${e.toId}`;
				// From the source's port to the target's: right to left side
				// (LR), bottom to top (TB).
				if (direction === 'LR') {
					expect([e.x1, e.y1], label).toEqual([from.x + DAG_NODE_W, from.y + DAG_NODE_H / 2]);
					expect([e.x2, e.y2], label).toEqual([to.x, to.y + DAG_NODE_H / 2]);
					expect(to.x > from.x, `${label} goes forward`).toBe(true);
				} else {
					expect([e.x1, e.y1], label).toEqual([from.x + DAG_NODE_W / 2, from.y + DAG_NODE_H]);
					expect([e.x2, e.y2], label).toEqual([to.x + DAG_NODE_W / 2, to.y]);
					expect(to.y > from.y, `${label} goes forward`).toBe(true);
				}
				expect(e.points[0]).toEqual([e.x1, e.y1]);
				expect(e.points[e.points.length - 1]).toEqual([e.x2, e.y2]);
				for (const s of segments(e.points)) {
					// Orthogonal, inside the drawing, and through no box.
					expect(s.x1 === s.x2 || s.y1 === s.y2, `${label} is orthogonal`).toBe(true);
					for (const v of [s.x1, s.x2]) expect(v >= 0 && v <= layout.width).toBe(true);
					for (const v of [s.y1, s.y2]) expect(v >= 0 && v <= layout.height).toBe(true);
					for (const n of boxes) expect(throughBox(s, n.x, n.y), `${label} passes through ${n.task.id}`).toBe(false);
				}
			}
		});

		test(`${name} (${direction}): the same Orkflow is drawn the same way`, () => {
			const a = layoutDag(stressTasks(spec), direction);
			const b = layoutDag(stressTasks(structuredClone(spec)), direction);
			expect(JSON.stringify(b)).toBe(JSON.stringify(a));
		});
	}
}

for (const name of PLANAR) {
	test(`${name}: a shape that can be drawn without crossings is drawn without one, both ways`, () => {
		for (const direction of DIRECTIONS) {
			const layout = layoutDag(stressTasks(DAG_STRESS[name]), direction);
			expect(drawnCrossings(layout), `${name} ${direction}`).toBe(0);
		}
	});
}

test('crossings are reduced on the shapes that must have some', () => {
	// What declaration order alone would draw: the bipartite stage at its
	// minimum, and the random and staged DAGs well under the order they were
	// declared in.
	const k = layoutDag(stressTasks(DAG_STRESS.k5x5));
	// K(5,5) cannot be drawn without crossings; a layered drawing of it has
	// at least C(5,2)·C(5,2) = 100.
	expect(drawnCrossings(k)).toBeLessThanOrEqual(100);
});

test('the task map holds every task and every item exactly once, for every shape', () => {
	for (const [name, spec] of Object.entries(DAG_STRESS)) {
		const tasks = spec.map((s) => ({
			id: s.id,
			status: 0,
			needs: s.needs,
			items: s.each ? Array.from({ length: 4 }, (_, index) => ({ index, status: 0 })) : undefined
		}));
		const model = buildModel(tasks, true);
		const taskCells = model.cells.filter((c) => c.item === undefined).map((c) => c.taskId);
		expect(taskCells.sort(), name).toEqual(spec.map((s) => s.id).sort());
		for (const s of spec.filter((x) => x.each)) {
			expect(model.cells.filter((c) => c.taskId === s.id && c.item !== undefined).map((c) => c.item)).toEqual([0, 1, 2, 3]);
		}
	}
});

test('a DAG at the legible limit lays out in well under a frame budget', () => {
	for (const name of ['random-200', 'staged-15x20']) {
		const tasks = stressTasks(DAG_STRESS[name]);
		const started = performance.now();
		layoutDag(tasks, 'LR');
		layoutDag(tasks, 'TB');
		expect(performance.now() - started, name).toBeLessThan(400);
	}
});

// The frames the run page draws in: its drawing column at 1440 × 900, and a
// 390 px phone's frame, each with the 327 px of drawing room a 900 px
// window's frame holds.
const DESK = { width: 989, height: 327 };
const PHONE = { width: 339, height: 327 };

test('auto: wide stages stay left to right; a long chain, and a small DAG on a phone, turn top to bottom', () => {
	const pick = (name: string, frame: { width: number; height: number }) => chooseDagDirection(stressTasks(DAG_STRESS[name]), frame);
	expect(pick('parallel-1x50', DESK)).toBe('LR');
	expect(pick('parallel-1x50', PHONE)).toBe('LR');
	expect(pick('staged-5x20', DESK)).toBe('LR');
	// Forty steps read at full size down the frame rather than at 0.6 across
	// six frames of it, on a desk too.
	expect(pick('chain-40', DESK)).toBe('TB');
	expect(pick('chain-40', PHONE)).toBe('TB');
	expect(pick('diamond', DESK)).toBe('LR');
	expect(pick('diamond', PHONE)).toBe('TB');
	// Nothing to measure yet, or nothing drawn: the usual direction.
	expect(chooseDagDirection(stressTasks(DAG_STRESS['chain-40']), { width: 0, height: 0 })).toBe('LR');
	expect(chooseDagDirection([], PHONE)).toBe('LR');
});

test('a page draws its DAG auto unless the address says dag=lr or dag=tb', () => {
	const at = (q: string) => dagDirectionParam(new URL(`http://x/orkflows/a${q}`));
	expect(at('')).toBe('auto');
	expect(at('?dag=auto')).toBe('auto');
	expect(at('?dag=lr')).toBe('LR');
	expect(at('?dag=tb')).toBe('TB');
	expect(at('?dag=sideways')).toBe('auto');
});
