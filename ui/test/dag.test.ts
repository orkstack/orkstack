import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { readFileSync, writeFileSync } from 'node:fs';
import { env } from 'node:process';
import { TaskSchema, type Task } from '../src/lib/gen/orkstack/v1/orkflow_pb';
import {
	DAG_DASH,
	DAG_DRAW_LIMIT,
	DAG_LINE_CHARS,
	DAG_LEGIBLE_LIMIT,
	dagBound,
	edgePath,
	fitDagLine,
	layerOrder,
	layoutDag,
	neighbourhood
} from '../src/lib/dag';
import { cpuMs } from './cpuTime';
import { DAG_FIXTURES } from './dagFixtures';

const tasksOf = (spec: [string, string[]][]): Task[] => spec.map(([id, needs]) => create(TaskSchema, { id, needs }));

// What layoutDag draws on each fixture, pinned so a change to the layout is
// a decision and never an accident. Recorded from the layered layout of
// 2026-10-22 (lanes for long edges, barycentre order, a track per split or
// join): against the drawing it replaced, `declaration-ties` lost its one
// crossing (`late` moved below `m`) and `skip-layers` drew its long edge
// a→d in a lane below b and c instead of through them; every other fixture
// kept every box where it was. UPDATE_DAG_PINS=1 rewrites the fixture.
const PIN_FILE = `${import.meta.dir}/fixtures/dag-layouts.json`;
const drawn = (spec: [string, string[]][]) => {
	const got = layoutDag(tasksOf(spec));
	return {
		nodes: got.nodes.map((n) => ({ id: n.task.id, layer: n.layer, index: n.index, x: n.x, y: n.y })),
		edges: got.edges,
		width: got.width,
		height: got.height
	};
};
if (env.UPDATE_DAG_PINS === '1') {
	writeFileSync(PIN_FILE, `${JSON.stringify(Object.fromEntries(Object.entries(DAG_FIXTURES).map(([k, v]) => [k, drawn(v)])), null, '\t')}\n`);
}
const PINNED = JSON.parse(readFileSync(PIN_FILE, 'utf8')) as Record<string, ReturnType<typeof drawn>>;

test('layoutDag draws every fixture exactly as pinned', () => {
	expect(Object.keys(PINNED).sort()).toEqual(Object.keys(DAG_FIXTURES).sort());
	for (const [name, spec] of Object.entries(DAG_FIXTURES)) expect(drawn(spec), name).toEqual(PINNED[name]);
});

/** The order layoutDag puts its nodes in, as declaration positions. */
function layoutOrder(tasks: Task[]): number[] {
	const pos = new Map(tasks.map((t, i) => [t.id, i]));
	return layoutDag(tasks).nodes.map((n) => pos.get(n.task.id)!);
}

test('layerOrder orders tasks as layoutDag does, on every fixture', () => {
	for (const spec of Object.values(DAG_FIXTURES)) {
		const tasks = tasksOf(spec);
		const { layer, order } = layerOrder(tasks);
		expect([...order]).toEqual(layoutOrder(tasks));
		const drawn = layoutDag(tasks).nodes;
		const pos = new Map(tasks.map((t, i) => [t.id, i]));
		for (const n of drawn) expect(layer[pos.get(n.task.id)!]).toBe(n.layer);
	}
});

// A small seeded generator, so a failure names a reproducible DAG.
function seeded(seed: number): () => number {
	let s = seed >>> 0;
	return () => {
		s = (s * 1664525 + 1013904223) >>> 0;
		return s / 2 ** 32;
	};
}

test('layerOrder agrees with layoutDag on seeded random DAGs', () => {
	for (let seed = 1; seed <= 40; seed++) {
		const rnd = seeded(seed);
		const n = 2 + Math.floor(rnd() * 60);
		// Shuffle declaration order so a task can be declared before its needs.
		const ids = Array.from({ length: n }, (_, i) => `t${i}`);
		const spec: [string, string[]][] = ids.map((id, i) => {
			const needs: string[] = [];
			for (let j = 0; j < i; j++) if (rnd() < 0.15) needs.push(ids[j]);
			if (rnd() < 0.1) needs.push('missing');
			return [id, needs];
		});
		for (let i = spec.length - 1; i > 0; i--) {
			const j = Math.floor(rnd() * (i + 1));
			[spec[i], spec[j]] = [spec[j], spec[i]];
		}
		const tasks = tasksOf(spec);
		expect([...layerOrder(tasks).order]).toEqual(layoutOrder(tasks));
	}
});

test('layerOrder handles a 100,000-task chain without recursion', () => {
	const n = 100_000;
	const tasks = Array.from({ length: n }, (_, i) => ({ id: `t${i}`, needs: i ? [`t${i - 1}`] : [] }));
	// Bounded on CPU time, not the wall clock (cpuTime.ts): a busy host delays
	// the ordering without adding much to it. One run: this one is long enough
	// that three would meet the runner's own per-test timeout on a loaded host.
	const started = cpuMs();
	const { layer, order } = layerOrder(tasks);
	const ms = cpuMs() - started;
	expect(layer[n - 1]).toBe(n - 1);
	expect(order[0]).toBe(0);
	expect(order[n - 1]).toBe(n - 1);
	// Linear: a chain this long costs well under a second of CPU time.
	expect(ms < 1000).toBe(true);
});

test('layerOrder handles a dense 4,000-task DAG (each needs the previous 50) in linear time and builds no edge', () => {
	const n = 4000;
	const tasks = Array.from({ length: n }, (_, i) => ({
		id: `t${i}`,
		needs: Array.from({ length: Math.min(i, 50) }, (_, k) => `t${i - 1 - k}`)
	}));
	const started = cpuMs();
	const { layer, order } = layerOrder(tasks);
	const ms = cpuMs() - started;
	expect(layer[n - 1]).toBe(n - 1);
	expect(new Set(order).size).toBe(n);
	expect(ms < 1000).toBe(true);
	// It returns two typed arrays and nothing per edge.
	expect(layer instanceof Int32Array).toBe(true);
	expect(order instanceof Int32Array).toBe(true);
});

test('a cycle still orders every task exactly once, the unplaced ones after the last layer', () => {
	const tasks = [
		{ id: 'a', needs: [] },
		{ id: 'b', needs: ['c'] },
		{ id: 'c', needs: ['b'] },
		{ id: 'd', needs: ['a'] }
	];
	const { layer, order } = layerOrder(tasks);
	expect([...order]).toEqual([0, 3, 1, 2]);
	expect([...layer]).toEqual([0, 2, 2, 1]);
});

test('edgePath routes an edge orthogonally: across, down, across', () => {
	expect(edgePath({ x1: 214, y1: 53, x2: 324, y2: 145 })).toBe('M 214 53 H 269 V 145 H 324');
	// A straight edge stays straight.
	expect(edgePath({ x1: 0, y1: 10, x2: 100, y2: 10 })).toBe('M 0 10 H 50 V 10 H 100');
	// A routed edge follows its points; the inset shortens its last leg so
	// the arrowhead's tip meets the target's outline, whichever way it runs.
	const routed = { x1: 0, y1: 0, x2: 30, y2: 40, points: [[0, 0], [10, 0], [10, 40], [30, 40]] as [number, number][] };
	expect(edgePath(routed)).toBe('M 0 0 L 10 0 L 10 40 L 30 40');
	expect(edgePath(routed, 2)).toBe('M 0 0 L 10 0 L 10 40 L 28 40');
	const down = { x1: 0, y1: 0, x2: 0, y2: 50, points: [[0, 0], [0, 50]] as [number, number][] };
	expect(edgePath(down, 2)).toBe('M 0 0 L 0 48');
});

test('the failure-handler dash is the attribute join-policy.spec.ts reads', () => {
	expect(DAG_DASH).toBe('5 4');
});

test('a node line fits its node: durations go first, then the end is cut', () => {
	expect(fitDagLine('ok · 1.6s')).toBe('ok · 1.6s');
	expect(fitDagLine('CONTAINER_EXIT_NONZERO · 1.6s')).toBe('CONTAINER_EXIT_NONZERO');
	expect(fitDagLine('ARTIFACT_UPLOAD_FAILED_AFTER_RETRIES · 12m 3s')).toBe('ARTIFACT_UPLOAD_FAILED…');
	expect(fitDagLine('ghcr.io/acme/migrate@sha256:0123456789abcdef')).toHaveLength(DAG_LINE_CHARS);
	expect(fitDagLine('handler · never ran')).toBe('handler · never ran');
	// A cached node gives "cached " of its room to the tag.
	expect(fitDagLine('CONTAINER_EXIT_NONZERO · 1.6s', DAG_LINE_CHARS - 7)).toBe('CONTAINER_EXIT_…');
});

test('the DAG bound: draw up to 80, on request to 300, never above', () => {
	expect(DAG_DRAW_LIMIT).toBe(80);
	expect(DAG_LEGIBLE_LIMIT).toBe(300);
	expect(dagBound(0)).toBe('draw');
	expect(dagBound(80)).toBe('draw');
	expect(dagBound(81)).toBe('on-request');
	expect(dagBound(300)).toBe('on-request');
	expect(dagBound(301)).toBe('too-large');
	expect(dagBound(4000)).toBe('too-large');
});

test('the one-hop neighbourhood is the task, its needs and its dependents, in declaration order', () => {
	const tasks = tasksOf(DAG_FIXTURES['order-saga']);
	expect(neighbourhood(tasks, 'charge').map((t) => t.id)).toEqual(['reserve-stock', 'charge', 'ship', 'refund']);
	expect(neighbourhood(tasks, 'validate').map((t) => t.id)).toEqual(['validate', 'reserve-stock']);
	expect(neighbourhood(tasks, 'nope')).toEqual([]);
	// Its needs are trimmed to what is drawn, so no edge points off the drawing.
	const drawn = neighbourhood(tasks, 'ship');
	expect(drawn.map((t) => [t.id, t.needs])).toEqual([
		['charge', []],
		['ship', ['charge']],
		['report', ['ship']]
	]);
});

test("a composed task's line keeps its version whole and lets the name give way", async () => {
	const { fitDagRef } = await import('../src/lib/dag');
	expect(fitDagRef('notify', '1.0.0')).toBe('orkflow notify@1.0.0');
	expect(fitDagRef('release-block', '0.1.0')).toBe('orkflow release-…@0.1.0');
	expect(fitDagRef('release-block', '0.1.0')).toHaveLength(DAG_LINE_CHARS);
	// A cached node gives "cached " of its room to the tag.
	expect(fitDagRef('release-block', '0.1.0', DAG_LINE_CHARS - 7)).toBe('orkflow r…@0.1.0');
	// A version too long for any of the name to show is cut like any line.
	expect(fitDagRef('a', '2026.10.22-rc.1+build.7')).toHaveLength(DAG_LINE_CHARS);
});
