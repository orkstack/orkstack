// The shapes the DAG has to draw well, as plain task specs: wide parallel
// stages, diamonds, long chains, joins and failure handlers, pins, a
// fan-out, composed tasks, awaits, shapes that invite crossings, and DAGs
// near what the run page holds (DAG_LEGIBLE_LIMIT). dagLayout.test.ts checks
// every layout invariant on each, left to right and top to bottom, and the
// DAG gallery spec (e2e/dag-gallery.spec.ts) applies them as Orkflows and
// photographs them. Only ids and needs shape a layout; the other fields are
// what the drawing labels (a join, `each ≤ n`, a composed Orkflow).

export type StressJoin = 'any_failed' | 'all_done' | 'none_failed';

export interface StressTask {
	id: string;
	needs: string[];
	join?: StressJoin;
	pin?: string;
	each?: { over: string; maxItems: number };
	orkflow?: { name: string; version: string };
	await?: true;
}

const t = (id: string, needs: string[] = [], extra: Partial<StressTask> = {}): StressTask => ({ id, needs, ...extra });
const range = (n: number, f: (i: number) => StressTask): StressTask[] => Array.from({ length: n }, (_, i) => f(i));
const pad = (i: number) => String(i).padStart(2, '0');

/** A small seeded generator, so a random shape is the same shape every time. */
export function seeded(seed: number): () => number {
	let s = seed >>> 0;
	return () => {
		s = (s * 1664525 + 1013904223) >>> 0;
		return s / 2 ** 32;
	};
}

/** Stages of `width` tasks, each needing `fanIn` of the stage before (seeded). */
function staged(stages: number, width: number, fanIn: number, seed: number): StressTask[] {
	const rnd = seeded(seed);
	const out: StressTask[] = [];
	for (let s = 0; s < stages; s++) {
		for (let k = 0; k < width; k++) {
			const needs = new Set<string>();
			if (s > 0) while (needs.size < Math.min(fanIn, width)) needs.add(`s${s - 1}-${Math.floor(rnd() * width)}`);
			out.push(t(`s${s}-${k}`, [...needs]));
		}
	}
	return out;
}

/** A random DAG of `n` tasks, each needing some of the twelve before it, declared shuffled. */
function random(n: number, p: number, seed: number): StressTask[] {
	const rnd = seeded(seed);
	const ids = Array.from({ length: n }, (_, i) => `t${i}`);
	const out = ids.map((id, i) => {
		const needs: string[] = [];
		for (let j = Math.max(0, i - 12); j < i; j++) if (rnd() < p) needs.push(ids[j]);
		return t(id, needs);
	});
	for (let i = out.length - 1; i > 0; i--) {
		const j = Math.floor(rnd() * (i + 1));
		[out[i], out[j]] = [out[j], out[i]];
	}
	return out;
}

export const DAG_STRESS: Record<string, StressTask[]> = {
	// One task fans out to 50 parallel ones that fan back in.
	'parallel-1x50': [
		t('start'),
		...range(50, (i) => t(`p${pad(i + 1)}`, ['start'])),
		t('end', Array.from({ length: 50 }, (_, i) => `p${pad(i + 1)}`))
	],
	diamond: [t('top'), t('left', ['top']), t('right', ['top']), t('bottom', ['left', 'right'])],
	// Three diamonds in a row, and a diamond of diamonds.
	'diamond-chain': [
		t('a'),
		t('b1', ['a']),
		t('b2', ['a']),
		t('c', ['b1', 'b2']),
		t('d1', ['c']),
		t('d2', ['c']),
		t('e', ['d1', 'd2']),
		t('f1', ['e']),
		t('f2', ['e']),
		t('g', ['f1', 'f2'])
	],
	'nested-diamond': [
		t('top'),
		t('l', ['top']),
		t('r', ['top']),
		t('l1', ['l']),
		t('l2', ['l']),
		t('r1', ['r']),
		t('r2', ['r']),
		t('lj', ['l1', 'l2']),
		t('rj', ['r1', 'r2']),
		t('bottom', ['lj', 'rj'])
	],
	'chain-40': range(40, (i) => t(`step-${pad(i + 1)}`, i ? [`step-${pad(i)}`] : [])),
	// A saga with failure handlers and joins of every kind.
	'joins-and-handlers': [
		t('validate'),
		t('reserve', ['validate']),
		t('charge', ['reserve']),
		t('ship', ['charge']),
		t('notify', ['charge'], { join: 'none_failed' }),
		t('refund', ['charge'], { join: 'any_failed' }),
		t('release', ['reserve', 'charge'], { join: 'any_failed' }),
		t('report', ['ship', 'refund', 'release', 'notify'], { join: 'all_done' })
	],
	// Two pins of three members each, beside unpinned work.
	pins: [
		t('checkout', [], { pin: 'build' }),
		t('compile', ['checkout'], { pin: 'build' }),
		t('package', ['compile'], { pin: 'build' }),
		t('fetch', [], { pin: 'docs' }),
		t('render', ['fetch'], { pin: 'docs' }),
		t('bundle', ['render'], { pin: 'docs' }),
		t('publish', ['package', 'bundle'])
	],
	// A fan-out between a list and a merge, an await and a composed task.
	'each-compose-await': [
		t('list'),
		t('convert', ['list'], { each: { over: 'list', maxItems: 120 } }),
		t('merge', ['convert']),
		t('approve', ['merge'], { await: true }),
		t('release', ['approve'], { orkflow: { name: 'release-block', version: '2.1.0' } }),
		t('announce', ['release'])
	],
	// Declared so that declaration order crosses every edge: the layout must
	// not keep that order.
	'crossing-prone': [
		t('a1'),
		t('a2'),
		t('a3'),
		t('a4'),
		t('b1', ['a4']),
		t('b2', ['a3']),
		t('b3', ['a2']),
		t('b4', ['a1']),
		t('c1', ['b4']),
		t('c2', ['b3']),
		t('c3', ['b2']),
		t('c4', ['b1'])
	],
	// Long edges: every task also needs the first, so each skips layers.
	'skip-heavy': [t('root'), ...range(8, (i) => t(`k${i + 1}`, i ? [`k${i}`, 'root'] : ['root'])), t('sink', ['k8', 'root'])],
	// A complete bipartite stage (5 × 5) between two single tasks.
	k5x5: [
		t('in'),
		...range(5, (i) => t(`a${i + 1}`, ['in'])),
		...range(5, (i) => t(`b${i + 1}`, ['a1', 'a2', 'a3', 'a4', 'a5'])),
		t('out', ['b1', 'b2', 'b3', 'b4', 'b5'])
	],
	// Five stages of twenty, each task needing three of the stage before.
	'staged-5x20': staged(5, 20, 3, 7),
	// Near what the run page holds: drawn on request (81..300 tasks).
	'random-200': random(200, 0.12, 11),
	'staged-15x20': staged(15, 20, 2, 23)
};

/** The shapes that can be drawn without a single crossing, and are expected to be. */
export const PLANAR = ['parallel-1x50', 'diamond', 'diamond-chain', 'nested-diamond', 'chain-40', 'pins', 'each-compose-await', 'crossing-prone', 'skip-heavy'];
