// The fixture DAGs dag.test.ts pins layoutDag on. Each is a list of
// [id, needs]; test/fixtures/dag-layouts.json holds what layoutDag drew for
// each BEFORE it was re-based on layerOrder, so the re-base is proven not to
// move a node, an edge or the drawing's size. Its x coordinates and widths
// were later re-derived, layer by layer, for the tighter LAYER_GAP (56, was
// 110); no layer, row, y or height moved.
export const DAG_FIXTURES: Record<string, [string, string[]][]> = {
	single: [['only', []]],
	chain: [
		['a', []],
		['b', ['a']],
		['c', ['b']],
		['d', ['c']]
	],
	diamond: [
		['top', []],
		['left', ['top']],
		['right', ['top']],
		['bottom', ['left', 'right']]
	],
	'wide-fan-in': [
		['s1', []],
		['s2', []],
		['s3', []],
		['s4', []],
		['s5', []],
		['join', ['s1', 's2', 's3', 's4', 's5']]
	],
	'unknown-needs': [
		['a', ['ghost']],
		['b', ['a', 'phantom']],
		['c', []]
	],
	// Declaration order decides the row within a layer, and a task declared
	// before its need still lands after it.
	'declaration-ties': [
		['late', ['early']],
		['z', []],
		['early', []],
		['m', ['z']],
		['tail', ['late', 'm']],
		['also', ['z']]
	],
	'order-saga': [
		['validate', []],
		['reserve-stock', ['validate']],
		['charge', ['reserve-stock']],
		['ship', ['charge']],
		['refund', ['charge']],
		['report', ['ship', 'refund']]
	],
	'skip-layers': [
		['a', []],
		['b', ['a']],
		['c', ['b']],
		['d', ['a', 'c']],
		['e', ['a']]
	]
};
