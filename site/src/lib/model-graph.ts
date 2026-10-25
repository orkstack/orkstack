// The landing page's model graph, which the README's architecture diagram
// (scripts/readme-diagrams.mjs) draws too: one graph, two drawings of it.
// Plain data with erasable types only, so node imports it as it is.

export type Node = { id: string; x: number; y: number; name: string; note: string; balloon?: number };
export type Edge = { d: string; label?: string; lx?: number; ly?: number; anchor?: 'start' | 'middle' | 'end' };
export const W = 190;
export const H = 58;

// Across: four columns, three rows.
export const across: { w: number; h: number; nodes: Node[]; edges: Edge[] } = {
	w: 1160,
	h: 440,
	nodes: [
		{ id: 'orkflow', x: 40, y: 48, name: 'Orkflow', note: 'a YAML document', balloon: 1 },
		{ id: 'clients', x: 330, y: 48, name: 'orkctl · UI · API', note: 'one Connect contract' },
		{ id: 'ork', x: 620, y: 48, name: 'ork', note: 'engine and API' },
		{ id: 'http', x: 910, y: 48, name: 'Triggers, callbacks', note: 'plain HTTP' },
		{ id: 'temporal', x: 620, y: 200, name: 'Temporal', note: 'durable execution' },
		{ id: 'orker', x: 910, y: 200, name: 'Orker', note: 'a generic worker', balloon: 3 },
		{ id: 'storage', x: 620, y: 352, name: 'Object storage', note: 'S3-compatible' },
		{ id: 'activity', x: 910, y: 352, name: 'Activity', note: 'any OCI image', balloon: 2 },
	],
	edges: [
		{ d: 'M230 77H330', label: 'apply', lx: 280, ly: 69 },
		{ d: 'M520 77H620', label: 'submit', lx: 570, ly: 69 },
		{ d: 'M910 77H810', label: 'start', lx: 860, ly: 69 },
		{ d: 'M715 106V200', label: 'start a run', lx: 724, ly: 158, anchor: 'start' },
		{ d: 'M810 229H910', label: 'dispatch', lx: 860, ly: 221 },
		{ d: 'M1005 258V352', label: 'run, files in and out', lx: 1014, ly: 310, anchor: 'start' },
		{ d: 'M910 244H866V381H810', label: 'artifacts, records', lx: 858, ly: 312, anchor: 'end' },
	],
};

// Down: two columns, for a narrow screen.
export const down: typeof across = {
	w: 470,
	h: 690,
	nodes: [
		{ id: 'orkflow', x: 20, y: 24, name: 'Orkflow', note: 'a YAML document', balloon: 1 },
		{ id: 'clients', x: 20, y: 152, name: 'orkctl · UI · API', note: 'one Connect contract' },
		{ id: 'ork', x: 20, y: 280, name: 'ork', note: 'engine and API' },
		{ id: 'http', x: 260, y: 280, name: 'Triggers, callbacks', note: 'plain HTTP' },
		{ id: 'temporal', x: 20, y: 408, name: 'Temporal', note: 'durable execution' },
		{ id: 'orker', x: 260, y: 408, name: 'Orker', note: 'a generic worker', balloon: 3 },
		{ id: 'storage', x: 20, y: 590, name: 'Object storage', note: 'S3-compatible' },
		{ id: 'activity', x: 260, y: 590, name: 'Activity', note: 'any OCI image', balloon: 2 },
	],
	edges: [
		{ d: 'M115 82V152', label: 'apply', lx: 124, ly: 122, anchor: 'start' },
		{ d: 'M115 210V280', label: 'submit', lx: 124, ly: 250, anchor: 'start' },
		{ d: 'M260 309H210', label: '', lx: 0, ly: 0 },
		{ d: 'M115 338V408', label: 'start a run', lx: 124, ly: 378, anchor: 'start' },
		{ d: 'M210 437H260', label: '', lx: 0, ly: 0 },
		{ d: 'M355 466V590', label: 'run, files', lx: 364, ly: 530, anchor: 'start' },
		{ d: 'M300 466V530H115V590', label: 'artifacts, records', lx: 124, ly: 522, anchor: 'start' },
	],
};
