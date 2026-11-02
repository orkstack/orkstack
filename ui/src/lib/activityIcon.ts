// An activity's icon. A first-party activity, a composed task and an awaited
// task have the UI's own drawings (#lib/icons.ts, a-*); an image may declare
// its own (`org.orkstack.activity.icon`), which the engine reads, checks and
// returns as a tree of elements and attributes — never markup. The UI renders
// that tree through a closed switch over the names it allows (the same list
// the engine enforces, specs/ui-revamp-icons.md), and draws the default icon
// for anything else: an unknown element or attribute shows the default icon,
// never a partial one.
import type { ActivityKind } from './icons';

/** One attribute of a declared icon, as the API returns it. */
export interface IconAttr {
	name: string;
	value: string;
}

/** One element of a declared icon; children only under `g`. */
export interface IconNode {
	name: string;
	attributes: readonly IconAttr[];
	children: readonly IconNode[];
}

/** A declared icon: the root's presentation attributes and its elements. */
export interface DeclaredIcon {
	attributes: readonly IconAttr[];
	elements: readonly IconNode[];
}

export type IconTagName = 'path' | 'circle' | 'ellipse' | 'rect' | 'line' | 'polyline' | 'polygon' | 'g';

const PRESENTATION = new Set(['fill', 'stroke', 'stroke-width', 'stroke-linecap', 'stroke-linejoin', 'fill-rule', 'clip-rule']);
const GEOMETRY: Record<IconTagName, ReadonlySet<string>> = {
	path: new Set(['d']),
	circle: new Set(['cx', 'cy', 'r']),
	ellipse: new Set(['cx', 'cy', 'rx', 'ry']),
	rect: new Set(['x', 'y', 'width', 'height', 'rx', 'ry']),
	line: new Set(['x1', 'y1', 'x2', 'y2']),
	polyline: new Set(['points']),
	polygon: new Set(['points']),
	g: new Set(['transform'])
};
const PAINT = new Set(['none', 'currentColor']);

/** A checked element, ready to render: its tag and its attributes. */
export interface SafeNode {
	tag: IconTagName;
	attrs: Record<string, string>;
	children: SafeNode[];
}

function safeAttrs(tag: IconTagName, attrs: readonly IconAttr[]): Record<string, string> | null {
	const out: Record<string, string> = {};
	for (const { name, value } of attrs) {
		if (!PRESENTATION.has(name) && !GEOMETRY[tag].has(name)) return null;
		if ((name === 'fill' || name === 'stroke') && !PAINT.has(value)) return null;
		if (Object.hasOwn(out, name)) return null;
		out[name] = value;
	}
	return out;
}

function safeNode(node: IconNode, depth: number, budget: { left: number }): SafeNode | null {
	if (--budget.left < 0) return null;
	if (!Object.hasOwn(GEOMETRY, node.name)) return null;
	const tag = node.name as IconTagName;
	if (tag !== 'g' && node.children.length > 0) return null;
	if (tag === 'g' && depth >= 4) return null;
	const attrs = safeAttrs(tag, node.attributes);
	if (!attrs) return null;
	const children: SafeNode[] = [];
	for (const child of node.children) {
		const c = safeNode(child, depth + 1, budget);
		if (!c) return null;
		children.push(c);
	}
	return { tag, attrs, children };
}

/**
 * The declared icon as safe nodes, or null when anything in it is outside
 * the allowed set (the default icon is drawn instead, never a partial one).
 */
export function safeIcon(icon: DeclaredIcon | undefined): { attrs: Record<string, string>; nodes: SafeNode[] } | null {
	if (!icon || icon.elements.length === 0) return null;
	const root: Record<string, string> = {};
	for (const { name, value } of icon.attributes) {
		if (!PRESENTATION.has(name)) return null;
		if ((name === 'fill' || name === 'stroke') && !PAINT.has(value)) return null;
		root[name] = value;
	}
	const budget = { left: 32 };
	const nodes: SafeNode[] = [];
	for (const el of icon.elements) {
		const n = safeNode(el, 0, budget);
		if (!n) return null;
		nodes.push(n);
	}
	return { attrs: root, nodes };
}

const FIRST_PARTY: Record<string, ActivityKind> = {
	http: 'http',
	shell: 'shell',
	file: 'file',
	json: 'json',
	git: 'git',
	template: 'template'
};

/**
 * The drawing the UI owns for a task's `uses:` — the first-party activities
 * by their catalog name, a composed task, an awaited task — and the default
 * for any other image.
 */
export function activityKind(uses: string | undefined, opts: { awaited?: boolean } = {}): ActivityKind {
	if (opts.awaited) return 'await';
	if (!uses) return 'default';
	if (uses.startsWith('orkflow:')) return 'orkflow';
	const name = uses.split(/[@:]/)[0];
	return FIRST_PARTY[name] ?? 'default';
}

/** What the icon stands for, in words (a tooltip; the icon itself is decorative). */
export function activityKindWords(kind: ActivityKind): string {
	switch (kind) {
		case 'orkflow':
			return 'Composed task';
		case 'await':
			return 'Awaited task';
		case 'default':
			return 'Activity image';
		default:
			return `${kind} activity`;
	}
}
