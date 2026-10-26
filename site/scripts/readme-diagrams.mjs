// The README's diagrams, drawn in the UI's drawing language from
// ui/src/tokens.css: docs/images/<diagram>-{light,dark}.svg.
//
// GitHub shows a README's images as plain <img>, so each theme is a file of
// its own with its colours written out (the README's <picture> picks one by
// prefers-color-scheme), and no web font loads: the faces are the tokens'
// stacks, whose fallbacks carry the text. Every value here comes from the
// tokens, the landing page's model graph (src/lib/model-graph.ts) or the
// example Orkflow a diagram shows; test/readme-diagrams.test.mjs holds the
// committed files to this script. `bun run diagrams` writes them.
import { mkdirSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';
import { readTokens, repoRoot, resolve } from './tokens.mjs';
import { across, H, W } from '../src/lib/model-graph.ts';

export const outDir = path.join(repoRoot, 'docs', 'images');
export const DIAGRAMS = ['architecture', 'each-fan-out-run', 'durable-commit'];

const EXAMPLE = path.join(repoRoot, 'examples', 'each-fan-out.ork.yaml');
// The caption band at the top of every drawing, as the UI's drawing frame
// carries one (site/src/components/shared/DrawingFrame.astro).
const BAND = 32;

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/** The theme's values, every one a token of ui/src/tokens.css. */
function look(tokens, theme) {
	const v = (name) => resolve(tokens, name, theme);
	return {
		paper: v('--color-paper'),
		ink: v('--color-ink'),
		ink2: v('--color-ink-secondary'),
		rule: v('--color-rule'),
		grid: v('--color-grid'),
		gridMajor: v('--color-grid-major'),
		red: v('--color-red'),
		sans: v('--font-sans'),
		mono: v('--font-mono'),
		region: parseFloat(v('--rule-region')),
		hair: parseFloat(v('--rule-hair')),
		minor: parseFloat(v('--grid-pitch-minor')),
		major: parseFloat(v('--grid-pitch-major')),
		semibold: v('--font-weight-semibold'),
		bold: v('--font-weight-bold'),
	};
}

function style(t) {
	return [
		`.paper{fill:${t.paper}}`,
		`.frame{fill:none;stroke:${t.ink};stroke-width:${t.region}px}`,
		`.band{fill:none;stroke:${t.rule};stroke-width:${t.hair}px}`,
		`.caption{fill:${t.ink2};font-family:${t.mono};font-size:12px}`,
		`.node{fill:${t.paper};stroke:${t.ink};stroke-width:${t.region}px}`,
		`.group{fill:none;stroke:${t.ink};stroke-width:${t.hair}px;stroke-dasharray:6 4}`,
		`.name{fill:${t.ink};font-family:${t.sans};font-size:15px;font-weight:${t.semibold}}`,
		`.note{fill:${t.ink2};font-family:${t.mono};font-size:11.5px}`,
		`.edge{fill:none;stroke:${t.ink};stroke-width:${t.region}px}`,
		`.back{fill:none;stroke:${t.ink2};stroke-width:${t.region}px;stroke-dasharray:6 4}`,
		`.arrow{fill:${t.ink}}`,
		`.arrow-back{fill:${t.ink2}}`,
		`.label{fill:${t.ink2};font-family:${t.mono};font-size:11px;paint-order:stroke;stroke:${t.paper};stroke-width:4px}`,
		`.label-fail{fill:${t.red};font-family:${t.mono};font-size:11px;font-weight:${t.bold};paint-order:stroke;stroke:${t.paper};stroke-width:4px}`,
		`.fail{fill:none;stroke:${t.red};stroke-width:${t.region * 1.5}px}`,
		`.brace{fill:none;stroke:${t.ink2};stroke-width:${t.hair}px}`,
		`.balloon circle{fill:${t.paper};stroke:${t.ink};stroke-width:${t.region}px}`,
		`.balloon text{fill:${t.ink};font-family:${t.sans};font-size:13px;font-weight:${t.bold}}`,
	].join('');
}

/** A drawing: paper, the drafting grid below the caption band, a 1.5 px ink frame. */
function sheet(t, { id, w, h, caption, title }, body) {
	const patch = Math.round(caption.length * 7.2 + 24);
	return (
		[
			`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-labelledby="${id}-title">`,
			`<title id="${id}-title">${esc(title)}</title>`,
			`<style>${style(t)}</style>`,
			'<defs>',
			`<pattern id="minor" width="${t.minor}" height="${t.minor}" patternUnits="userSpaceOnUse"><path d="M${t.minor} 0H0V${t.minor}" fill="none" stroke="${t.grid}" stroke-width="${t.hair}"/></pattern>`,
			`<pattern id="major" width="${t.major}" height="${t.major}" patternUnits="userSpaceOnUse"><rect width="${t.major}" height="${t.major}" fill="url(#minor)"/><path d="M${t.major} 0H0V${t.major}" fill="none" stroke="${t.gridMajor}" stroke-width="${t.hair}"/></pattern>`,
			'<marker id="arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto"><path d="M0 0L8 4L0 8z" class="arrow"/></marker>',
			'<marker id="arrow-back" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto"><path d="M0 0L8 4L0 8z" class="arrow-back"/></marker>',
			'</defs>',
			`<rect class="paper" width="${w}" height="${h}"/>`,
			`<rect y="${BAND}" width="${w}" height="${h - BAND}" fill="url(#major)"/>`,
			`<path class="band" d="M0 ${BAND + 0.5}H${w}M${patch + 0.5} 0V${BAND}"/>`,
			`<text class="caption" x="12" y="21">${esc(caption)}</text>`,
			body,
			`<rect class="frame" x="${t.region / 2}" y="${t.region / 2}" width="${w - t.region}" height="${h - t.region}"/>`,
			'</svg>',
		].join('\n') + '\n'
	);
}

function node(x, y, name, note, attrs = '', w = W, h = H) {
	return [
		`<g transform="translate(${x} ${y})"${attrs}>`,
		`<rect class="node" width="${w}" height="${h}"/>`,
		`<text class="name" x="14" y="${h / 2 - 4}">${esc(name)}</text>`,
		`<text class="note" x="14" y="${h / 2 + 15}">${esc(note)}</text>`,
		'</g>',
	].join('');
}

function label(x, y, text, anchor = 'middle', cls = 'label') {
	return `<text class="${cls}" x="${x}" y="${y}" text-anchor="${anchor}">${esc(text)}</text>`;
}

function balloon(x, y, n) {
	return `<g class="balloon" transform="translate(${x} ${y})"><circle r="12"/><text y="4.5" text-anchor="middle">${n}</text></g>`;
}

// 1. The architecture: the landing page's model graph, as it is drawn across.
function architecture(t) {
	const pad = 24;
	const body = [`<g transform="translate(0 ${BAND + pad})">`];
	for (const e of across.edges) {
		body.push(`<path class="edge" d="${e.d}" marker-end="url(#arrow)"/>`);
		if (e.label) body.push(label(e.lx, e.ly, e.label, e.anchor ?? 'middle'));
	}
	for (const n of across.nodes) {
		body.push(node(n.x, n.y, n.name, n.note));
		if (n.balloon) body.push(balloon(n.x + W, n.y, n.balloon));
	}
	body.push('</g>');
	return sheet(
		t,
		{
			id: 'architecture',
			// The landing page's drawing has its frame's padding around it; a
			// sheet adds its own on the right, where the longest label ends.
			w: across.w + 24,
			h: BAND + pad + across.h,
			caption: 'How a run flows · ARCHITECTURE.md',
			title:
				'An Orkflow is applied through orkctl, the UI or the API to the ork engine; triggers and callbacks also reach it over plain HTTP. The engine starts each run on Temporal, which dispatches tasks to Orkers. An Orker runs the activity’s OCI image, passing files in and out, and writes artifacts and records to object storage.',
		},
		body.join('\n'),
	);
}

// 2. The each-fan-out example as a run: its tasks, its needs, one item per
// element of the list, as the run page draws a fan-out.
function fanOutRun(t, example) {
	const byId = new Map(example.tasks.map((task) => [task.id, task]));
	const fan = example.tasks.find((task) => task.each);
	const source = byId.get(fan.needs[0]);
	const sink = example.tasks.find((task) => (task.needs ?? []).includes(fan.id));
	const parts = source.with.vars.parts;
	const itemH = 44;
	const gap = 12;
	const groupTop = BAND + 40;
	const groupH = 30 + parts.length * itemH + (parts.length - 1) * gap + 18;
	const mid = groupTop + groupH / 2;
	const w = 960;
	const h = groupTop + groupH + 40;
	const gx = 330;
	const gw = 300;
	const body = [
		`<g data-task="${esc(source.id)}">${node(40, mid - H / 2, source.id, `uses: ${source.uses}`)}</g>`,
		`<g data-edge="${esc(source.id)}->${esc(fan.id)}"><path class="edge" d="M230 ${mid}H${gx}" marker-end="url(#arrow)"/>${label(280, mid - 8, `${parts.length} parts`)}</g>`,
		`<g data-task="${esc(fan.id)}">`,
		`<rect class="group" x="${gx}" y="${groupTop}" width="${gw}" height="${groupH}"/>`,
		label(gx + 12, groupTop + 20, `each: one item per part, ${fan.each.max_parallel} at a time`, 'start'),
	];
	parts.forEach((_, i) => {
		const y = groupTop + 30 + i * (itemH + gap);
		body.push(node(gx + 20, y, `${fan.id}[${i}]`, `uses: ${fan.uses}`, ` data-item="${i}"`, gw - 40, itemH));
	});
	body.push('</g>');
	body.push(
		`<g data-edge="${esc(fan.id)}->${esc(sink.id)}"><path class="edge" d="M${gx + gw} ${mid}H730" marker-end="url(#arrow)"/>${label(680, mid - 8, `${parts.length} × part.txt`)}</g>`,
		`<g data-task="${esc(sink.id)}">${node(730, mid - H / 2, sink.id, `uses: ${sink.uses}`)}</g>`,
	);
	return sheet(
		t,
		{
			id: 'each-fan-out-run',
			w,
			h,
			caption: `${example.name} @ ${example.version} · ${example.tasks.length} tasks`,
			title: `The ${example.name} example as a run: ${source.id} computes ${parts.length} parts, ${fan.id} runs one item per part, ${fan.each.max_parallel} at a time, and ${sink.id} reads every item’s part.txt once all of them have ended.`,
		},
		body.join('\n'),
	);
}

// 3. How a task's result becomes durable: the commit order of AGENTS.md §4.1
// and docs/OCI_ACTIVITY_CONTRACT.md, and what a retry finds.
const STEPS = [
	{ name: 'Validate', note: 'values.json' },
	{ name: 'Hash', note: 'artifact hashes' },
	{ name: 'Upload', note: 'artifacts' },
	{ name: 'Upload', note: 'values.json' },
	{ name: 'Write', note: 'commit.json' },
	{ name: 'Report', note: 'success to Temporal' },
];

function durableCommit(t) {
	const sw = 160;
	const gap = 32;
	const x0 = 130;
	const y = BAND + 56;
	const w = x0 + STEPS.length * sw + (STEPS.length - 1) * gap + 30;
	const h = y + H + 128;
	const xAt = (i) => x0 + i * (sw + gap);
	const body = [label(14, y + H / 2 - 6, 'container', 'start'), label(14, y + H / 2 + 8, 'exits 0', 'start')];
	body.push(`<path class="edge" d="M80 ${y + H / 2}H${x0}" marker-end="url(#arrow)"/>`);
	STEPS.forEach((s, i) => {
		const x = xAt(i);
		body.push(`<g data-step="${i + 1}">${node(x, y, s.name, s.note, '', sw)}${balloon(x + sw, y, i + 1)}</g>`);
		if (i < STEPS.length - 1) body.push(`<path class="edge" d="M${x + sw} ${y + H / 2}H${xAt(i + 1)}" marker-end="url(#arrow)"/>`);
	});
	// Steps 3 to 5 write to object storage, each object once; step 6 is
	// Temporal's.
	const bx = xAt(2);
	const bw = xAt(4) + sw - bx;
	const by = y + H + 14;
	body.push(`<path class="brace" d="M${bx} ${by}V${by + 8}H${bx + bw}V${by}"/>`);
	body.push(label(bx + bw / 2, by + 24, 'object storage, each object written once', 'middle'));
	// The Orker lost between the commit and the report: Temporal retries, and
	// the retry adopts the commit instead of running the container again.
	const cx = xAt(5) - gap / 2;
	body.push(`<path class="fail" d="M${cx - 6} ${y + H / 2 - 18}l12 12m0 -12l-12 12"/>`);
	body.push(label(cx, y - 28, 'Orker lost here?', 'middle', 'label-fail'));
	const ry = by + 56;
	body.push(
		`<path class="back" d="M${xAt(5) + sw / 2} ${y + H}V${ry}H${xAt(4) + sw / 2}V${y + H + 4}" marker-end="url(#arrow-back)"/>`,
	);
	const nx = xAt(4) + sw / 2 - 14;
	body.push(label(nx, ry - 4, 'Temporal retries the task: the retry finds commit.json', 'end'));
	body.push(label(nx, ry + 12, 'and returns its result; the container does not run again', 'end'));
	return sheet(
		t,
		{
			id: 'durable-commit',
			w,
			h,
			caption: 'A task’s result becomes durable · AGENTS.md §4.1',
			title:
				'When a container exits 0, the Orker validates values.json, hashes every artifact, uploads the artifacts and values.json, writes an immutable commit.json, and only then reports success to Temporal. If the Orker is lost before that report, Temporal retries the task, and the retry finds commit.json and returns its result without running the container again.',
		},
		body.join('\n'),
	);
}

/** Every diagram in both themes: file name → SVG text. */
export function renderDiagrams(tokens = readTokens(), example = parse(readFileSync(EXAMPLE, 'utf8'))) {
	const out = new Map();
	for (const theme of ['light', 'dark']) {
		const t = look(tokens, theme);
		out.set(`architecture-${theme}.svg`, architecture(t));
		out.set(`each-fan-out-run-${theme}.svg`, fanOutRun(t, example));
		out.set(`durable-commit-${theme}.svg`, durableCommit(t));
	}
	return out;
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
	mkdirSync(outDir, { recursive: true });
	for (const [name, svg] of renderDiagrams()) {
		writeFileSync(path.join(outDir, name), svg);
		console.log(`docs/images/${name}`);
	}
}
