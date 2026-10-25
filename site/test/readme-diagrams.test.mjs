// The README's diagrams: docs/images/*.svg, drawn by scripts/readme-diagrams.mjs
// from ui/src/tokens.css, the landing page's model graph and the example
// Orkflow they show, one file per theme.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { parse } from 'yaml';
import { readTokens, resolve, repoRoot } from '../scripts/tokens.mjs';
import { DIAGRAMS, outDir, renderDiagrams } from '../scripts/readme-diagrams.mjs';
import { across } from '../src/lib/model-graph.ts';

const example = parse(readFileSync(path.join(repoRoot, 'examples', 'each-fan-out.ork.yaml'), 'utf8'));

test('every diagram is committed as the generator draws it, in both themes', () => {
	const want = renderDiagrams();
	assert.deepEqual(
		[...want.keys()].sort(),
		DIAGRAMS.flatMap((d) => [`${d}-dark.svg`, `${d}-light.svg`]).sort(),
	);
	for (const [name, svg] of want) {
		const file = path.join(outDir, name);
		assert.ok(existsSync(file), `docs/images/${name} is missing: run bun run diagrams in site/`);
		assert.equal(readFileSync(file, 'utf8'), svg, `docs/images/${name} is stale: run bun run diagrams in site/`);
	}
	const orphans = readdirSync(outDir).filter((f) => f.endsWith('.svg') && !want.has(f));
	assert.deepEqual(orphans, [], 'docs/images holds an SVG the generator no longer draws');
});

test('every colour in a diagram is a token value of its own theme', () => {
	const tokens = readTokens();
	for (const [name, svg] of renderDiagrams(tokens)) {
		const theme = name.endsWith('-dark.svg') ? 'dark' : 'light';
		const allowed = new Set(
			[...tokens.names].filter((n) => n.startsWith('--color-')).map((n) => resolve(tokens, n, theme)),
		);
		const found = svg.match(/#[0-9a-fA-F]{3,8}\b|rgba?\([^)]*\)/g) ?? [];
		assert.ok(found.length > 0, `${name} names no colour`);
		for (const c of found) assert.ok(allowed.has(c), `${name}: ${c} is not a ${theme} colour token`);
	}
});

test('the run diagram draws the example Orkflow: its tasks, its needs and one item per element', () => {
	const svg = renderDiagrams().get('each-fan-out-run-light.svg');
	const tasks = [...svg.matchAll(/data-task="([^"]+)"/g)].map((m) => m[1]);
	assert.deepEqual(tasks.sort(), example.tasks.map((t) => t.id).sort());
	const edges = [...svg.matchAll(/data-edge="([^"]+)"/g)].map((m) => m[1]);
	const needs = example.tasks.flatMap((t) => (t.needs ?? []).map((n) => `${n}->${t.id}`));
	assert.deepEqual(edges.sort(), needs.sort());
	const fan = example.tasks.find((t) => t.each);
	const list = example.tasks.find((t) => t.id === 'list');
	const items = [...svg.matchAll(/data-item="(\d+)"/g)].map((m) => Number(m[1]));
	assert.deepEqual(items, list.with.vars.parts.map((_, i) => i), `one ${fan.id} item per element of the list`);
	assert.match(svg, new RegExp(`${fan.each.max_parallel} at a time`));
});

test('the architecture diagram is the landing page model, every part named', () => {
	const svg = renderDiagrams().get('architecture-light.svg');
	for (const n of across.nodes) {
		assert.ok(svg.includes(`>${n.name.replace(/&/g, '&amp;')}<`), `architecture lacks ${n.name}`);
		assert.ok(svg.includes(`>${n.note}<`), `architecture lacks ${n.note}`);
	}
});

test('the commit diagram follows the durable commit order of AGENTS.md §4.1, step by step', () => {
	const svg = renderDiagrams().get('durable-commit-light.svg');
	const steps = [...svg.matchAll(/<g data-step="(\d+)">.*?class="name"[^>]*>([^<]+)<\/text>.*?class="note"[^>]*>([^<]+)<\/text>/g)].map(
		(m) => ({ n: Number(m[1]), name: m[2], note: m[3] }),
	);
	assert.deepEqual(
		steps.map((s) => s.n),
		[1, 2, 3, 4, 5, 6],
	);
	// AGENTS.md §4.1's own numbered list, which the diagram draws.
	const agents = readFileSync(path.join(repoRoot, 'AGENTS.md'), 'utf8');
	const section = agents.slice(agents.indexOf('### 4.1 Durable output commit'));
	const order = [...section.matchAll(/^(\d)\. (.+)$/gm)].slice(0, 6).map((m) => m[2].replace(/`/g, '').toLowerCase());
	assert.equal(order.length, 6, 'AGENTS.md §4.1 lists six steps');
	steps.forEach((s, i) => {
		for (const word of s.note.toLowerCase().split(/\s+/).filter((w) => w.length > 2)) {
			assert.ok(order[i].includes(word), `step ${s.n} (${s.name} ${s.note}) is not AGENTS.md §4.1's step ${i + 1}: ${order[i]}`);
		}
	});
});

test('every diagram is labelled for a screen reader, uses the token faces and stays small', () => {
	const tokens = readTokens();
	const sans = resolve(tokens, '--font-sans', 'light');
	for (const [name, svg] of renderDiagrams(tokens)) {
		assert.match(svg, /^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg"[^>]* role="img" aria-labelledby="[^"]+"/, name);
		assert.match(svg, /<title id="[^"]+">[^<]{40,}<\/title>/, `${name} has no useful title`);
		assert.ok(svg.includes(sans), `${name} does not set the token sans face`);
		assert.ok(Buffer.byteLength(svg) < 64 * 1024, `${name} is over 64 KiB`);
	}
});
