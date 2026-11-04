import { expect, test } from 'bun:test';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';

// The data-testid contract, as a baseline. The Playwright specs reference
// hundreds of ids; a redesign that silently drops one breaks a spec far from
// the edit that caused it, and only when that spec runs. This test records
// every testid SHAPE the sources produce — a literal (`run-status`), a
// templated value with its expressions normalised to `{}` (`task-{}-status`),
// and every value handed to a component's testid prop, from which the
// component derives more (`{testid}-cli-trigger`) — and fails when a shape
// on the baseline is no longer produced.
//
// A removal is a UI change, never an accident: it is listed in
// ALLOWED_REMOVALS with its reason and the spec change that goes with it.
// `UPDATE_TESTID_BASELINE=1 bun test test/testidBaseline.test.ts` rewrites
// the fixture from the current sources (additions are always fine).

const src = `${import.meta.dir}/../src`;
const fixture = `${import.meta.dir}/fixtures/testid-baseline.txt`;

/** Shape → why it may be gone (and the spec change that goes with it). */
const ALLOWED_REMOVALS: Record<string, string> = {
	'prop activity-inspect-cli':
		"Inspect a reference's command joined the Activities header's popover, activities-cli: one per screen (admin.spec.ts).",
	'id task-{}-item-panel':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-artifacts':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-attempt':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-exit':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-label':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-live':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-orker':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-item-panel-status':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-live':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-live-orker':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'id task-{}-running-for':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'prop task-{}-item-panel-cache':
		'The Item card (ItemCard.svelte) derives this from its prefix prop, task-{id} or task-{id}-item-panel: the same id, a shape the baseline cannot see; run-page and each-fan-out specs read it after selecting the task.',
	'prop task-{}-item-panel-cli-artifacts':
		'One command-line popover per Item card, task-{}-item-panel-cli, holds what three buttons held.',
	'prop task-{}-item-panel-cli-attempts':
		'One command-line popover per Item card, task-{}-item-panel-cli, holds what three buttons held.',
	'prop task-{}-item-panel-cli-logs':
		'One command-line popover per Item card, task-{}-item-panel-cli, holds what three buttons held.',
	'id run-elapsed':
		'The Timing card went with the frozen design; run-lasted in the facts strip says how long the run took (run-operations, run-sheet-polish).',
	'id {}-retryable':
		"FailurePanel no longer prints the error's bare retryable flag: what happened to the retries is one phrase read off the failing task's tries ({testid}-retry-state), so the run's cloud and the task's panel say the same thing (finish review 2026-10-22, run page contradictions). No spec read it; no spec changes.",
	'id {}-filters-none':
		"ListFilter's zero-state sentence is gone: the filter toolbar is one row and says nothing while no filter is in force (finish review, list-filter chrome). No spec read it; no spec changes.",
	// Harbour (phase 2 of the UI revamp, 2026-11-01): the command line lives only
	// behind the terminal icon (brief §6.4), one popover per screen or task row.
	'id activity-build-command': 'Activities: the local build command is in the page\'s command-line popover (activities-cli), not printed on the page. admin.spec reads the popover.',
	'id activity-test-command': 'Activities: the contract-test command is in the page\'s command-line popover (activities-cli). admin.spec reads the popover.',
	'id activity-local-tools': 'Activities: the "Local activity tools" section is gone; its commands are in activities-cli.',
	'id activity-local-tool-copied': 'Activities: the popover says what was copied in its own status line (activities-cli-status).',
	'id orker-cordon-command': 'Pools: the cordon dialog no longer prints its orkctl command (brief §6.4); the Orker row\'s popover (orker-cli-{id}) lists it.',
	'prop run-children-cli': "Run page: listing a run's child runs is the header's \"Child runs\" link; the command is in the run's popover.",
	'prop run-stability-cli': 'Run page: the retry-history command is in the run popover, not beside the toggle.',
	'prop run-task-{}-inspect-cli': "Run page: a task row's commands are one popover, run-task-{id}-cli.",
	'prop run-task-{}-logs-cli': "Run page: a task row's commands are one popover, run-task-{id}-cli.",
	'prop run-task-{}-replay-cli': "Run page: a task row's commands are one popover, run-task-{id}-cli.",
	'prop run-task-{}-timing-cli': "Run page: a task row's commands are one popover, run-task-{id}-cli.",
	'prop task-{}-cache-cli': "Run page: a task row's commands are one popover, run-task-{id}-cli."
};

function sources(): string[] {
	return readdirSync(src, { recursive: true })
		.map((rel) => `${src}/${rel}`)
		.filter((p) => /\.(svelte|ts)$/.test(p) && !p.includes('/lib/gen/'));
}

// balanced returns the index just past the `}` closing the `{` at `open`.
function balanced(text: string, open: number): number {
	let depth = 0;
	for (let i = open; i < text.length; i++) {
		const c = text[i];
		if (c === '{') depth++;
		else if (c === '}') {
			depth--;
			if (depth === 0) return i + 1;
		} else if (c === "'" || c === '"' || c === '`') {
			i = skipString(text, i);
		}
	}
	return text.length;
}

// skipString returns the index of the closing quote of the string at `start`,
// stepping over `${…}` inside a template literal.
function skipString(text: string, start: number): number {
	const quote = text[start];
	for (let i = start + 1; i < text.length; i++) {
		const c = text[i];
		if (c === '\\') {
			i++;
			continue;
		}
		if (quote === '`' && c === '$' && text[i + 1] === '{') {
			i = balanced(text, i + 1) - 1;
			continue;
		}
		if (c === quote) return i;
	}
	return text.length;
}

// normalise replaces every `{…}` and `${…}` with `{}`.
export function normalise(value: string): string {
	let out = '';
	for (let i = 0; i < value.length; i++) {
		const c = value[i];
		if (c === '$' && value[i + 1] === '{') {
			out += '{}';
			i = balanced(value, i + 1) - 1;
		} else if (c === '{') {
			out += '{}';
			i = balanced(value, i) - 1;
		} else out += c;
	}
	return out;
}

// literals returns every string literal inside a JS expression, normalised.
function literals(expr: string): string[] {
	const found: string[] = [];
	for (let i = 0; i < expr.length; i++) {
		const c = expr[i];
		if (c === "'" || c === '"' || c === '`') {
			const end = skipString(expr, i);
			found.push(normalise(expr.slice(i + 1, end)));
			i = end;
		}
	}
	return found;
}

const NAME = /(data-testid|[A-Za-z]*[tT]est[iI]d)(=|\s*:\s*)/g;

export function shapes(text: string): string[] {
	const out: string[] = [];
	for (const match of text.matchAll(NAME)) {
		const name = match[1];
		const at = (match.index ?? 0) + match[0].length;
		const c = text[at];
		let values: string[] = [];
		if (match[2] === '=') {
			if (c === '"' || c === "'") values = [normalise(text.slice(at + 1, skipString(text, at)))];
			else if (c === '{') values = literals(text.slice(at + 1, balanced(text, at) - 1));
		} else if (c === '"' || c === "'" || c === '`') {
			values = [normalise(text.slice(at + 1, skipString(text, at)))];
		}
		const kind = name === 'data-testid' ? 'id' : 'prop';
		for (const v of values) if (v !== '' && /[a-z]/.test(v)) out.push(`${kind} ${v}`);
	}
	return out;
}

function current(): Set<string> {
	const all = new Set<string>();
	for (const file of sources()) for (const s of shapes(readFileSync(file, 'utf8'))) all.add(s);
	return all;
}

test('the extractor reads literal, templated and prop testids', () => {
	const sample = [
		'<div data-testid="run-status"></div>',
		'<div data-testid="task-{task.id}-status"></div>',
		'<b data-testid={retryTestid || `${testid}-retry`}></b>',
		"<X testid=\"runs\" retryTestid={'r-' + x} />",
		"const c = { testid: 'stat-total' };"
	].join('\n');
	expect(shapes(sample).sort()).toEqual(
		['id run-status', 'id task-{}-status', 'id {}-retry', 'prop runs', 'prop r-', 'prop stat-total'].sort()
	);
});

test('every testid shape on the baseline is still produced', () => {
	const now = current();
	const env = (globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env;
	if (env.UPDATE_TESTID_BASELINE === '1') {
		writeFileSync(fixture, [...now].sort().join('\n') + '\n');
	}
	const baseline = readFileSync(fixture, 'utf8').split('\n').filter(Boolean);
	expect(baseline.length > 700).toBe(true);
	// A literal that moved from an attribute into a component prop or an
	// object field (`testid: 'theme-dark'`, rendered as data-testid={c.testid})
	// is still produced.
	const produced = (shape: string) => now.has(shape) || now.has(shape.replace(/^id /, 'prop '));
	const missing = baseline.filter((shape) => !produced(shape) && !(shape in ALLOWED_REMOVALS));
	expect(missing).toEqual([]);
	// An allowance for a shape that is still produced (or never was) is stale.
	const stale = Object.keys(ALLOWED_REMOVALS).filter((shape) => produced(shape) || !baseline.includes(shape));
	expect(stale).toEqual([]);
});
