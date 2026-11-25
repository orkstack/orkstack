import { expect, test } from 'bun:test';
import { listTree, readSource, scanTest } from './sourceTree';

// The Shell Icon Rule's source half (ui/DESIGN.md). e2e/cli-icon-rule.spec.ts
// holds every route to one page-level icon in a browser, in the states it
// visits; these hold every state, by reading the sources:
//
//   - the terminal glyph is drawn only by the two CLI components, so the
//     browser check, which finds icons by their marker, cannot miss one;
//   - ListFilter draws no icon, because a list's filters fold into the page's;
//   - no template prints `orkctl` as text, on any screen.

const src = `${import.meta.dir}/../src`;
const POPOVER = 'lib/components/CliPopover.svelte';
const EQUIVALENT = 'lib/components/CliEquivalent.svelte';

function svelteFiles(): string[] {
	return listTree(src).filter((rel) => rel.endsWith('.svelte'));
}

/** A component's markup: no scripts, no styles, no comments. */
function markup(rel: string): string {
	return readSource(`${src}/${rel}`)
		.replace(/<script[^>]*>[\s\S]*?<\/script>/g, '')
		.replace(/<style[^>]*>[\s\S]*?<\/style>/g, '')
		.replace(/<!--[\s\S]*?-->/g, '');
}

scanTest('only the two CLI components draw the terminal glyph', () => {
	const drawing = svelteFiles().filter((rel) => /name="term"/.test(markup(rel)));
	expect(drawing.sort()).toEqual([EQUIVALENT, POPOVER]);
});

test('ListFilter draws no CLI icon', () => {
	const source = readSource(`${src}/lib/components/ListFilter.svelte`);
	expect(source.includes('CliEquivalent')).toBe(false);
	expect(source.includes('CliPopover')).toBe(false);
});

scanTest('no label a person reads names orkctl', () => {
	// A palette entry or a popover row is named by its `label`; a command lives
	// in `command` or `text`, behind the icon that copies it.
	const named = /\blabel:\s*(['"`])(?:(?!\1).)*\borkctl\b/;
	expect(named.test(`label: 'Copy orkctl run get -j'`)).toBe(true);
	expect(named.test(`label: 'The run, as JSON', command: 'orkctl run get'`)).toBe(false);
	const sources = listTree(src).filter((rel) => /\.(svelte|ts)$/.test(rel) && !rel.startsWith('lib/gen/'));
	expect(sources.filter((rel) => named.test(readSource(`${src}/${rel}`)))).toEqual([]);
});

scanTest('no template prints orkctl as text', () => {
	// Text is what stands between a closing `>` and the next tag or expression;
	// a command in an attribute or a snippet's expression is behind an icon.
	// CliPopover's own sentence is inside the popover.
	const printing = svelteFiles()
		.filter((rel) => rel !== POPOVER)
		.filter((rel) => />[^<>{}]*\borkctl\b/.test(markup(rel)));
	expect(printing).toEqual([]);
});
