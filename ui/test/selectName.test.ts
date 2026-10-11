import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { selectAria } from '../src/lib/selectName';

// A Select whose trigger shows only its value is announced as "default,
// combobox": these hold that each control the review found unnamed now
// carries a name, and that a <label for=id> keeps winning where one exists.

test('a label names the trigger and the listbox when no <label for> does', () => {
	expect(selectAria(undefined, 'Namespace')).toEqual({
		trigger: { 'aria-label': 'Namespace' },
		listbox: { 'aria-label': 'Namespace' }
	});
});

test('an id defers to the sibling <label for=id>, label or not', () => {
	expect(selectAria('run-filter-size', undefined)).toEqual({
		trigger: {},
		listbox: { 'aria-labelledby': 'run-filter-size' }
	});
	expect(selectAria('run-filter-size', 'Size').trigger).toEqual({});
});

test('neither leaves both unnamed, never an empty aria-label', () => {
	expect(selectAria(undefined, undefined)).toEqual({ trigger: {}, listbox: {} });
	expect(selectAria(undefined, '')).toEqual({ trigger: {}, listbox: {} });
});

// The sites: each Select the review named passes its name.
const src = `${import.meta.dir}/../src`;
function selectTag(file: string, testid: string): string {
	const text = readFileSync(`${src}/${file}`, 'utf8');
	const tags = text.match(/<Select\b[\s\S]*?\/>/g) ?? [];
	const found = tags.filter((tag) => tag.includes(`testid=${testid}`) || tag.includes(`testid="${testid}"`));
	if (found.length === 0) throw new Error(`${file}: no Select with testid ${testid}`);
	return found.join('\n');
}

test('the namespace picker is named Namespace', () => {
	expect(selectTag('routes/+layout.svelte', 'namespace-picker')).toContain('label="Namespace"');
});

test('the cache purge orkflow select is named Orkflow', () => {
	expect(selectTag('routes/admin/cache/+page.svelte', 'cache-purge-orkflow-select')).toContain('label="Orkflow"');
});

test("ListFilter's enum and bool value selects are named after their field", () => {
	const tags = selectTag('lib/components/ListFilter.svelte', '{testid}-filter-value-{i}').split('\n');
	expect(tags).toHaveLength(2);
	for (const tag of tags) expect(tag).toContain('label="{field.label} value"');
});
