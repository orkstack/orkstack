import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { ICONS } from '../src/lib/icons';

const routes = `${import.meta.dir}/../src/routes`;

test('the changed-run mark is round and neutral: nothing about it reads as the Failed triangle', () => {
	const tags = ICONS.updated.map(([tag]) => tag);
	expect(tags).toContain('circle');
	const runs = readFileSync(`${routes}/runs/+page.svelte`, 'utf8');
	expect(runs).not.toContain('name="delta"');
	expect(runs).toContain('<Icon name="updated" class="size-3.5 text-ink-2" />');
});

test('the Runs glyph is its own: not three lines like Menu and Events', () => {
	expect(JSON.stringify(ICONS.runs)).not.toBe(JSON.stringify(ICONS.menu));
	expect(ICONS.runs.length > 1).toBe(true);
	expect(JSON.stringify(ICONS.runs)).toContain('M2 13h12');
});

const lib = `${import.meta.dir}/../src/lib/components`;

// Every Delete: its trigger and its confirm, the same glyph (ui/DESIGN.md, The
// No Hue For Destruction Rule). Ban keeps one meaning, stopping a run.
const deletes = [
	`${lib}/OrkflowDelete.svelte`,
	`${lib}/OrkflowSchedules.svelte`,
	`${lib}/OrkflowTriggers.svelte`,
	`${routes}/admin/webhooks/+page.svelte`,
	`${routes}/admin/namespaces/+page.svelte`
];

test('the trash glyph is of the family: 1.4 stroke, round caps, currentColor, nothing filled', () => {
	const elements: readonly (readonly [string, Readonly<Record<string, string>>])[] = ICONS.trash;
	expect(elements.length > 0).toBe(true);
	for (const [, attrs] of elements) {
		expect(attrs['stroke']).toBe('currentColor');
		expect(attrs['stroke-width']).toBe('1.4');
		expect(attrs['stroke-linecap']).toBe('round');
		expect(attrs['fill']).toBe('none');
	}
});

for (const file of deletes) {
	test(`${file}: a Delete and its confirm carry the trash glyph, and no ban`, () => {
		const source = readFileSync(file, 'utf8');
		expect(/<Icon name="trash" \/>\s*Delete/.test(source)).toBe(true);
		expect(/title="Delete [^"]*"[\s\S]*?glyph="trash"/.test(source)).toBe(true);
		expect(source).not.toContain('name="ban"');
		expect(source).not.toContain('glyph="ban"');
	});
}

test('ban means stopping a run and nothing else', () => {
	const run = readFileSync(`${routes}/runs/[id]/RunView.svelte`, 'utf8');
	expect(run).toContain('<Icon name="ban" />');
	expect(run).toContain('glyph="ban"');
});
