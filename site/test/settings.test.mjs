// The configuration pages' generator: each test names what a reader would see
// go wrong if it broke. It runs on the repository's own table as well as on
// planted rows, so a table the parser can no longer read fails here.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import { repoRoot } from '../scripts/tokens.mjs';
import {
	AREAS,
	acceptsFor,
	acceptsProblems,
	bytesDefault,
	checkedWhen,
	headingAnchors,
	inlineHtml,
	leadAndRest,
	linkMentions,
	parseSettings,
	renderEntry,
	schemaKinds,
	sentences,
	settingArea,
	settingId,
	splitRow,
} from '../scripts/settings.mjs';
import { matchesFilter } from '../src/lib/settings-filter.ts';

const deploy = readFileSync(path.join(repoRoot, 'docs/DEPLOY.md'), 'utf8');
const configGo = readFileSync(path.join(repoRoot, 'internal/config/config.go'), 'utf8');
const rows = parseSettings(deploy);

test('every setting of the code has an entry: a key the Schema holds is never left off the page', () => {
	const kinds = schemaKinds(configGo);
	const documented = new Set(rows.flatMap((r) => r.keys));
	const compound = rows.flatMap((r) => r.keys).filter((k) => k.includes('{'));
	const missing = [...kinds.keys()].filter((key) => !documented.has(key) && !key.startsWith('telemetry.otlp'));
	assert.deepEqual(missing, []);
	assert.ok(compound.length > 0, 'the OTLP keys are documented as one compound row');
});

test('every entry has its own anchor: a link to one setting lands on that setting', () => {
	const ids = rows.map(settingId);
	assert.ok(ids.every(Boolean), 'an entry with no id cannot be linked');
	assert.equal(new Set(ids).size, ids.length);
	assert.equal(settingId({ keys: ['engine.orkflow_max_tasks'], envs: [] }), 'engine-orkflow-max-tasks');
});

test('every entry falls in a known area, so All settings drops none', () => {
	const known = new Set(AREAS.map((a) => a.id));
	for (const row of rows) assert.ok(known.has(settingArea(row)), row.keys[0]);
	assert.equal(settingArea({ keys: ['orker.max_concurrent'] }), 'orker');
	assert.equal(settingArea({ keys: [] }), 'telemetry');
});

test('a pipe in code or an escaped pipe stays in its cell, so a row never splits in two', () => {
	assert.deepEqual(splitRow('| `a|b` | c\\|d | e |'), ['`a|b`', 'c|d', 'e']);
});

test('Checked says only what the table says: a setting it is silent on shows no such row', () => {
	assert.equal(checkedWhen('At least 100; anything else is refused at boot.'), 'at boot');
	assert.equal(checkedWhen('Read at submit, so a change applies to new runs.'), 'read at submit, so a change applies to new runs');
	assert.match(checkedWhen('A whole number; anything else is refused at boot. Checked at validate, apply and plan, and again at submit.'), /^at boot; against each Orkflow at validate/);
	assert.equal(checkedWhen('Temporal frontend.'), null);
});

test('an accepts line cannot outlive its row: a value the table no longer names is refused', () => {
	const row = { defaultCell: '`docker`', effect: '`docker` or `podman`; at least 100', keys: ['x'], envCell: '`X`' };
	assert.deepEqual(acceptsProblems('x', '`docker` or `podman`, at least 100', row), []);
	assert.equal(acceptsProblems('x', '`nerdctl`', row).length, 1);
	assert.equal(acceptsProblems('x', 'at least 200', row).length, 1);
});

test('the repository accepts.yaml names only settings and values the table holds', () => {
	const overlay = YAML.parse(readFileSync(path.join(repoRoot, 'site/content/configuration/accepts.yaml'), 'utf8'));
	for (const [key, text] of Object.entries(overlay)) {
		const row = rows.find((r) => r.keys.includes(key) || r.envs.includes(key));
		assert.ok(row, `${key} is not in the table`);
		assert.deepEqual(acceptsProblems(key, text, row), []);
	}
});

test('folding loses no words: the lead and the rest together are the whole cell', () => {
	for (const row of rows) {
		const { lead, rest } = leadAndRest(row.effect);
		assert.equal(`${lead} ${rest}`.replace(/\s+/g, ' ').trim(), row.effect.replace(/\s+/g, ' ').trim(), row.keys[0]);
	}
});

test('a sentence never ends inside code or at an abbreviation', () => {
	assert.deepEqual(sentences('Set `a. B` here, e.g. There. Then this.'), ['Set `a. B` here, e.g. There.', 'Then this.']);
});

test('an entry is one line of HTML: a blank line would end the block and print its markup as text', () => {
	const kinds = schemaKinds(configGo);
	for (const row of rows) {
		const html = renderEntry(row, { accepts: kinds.get(row.keys[0]) ? 'text' : null, link: (t) => t });
		assert.ok(!html.includes('\n'), row.keys[0]);
		assert.match(html, /<dt>Environment<\/dt>/);
		assert.match(html, /<dt>Default<\/dt>/);
	}
});

test('a cell renders its code, bold and links, and escapes the rest', () => {
	assert.equal(inlineHtml('`a<b` **c** [d](e) <f>', (t) => `/x/${t}`), '<code>a&lt;b</code> <b>c</b> <a href="/x/e">d</a> &lt;f&gt;');
});

test('the table speaking of itself becomes links a reader on another page can follow', () => {
	const ctx = {
		deployHeadings: new Map([['Health checks', 'health-checks'], ['§4', '4-rolling-back']]),
		docHeadings: () => new Map([['Pins', 'pins']]),
		detailHeadings: new Map([['Sizing', 'sizing']]),
	};
	assert.equal(linkMentions('see §5, "Health checks"', ctx), 'see [Health checks](/docs/DEPLOY.md#health-checks)');
	assert.equal(linkMentions('(`docs/DSL.md`, "Pins")', ctx), '([Pins](/docs/DSL.md#pins))');
	assert.equal(linkMentions('see "Sizing" below', ctx), 'see [Sizing](route:docs/configuration/details#sizing)');
	assert.equal(linkMentions('see §4', ctx), 'see [DEPLOY.md §4](/docs/DEPLOY.md#4-rolling-back)');
	assert.equal(linkMentions('harnesses (AGENTS.md §11.1). More', ctx), 'harnesses. More');
});

test('a size says it takes digits: 256 KiB as the default must not read as a value to write', () => {
	assert.deepEqual(bytesDefault('`256 KiB`'), { value: 262144, perSecond: false });
	assert.deepEqual(bytesDefault('`10 MiB/s`'), { value: 10485760, perSecond: true });
	assert.equal(bytesDefault('`8`'), null);
	const kinds = new Map([['a.size', 'KindInt'], ['a.limit', 'KindInt']]);
	const size = { keys: ['a.size'], envs: ['A'], defaultCell: '`64 KiB`' };
	assert.equal(acceptsFor(size, kinds, {}), 'a whole number of bytes, written in digits: the default is `65536`');
	assert.equal(acceptsFor(size, kinds, { 'a.size': 'a whole number of bytes, at least 16 KiB' }), 'a whole number of bytes, at least 16 KiB, written in digits: the default is `65536`');
	assert.equal(acceptsFor({ keys: ['a.limit'], envs: ['B'], defaultCell: '`16`' }, kinds, {}), 'a whole number');
});

test('a row with no key finds its accepts line by its variable', () => {
	assert.equal(acceptsFor({ keys: [], envs: ['OTEL_X'], defaultCell: '' }, new Map(), { OTEL_X: 'text' }), 'text');
});

test('every key of a row is a link target: orker.capacity.memory lands on its entry', () => {
	const row = rows.find((r) => r.keys.includes('orker.capacity.memory'));
	assert.match(renderEntry(row, { accepts: null, link: (t) => t }), /id="orker-capacity-memory"/);
});

test('the proof harness is no reader’s concern: its words leave the entry, the fact stays', () => {
	const ctx = { deployHeadings: new Map(), docHeadings: () => new Map(), detailHeadings: new Map() };
	const row = rows.find((r) => r.keys.includes('engine.orkflow_max_tasks'));
	const text = linkMentions(row.effect, ctx);
	assert.doesNotMatch(text, /soak|nightly|proof/i);
	assert.match(text, /the largest size measured/);
});

test('a heading is found by its text and by its number', () => {
	const map = headingAnchors('## 4. Rolling back\n\n### Health checks\n\n```\n# not a heading\n```\n');
	assert.equal(map.get('Health checks'), 'health-checks');
	assert.equal(map.get('§4'), '4-rolling-back');
	assert.equal(map.has('not a heading'), false);
});

test('the filter needs every word, in any case and any order', () => {
	assert.equal(matchesFilter('engine.orkflow_max_tasks ORK_ORKFLOW_MAX_TASKS', 'MAX orkflow'), true);
	assert.equal(matchesFilter('storage.backend', 'storage engine'), false);
	assert.equal(matchesFilter('anything', '   '), true);
});
