import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';
import { exampleHref, exampleResponse, exampleSources, gallery, galleryHref } from '../src/lib/examples';

// F-ui-perf-5: the gallery carries names and descriptions, never documents;
// a document is served one at a time, byte for byte, from examples/. Both
// are written into the static build as files (routes/apply/examples*), which
// the engine serves as it serves any file of the build.
const dir = `${import.meta.dir}/../../examples`;
const files = readdirSync(dir).filter((name) => name.endsWith('.ork.yaml'));
const raw = Object.fromEntries(files.map((name) => [`../../../../examples/${name}`, readFileSync(`${dir}/${name}`, 'utf8')]));
const sources = exampleSources(raw);

test('the gallery lists every example in directory order, with no YAML', () => {
	const cards = gallery(sources);
	expect(cards.length).toBe(files.length);
	expect(cards.map((card) => card.file)).toEqual([...files].sort());
	for (const card of cards) {
		expect(Object.keys(card).sort()).toEqual(['description', 'file', 'name', 'slug']);
		expect(card.name.length > 0).toBe(true);
	}
	// What the page serializes is small: no document body rides along.
	const payload = JSON.stringify(cards);
	for (const source of sources) expect(payload).not.toContain(source.yaml);
});

test('an example is served byte for byte as text/yaml', async () => {
	for (const name of files) {
		const response = exampleResponse(sources, name);
		expect(response.status).toBe(200);
		expect(response.headers.get('content-type')).toBe('text/yaml; charset=utf-8');
		expect(await response.text()).toBe(readFileSync(`${dir}/${name}`, 'utf8'));
	}
});

test('an unknown file is 404, never another example', async () => {
	const response = exampleResponse(sources, 'no-such-example.ork.yaml');
	expect(response.status).toBe(404);
	expect(await response.text()).toContain('no-such-example');
	expect(exampleResponse(sources, '../secrets').status).toBe(404);
	// A slug alone is not a file name: the build writes each example under
	// its file name, the URL the page asks for.
	expect(exampleResponse(sources, 'decide-and-route').status).toBe(404);
});

test('the page asks for the files the build writes', () => {
	expect(galleryHref).toBe('/apply/examples.json');
	for (const source of sources) expect(exampleHref(source.slug)).toBe(`/apply/examples/${source.file}`);
});

test('only server code reads examples/ into the bundle', () => {
	// A glob of examples/ in any module a page can import would put every
	// document back into the client chunk.
	const offenders: string[] = [];
	const walk = (path: string) => {
		for (const name of readdirSync(path)) {
			const full = `${path}/${name}`;
			if (name === 'gen' || name === 'server') continue;
			if (!name.includes('.')) {
				walk(full);
				continue;
			}
			if (!/\.(ts|svelte)$/.test(name) || name.endsWith('.server.ts') || name === '+server.ts') continue;
			if (readFileSync(full, 'utf8').includes('examples/*.ork.yaml')) offenders.push(full);
		}
	};
	walk(`${import.meta.dir}/../src`);
	expect(offenders).toEqual([]);
});
