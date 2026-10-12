import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';

// F-ui-perf-3: a route loads only the schemas of the services it calls.
// Generated code is one module per .proto file, so this holds only while
// (1) every generated file declares at most one service, (2) every client
// module imports only its own service's file, and (3) nothing builds every
// client at once — one import of such a factory would put every service's
// schemas back on every route.
const root = new URL('../src/lib/', import.meta.url).pathname;
const read = (path: string) => readFileSync(root + path, 'utf8');

test('every generated file declares at most one service', () => {
	const files = readdirSync(root + 'gen/orkstack/v1').filter((f) => f.endsWith('_pb.ts'));
	let services = 0;
	for (const file of files) {
		const declared = read(`gen/orkstack/v1/${file}`).match(/serviceDesc\(/g)?.length ?? 0;
		expect(`${file}: ${declared}`).toBe(`${file}: ${Math.min(declared, 1)}`);
		services += declared;
	}
	expect(services).toBe(12);
});

test('each client module imports only its own service', () => {
	const modules = readdirSync(root + 'clients').filter((f) => f.endsWith('.ts'));
	expect(modules.length).toBe(12);
	for (const module of modules) {
		const source = read(`clients/${module}`);
		const generated = [...source.matchAll(/from '\.\.\/gen\/orkstack\/v1\/(\w+)'/g)].map((m) => m[1]);
		expect(`${module}: ${generated.length}`).toBe(`${module}: 1`);
		const created = source.match(/createClient\((\w+),/)?.[1] ?? '';
		expect(read(`gen/orkstack/v1/${generated[0]}.ts`)).toContain(`export const ${created}: GenService<`);
	}
});

test('client.ts imports no generated module and builds no client', () => {
	const source = read('client.ts');
	expect(source).not.toContain('/gen/');
	expect(source).not.toContain('createClient(');
});

test('nothing outside src/lib/clients creates a client', () => {
	const src = new URL('../src/', import.meta.url).pathname;
	const offenders = readdirSync(src, { recursive: true })
		.filter((path) => /\.(ts|svelte)$/.test(path) && !/^lib\/(gen|clients)\//.test(path))
		.filter((path) => readFileSync(src + path, 'utf8').includes('createClient('));
	expect(offenders).toEqual([]);
});
