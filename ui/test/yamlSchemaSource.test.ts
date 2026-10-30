import { expect, test } from 'bun:test';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { SCHEMAS } from '../src/lib/yaml/schemas';

// The drift test. The editor validates, completes and explains from the
// generated schemas and from nothing else: ui/src/lib/schemas/ must be the
// docs/schemas/ files byte for byte (go run ./internal/schemas/cmd/genschemas
// writes both, TestSchemas_AreGenerated refuses either stale), the editor must
// read exactly those files, and no other schema may exist anywhere in ui/src.

const ui = `${import.meta.dir}/..`;
const repo = `${ui}/..`;
const KINDS = { Orkflow: 'orkflow.v1.json', Automation: 'automation.v1.json' } as const;

test('the UI’s copy is docs/schemas/, byte for byte', () => {
	const copies = readdirSync(`${ui}/src/lib/schemas`).sort();
	expect(copies).toEqual(Object.values(KINDS).sort());
	for (const file of copies) {
		const copy = readFileSync(`${ui}/src/lib/schemas/${file}`, 'utf8');
		const reference = readFileSync(`${repo}/docs/schemas/${file}`, 'utf8');
		expect(`${file}: ${copy === reference}`).toBe(`${file}: true`);
	}
});

test('the editor reads exactly those files', () => {
	for (const [kind, file] of Object.entries(KINDS)) {
		const reference = JSON.parse(readFileSync(`${repo}/docs/schemas/${file}`, 'utf8'));
		expect(SCHEMAS[kind as keyof typeof KINDS]).toEqual(reference);
	}
	const source = readFileSync(`${ui}/src/lib/yaml/schemas.ts`, 'utf8');
	const imports = [...source.matchAll(/^import\s+\w+\s+from\s+'([^']+\.json)';$/gm)].map((m) => m[1]).sort();
	expect(imports).toEqual(['../schemas/automation.v1.json', '../schemas/orkflow.v1.json']);
});

test('no other schema, and no hand-made key table, lives in ui/src', () => {
	const found: string[] = [];
	// Files only: a route directory may carry an extension (routes/apply/examples.json).
	const files = readdirSync(`${ui}/src`, { recursive: true }).filter(
		(f) => /\.(?:ts|svelte|js|json|css|html)$/.test(f) && statSync(`${ui}/src/${f}`).isFile()
	);
	expect(files.length > 100).toBe(true);
	for (const file of files) {
		if (file.startsWith('lib/schemas/')) continue;
		const text = readFileSync(`${ui}/src/${file}`, 'utf8');
		if (/"\$schema"\s*:|json-schema\.org/.test(text)) found.push(file);
		// The task keys spelled out as a list would be a second key table.
		if (/['"]timeout_seconds['"][\s\S]{0,200}['"]run_as_orker_user['"]/.test(text)) found.push(`${file} (key table)`);
	}
	expect(found).toEqual([]);
});
