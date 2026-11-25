import { expect, test } from 'bun:test';
import { statSync } from 'node:fs';
import { SCHEMAS } from '../src/lib/yaml/schemas';
import { listDir, listTree, readSource, scanTest } from './sourceTree';

// The drift test. The editor validates, completes and explains from the
// generated schemas and from nothing else: ui/src/lib/schemas/ must be the
// docs/schemas/ files byte for byte (go run ./internal/schemas/cmd/genschemas
// writes both, TestSchemas_AreGenerated refuses either stale), the editor must
// read exactly those files, and no other schema may exist anywhere in ui/src.

const ui = `${import.meta.dir}/..`;
const repo = `${ui}/..`;
const KINDS = { Orkflow: 'orkflow.v1.json', Automation: 'automation.v1.json' } as const;

scanTest('the UI’s copy is docs/schemas/, byte for byte', () => {
	const copies = listDir(`${ui}/src/lib/schemas`).sort();
	expect(copies).toEqual(Object.values(KINDS).sort());
	for (const file of copies) {
		const copy = readSource(`${ui}/src/lib/schemas/${file}`);
		const reference = readSource(`${repo}/docs/schemas/${file}`);
		expect(`${file}: ${copy === reference}`).toBe(`${file}: true`);
	}
});

test('the editor reads exactly those files', () => {
	for (const [kind, file] of Object.entries(KINDS)) {
		const reference = JSON.parse(readSource(`${repo}/docs/schemas/${file}`));
		expect(SCHEMAS[kind as keyof typeof KINDS]).toEqual(reference);
	}
	const source = readSource(`${ui}/src/lib/yaml/schemas.ts`);
	const imports = [...source.matchAll(/^import\s+\w+\s+from\s+'([^']+\.json)';$/gm)].map((m) => m[1]).sort();
	expect(imports).toEqual(['../schemas/automation.v1.json', '../schemas/orkflow.v1.json']);
});

scanTest('no other schema, and no hand-made key table, lives in ui/src', () => {
	const found: string[] = [];
	// Files only: a route directory may carry an extension (routes/apply/examples.json).
	const files = listTree(`${ui}/src`).filter(
		(f) => /\.(?:ts|svelte|js|json|css|html)$/.test(f) && statSync(`${ui}/src/${f}`).isFile()
	);
	expect(files.length > 100).toBe(true);
	for (const file of files) {
		if (file.startsWith('lib/schemas/')) continue;
		// The code those files compile to, which yamlValidators.test.ts holds to them byte for byte.
		if (file === 'lib/yaml/validators.gen.ts' || file === 'lib/yaml/conditions.gen.ts') continue;
		const text = readSource(`${ui}/src/${file}`);
		if (/"\$schema"\s*:|json-schema\.org/.test(text)) found.push(file);
		// The task keys spelled out as a list would be a second key table.
		if (/['"]timeout_seconds['"][\s\S]{0,200}['"]run_as_orker_user['"]/.test(text)) found.push(`${file} (key table)`);
	}
	expect(found).toEqual([]);
});
