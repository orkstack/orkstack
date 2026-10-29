// The generated schemas the editor is built from. ui/src/lib/schemas/ is
// written by `go run ./internal/schemas/cmd/genschemas` from the Go parsers'
// key tables, byte for byte the docs/schemas/ files (the UI is built from ui/
// alone, so it cannot import those). Nobody edits them: TestSchemas_AreGenerated
// refuses a stale copy, and ui/test/yamlSchemaSource.test.ts refuses a copy
// that differs from docs/schemas/ or a schema reaching the editor any other way.
import orkflow from '../schemas/orkflow.v1.json';
import automation from '../schemas/automation.v1.json';
import type { DocumentKind, JsonSchema } from './types';

export const SCHEMAS: Record<DocumentKind, Record<string, unknown>> = {
	Orkflow: orkflow as Record<string, unknown>,
	Automation: automation as Record<string, unknown>
};

/** The schema node a JSON pointer (`#/definitions/x/properties/y`) names, or undefined. */
export function schemaAt(root: Record<string, unknown>, pointer: string): JsonSchema | undefined {
	const parts = pointer.replace(/^#\/?/, '').split('/').filter((p) => p !== '');
	let node: unknown = root;
	for (const raw of parts) {
		const part = raw.replace(/~1/g, '/').replace(/~0/g, '~');
		if (node === null || typeof node !== 'object') return undefined;
		node = (node as Record<string, unknown>)[part];
	}
	return node as JsonSchema | undefined;
}
