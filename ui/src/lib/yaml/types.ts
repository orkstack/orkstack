// The YAML editor's vocabulary, shared by its pure modules (lint, server
// findings, status, completion, highlighting) and the Svelte component that
// draws them. Nothing here imports CodeMirror, so every rule is unit-tested
// under bun without a DOM.

/** The authored document kinds (AGENTS.md §3.1), each with its own schema. */
export type DocumentKind = 'Orkflow' | 'Automation';

/** One step of a path into a document: a mapping key or a sequence index. */
export type PathSegment = string | number;

/**
 * Where a finding came from. `syntax` and `schema` are the editor's own,
 * computed as the document is typed; `engine` is the server's answer
 * (ValidateOrkflow or PlanAutomation), which stays the authority.
 */
export type Source = 'syntax' | 'schema' | 'engine';

export type Severity = 'error' | 'warning';

/** One finding, placed by character offsets into the document. */
export interface Diagnostic {
	from: number;
	to: number;
	severity: Severity;
	source: Source;
	message: string;
	/** The node's path in the engine's own spelling (`tasks[0].with.op`); '' for the document. */
	path: string;
}

/** A JSON Schema as the generator writes it (draft-07); read, never built. */
export type JsonSchema = { [key: string]: unknown } | boolean;
