// The one sentence under the editor that says what is known about the
// document on screen.
//
// The rule: "no problems" is the engine's word, never the editor's. A document
// reads as clean only when the engine answered for exactly this text in
// exactly this namespace and reported no error. A check still in flight, an
// answer about an earlier draft, or a check that could not run is said as
// such — never as valid, because apply asks the same engine.
//
// The editor's own schema findings are errors until the engine has answered.
// Once it has found no problem, they are warnings: the schema is deliberately
// stricter than the compiler in a few documented places (docs/DSL.md, "Editor
// support"), and a red mark beside a document the engine takes would be the
// editor contradicting the authority. Even then the sentence never promises
// an apply: validation reads no edit token and no stored version.
import { ValidationSeverity } from '../gen/orkstack/v1/orkflow_service_pb';
import type { EngineIssue } from './server';
import type { Diagnostic } from './types';

export type EngineState =
	| { kind: 'idle' }
	| { kind: 'pending'; text: string; namespace: string }
	| { kind: 'answered'; text: string; namespace: string; issues: readonly EngineIssue[] }
	| { kind: 'failed'; text: string; namespace: string; message: string };

export type StatusTone = 'empty' | 'checking' | 'clean' | 'problems' | 'unavailable';

export interface EditorStatus {
	tone: StatusTone;
	errors: number;
	warnings: number;
	sentence: string;
}

const count = (n: number, word: string) => `${n} ${word}${n === 1 ? '' : 's'}`;

/** Whether the engine's answer is about exactly this text and namespace, with no error. */
export function engineClean(engine: EngineState, text: string, namespace: string): boolean {
	return (
		engine.kind === 'answered' &&
		engine.text === text &&
		engine.namespace === namespace &&
		!engine.issues.some((i) => i.severity !== ValidationSeverity.WARNING)
	);
}

/** The editor's own findings as they should be drawn, given the engine's answer. */
export function localFindings(local: readonly Diagnostic[], engine: EngineState, text: string, namespace: string): Diagnostic[] {
	if (!engineClean(engine, text, namespace)) return [...local];
	return local.map((d) => (d.source === 'schema' ? { ...d, severity: 'warning' } : d));
}

export function editorStatus(input: {
	text: string;
	namespace: string;
	local: readonly Diagnostic[];
	engine: EngineState;
	/** What a clean check did not cover, said after it ("Apply also …"). */
	cleanNote?: string;
}): EditorStatus {
	const { text, namespace, local, engine } = input;
	const note = input.cleanNote ? ` ${input.cleanNote}` : '';
	if (text.trim() === '') return { tone: 'empty', errors: 0, warnings: 0, sentence: 'Nothing to check yet.' };

	const syntax = local.filter((d) => d.source === 'syntax').length;
	if (syntax > 0) {
		return {
			tone: 'problems',
			errors: syntax,
			warnings: 0,
			sentence: `${count(syntax, 'syntax error')}. The engine checks the document once it parses.`
		};
	}

	const localErrors = local.filter((d) => d.severity === 'error').length;
	const localWarnings = local.length - localErrors;
	const current = engine.kind !== 'idle' && engine.text === text && engine.namespace === namespace;

	if (!current || engine.kind === 'pending') {
		const found = localErrors > 0 ? `${count(localErrors, 'problem')} found so far. ` : '';
		return { tone: 'checking', errors: localErrors, warnings: localWarnings, sentence: `${found}Checking with the engine…` };
	}
	if (engine.kind === 'failed') {
		return {
			tone: 'unavailable',
			errors: localErrors,
			warnings: localWarnings,
			sentence: `The engine could not check this document, so it is not known to be valid: ${engine.message}`
		};
	}

	const engineErrors = engine.issues.filter((i) => i.severity !== ValidationSeverity.WARNING).length;
	const engineWarnings = engine.issues.length - engineErrors;
	if (engineErrors > 0) {
		const errors = engineErrors + localErrors;
		const warned = engineWarnings + localWarnings > 0 ? ` ${count(engineWarnings + localWarnings, 'warning')}.` : '';
		return {
			tone: 'problems',
			errors,
			warnings: engineWarnings + localWarnings,
			sentence: `The engine refuses this document: ${count(errors, 'problem')}.${warned}`
		};
	}
	// The engine is satisfied: the editor's own findings are warnings now.
	const warnings = engineWarnings + local.length;
	const schema = local.length > 0 ? ` The schema check, stricter in places, notes ${count(local.length, 'point')}.` : '';
	const warned = engineWarnings > 0 ? ` ${count(engineWarnings, 'warning')} from the engine.` : '';
	return { tone: 'clean', errors: 0, warnings, sentence: `The engine finds no problems in this document.${warned}${schema}${note}` };
}
