// The engine's answer, placed in the document.
//
// ValidateOrkflow and PlanAutomation answer with issues that carry a field
// path in the engine's spelling (`tasks[2].with.body`, `pins.etl`) and a
// sentence, which for authored data begins with the line it is about
// (`line 9: yes is a boolean…`, internal/dsl/strict.go). The field places
// the finding on its key or value; the line, when the sentence names one and
// the field resolves elsewhere or nowhere, wins, because the engine read that
// line itself. The sentence is the engine's, verbatim but for that prefix,
// which the editor already shows as a position.
import { ValidationSeverity } from '../gen/orkstack/v1/orkflow_service_pb';
import type { Document } from 'yaml';
import { displayPath, headAt, lineCol, parseFieldPath, parseYaml, rangeAt, type Range } from './document';
import type { Diagnostic, PathSegment } from './types';

/** The part of a ValidationIssue the editor reads. */
export interface EngineIssue {
	field: string;
	message: string;
	severity: ValidationSeverity;
}

// "line N:" where a message states its line: at its start (the strict
// mapping, `line 9: yes is…`), or after a colon or a line break, as yaml.v3
// words a decode error (`parse yaml: yaml: unmarshal errors:\n  line 7: field
// need not found…`). Not "first at line 3)", which points elsewhere.
const LINE = /(?:^|\n|: )\s*line (\d+): /;
const LEADING_LINE = /^line \d+: /;

/** The range of one line's text, without its indentation or line break. */
function lineRange(text: string, line: number): Range | null {
	let start = 0;
	for (let n = 1; n < line; n++) {
		const next = text.indexOf('\n', start);
		if (next === -1) return null;
		start = next + 1;
	}
	let end = text.indexOf('\n', start);
	if (end === -1) end = text.length;
	const body = text.slice(start, end);
	const lead = body.length - body.trimStart().length;
	return { from: start + lead, to: start + body.trimEnd().length };
}

/**
 * Where a field path points: the node's value when it fits on one line, else
 * its key (a sequence item's first key); a path that is not written falls
 * back to its nearest written ancestor, and the empty path to the document's
 * first key.
 */
function placeField(text: string, doc: Document.Parsed, path: readonly PathSegment[]): Range | null {
	if (path.length > 0) {
		const value = rangeAt(doc, path, 'value');
		if (value && !text.slice(value.from, value.to).includes('\n')) return value;
	}
	for (let n = path.length; n >= 0; n--) {
		const head = headAt(doc, path.slice(0, n));
		if (head) return head;
	}
	return null;
}

export function engineFindings(text: string, issues: readonly EngineIssue[]): Diagnostic[] {
	const { doc } = parseYaml(text);
	return issues.map((issue) => {
		const m = LINE.exec(issue.message);
		const line = m ? Number(m[1]) : null;
		const lead = LEADING_LINE.exec(issue.message);
		const path = parseFieldPath(issue.field);
		let range = placeField(text, doc, path);
		if (line !== null && (range === null || lineCol(text, range.from).line !== line)) {
			range = lineRange(text, line) ?? range;
		}
		const r = range ?? lineRange(text, 1) ?? { from: 0, to: 0 };
		return {
			from: Math.min(r.from, text.length),
			to: Math.min(Math.max(r.to, r.from), text.length),
			severity: issue.severity === ValidationSeverity.WARNING ? 'warning' : 'error',
			source: 'engine',
			message: lead ? issue.message.slice(lead[0].length) : issue.message,
			path: displayPath(path)
		} satisfies Diagnostic;
	});
}
