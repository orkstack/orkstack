// The task the editor's cursor is in: its id, what it `uses:`, its `op`
// (when it names one), the `with:` keys it sets, and whether it runs an
// activity at all. The docs rail shows that activity's interface beside the
// code. Read from the document as typed, so a half-written task still
// answers with what it has so far.
import { isMap, isScalar, isSeq, parseDocument, type Node } from 'yaml';

/**
 * What the task runs: an activity (named by `uses:`, or not named yet), or
 * nothing of its own — an awaited task, a sleep, or a composed task, whose
 * `uses: orkflow:…` starts a child run.
 */
export type TaskKind = 'activity' | 'await' | 'sleep' | 'orkflow';

export interface TaskAtCursor {
	id: string;
	kind: TaskKind;
	uses: string;
	op: string;
	/** The `with:` keys this task sets, in document order. */
	set: string[];
	/** 1-based line the task starts on. */
	line: number;
}

const scalar = (node: unknown): string => (isScalar(node) && node.value !== null && node.value !== undefined ? String(node.value) : '');

/** The task whose mapping holds `pos`, or null outside every task. */
export function taskAt(text: string, pos: number): TaskAtCursor | null {
	const doc = parseDocument(text, { version: '1.2', strict: false, uniqueKeys: false });
	const tasks = isMap(doc.contents) ? doc.contents.get('tasks', true) : null;
	if (!isSeq(tasks)) return null;
	for (const item of tasks.items as Node[]) {
		if (!isMap(item) || !item.range) continue;
		const [start, , end] = item.range;
		if (pos < start || pos > end) continue;
		const withNode = item.get('with', true);
		const set = isMap(withNode) ? withNode.items.map((pair) => scalar(pair.key)).filter(Boolean) : [];
		const withOp = isMap(withNode) ? scalar(withNode.get('op', true)) : '';
		const uses = scalar(item.get('uses', true));
		const kind: TaskKind = item.has('await') ? 'await' : item.has('sleep') ? 'sleep' : uses.startsWith('orkflow:') ? 'orkflow' : 'activity';
		return {
			id: scalar(item.get('id', true)),
			kind,
			uses,
			op: scalar(item.get('op', true)) || withOp,
			set,
			line: text.slice(0, start).split('\n').length
		};
	}
	return null;
}
