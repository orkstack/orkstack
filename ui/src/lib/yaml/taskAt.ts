// The task the editor's cursor is in: its id, what it `uses:`, its `op`
// (when it names one) and the `with:` keys it sets. The docs rail shows that
// activity's interface beside the code. Read from the document as typed, so
// a half-written task still answers with what it has so far.
import { isMap, isScalar, isSeq, parseDocument, type Node } from 'yaml';

export interface TaskAtCursor {
	id: string;
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
		return {
			id: scalar(item.get('id', true)),
			uses: scalar(item.get('uses', true)),
			op: scalar(item.get('op', true)) || withOp,
			set,
			line: text.slice(0, start).split('\n').length
		};
	}
	return null;
}
