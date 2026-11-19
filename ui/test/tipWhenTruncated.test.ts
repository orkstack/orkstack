import { expect, test } from 'bun:test';
import { tipWhenTruncated, truncationTip } from '../src/lib/tipWhenTruncated';

// A label cut short by its ellipsis shows its whole text as the tooltip; one
// that fits carries none.
test('the tip is the whole text exactly while the label overflows', () => {
	expect(truncationTip(400, 200, 'release-block')).toBe('release-block');
	expect(truncationTip(200, 200, 'release-block')).toBeUndefined();
	expect(truncationTip(120, 200, 'release-block')).toBeUndefined();
	expect(truncationTip(400, 200, '')).toBeUndefined();
});

test('the action sets and clears data-tip as the label overflows and its text changes', () => {
	const node = { scrollWidth: 400, clientWidth: 200, dataset: {} as Record<string, string> };
	const action = tipWhenTruncated(node as unknown as HTMLElement, 'a-very-long-orkflow-name');
	expect(node.dataset.tip).toBe('a-very-long-orkflow-name');
	action.update('another-long-name');
	expect(node.dataset.tip).toBe('another-long-name');
	node.scrollWidth = 150;
	action.update('short');
	expect(node.dataset.tip).toBeUndefined();
	action.destroy();
});
