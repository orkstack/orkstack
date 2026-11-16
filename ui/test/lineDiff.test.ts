import { expect, test } from 'bun:test';
import { unifiedDiff } from '../src/lib/lineDiff';

test('unifiedDiff marks removed and added lines and keeps the rest', () => {
	const d = unifiedDiff('a\nb\nc', 'a\nB\nc\nd').split('\n').slice(3);
	expect(d).toEqual([' a', '-b', '+B', ' c', '+d']);
});
