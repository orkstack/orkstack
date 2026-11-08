import { expect, test } from 'bun:test';
import { finishedWords } from '../src/lib/when';

test('finishedWords says the date only when the finish is not the day it started', () => {
	const started = new Date(2026, 10, 5, 23, 50, 0);
	expect(finishedWords(new Date(2026, 10, 5, 23, 59, 30), started)).toBe('23:59:30');
	const crossed = finishedWords(new Date(2026, 10, 6, 0, 10, 5), started);
	expect(crossed).toContain('00:10:05');
	expect(crossed).toContain('6');
	expect(crossed).not.toBe('00:10:05');
	// A task that never started has nothing to differ from.
	expect(finishedWords(new Date(2026, 10, 6, 0, 10, 5), undefined)).toBe('00:10:05');
});
