import { expect, test } from 'bun:test';
import { rowOrkers } from '../src/lib/taskOrker';

test('rowOrkers names one Orker, counts several and says nothing for none', () => {
	expect(rowOrkers([])).toBeNull();
	expect(rowOrkers(['', ''])).toBeNull();
	expect(rowOrkers(['orker-a'])).toEqual({ word: 'orker-a' });
	// A fan-out whose items all landed on one Orker reads like a plain task.
	expect(rowOrkers(['orker-a', '', 'orker-a'])).toEqual({ word: 'orker-a' });
	expect(rowOrkers(['orker-b', 'orker-a', 'orker-b'])).toEqual({ word: '2 Orkers', tip: 'orker-a, orker-b' });
});
