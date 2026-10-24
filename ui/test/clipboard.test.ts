import { expect, test } from 'bun:test';
import { copyText, type CopyEnv } from '../src/lib/clipboard';

// The three outcomes of a copy (F-ui-ux-6): the Clipboard API, the legacy
// execCommand path a plain-HTTP origin still allows, and 'manual' — never a
// silent nothing.

function fakeDocument(execResult: boolean | 'throw') {
	const log: string[] = [];
	const area = {
		value: '',
		style: {} as Record<string, string>,
		setAttribute() {},
		select() {
			log.push(`select:${area.value}`);
		}
	};
	const focused = { focus: () => log.push('refocus') };
	const doc = {
		createElement: () => area,
		body: {
			appendChild: () => log.push('append'),
			removeChild: () => log.push('remove')
		},
		execCommand: (command: string) => {
			log.push(`exec:${command}`);
			if (execResult === 'throw') throw new Error('not allowed');
			return execResult;
		},
		activeElement: focused
	};
	return { doc: doc as unknown as NonNullable<CopyEnv['document']>, log };
}

test('the Clipboard API is used when it is there', async () => {
	const written: string[] = [];
	const { doc, log } = fakeDocument(true);
	const outcome = await copyText('orkctl run get ork-1', {
		clipboard: { writeText: async (text) => void written.push(text) },
		document: doc
	});
	expect(outcome).toBe('copied');
	expect(written).toEqual(['orkctl run get ork-1']);
	expect(log).toEqual([]);
});

test('an absent Clipboard API falls back to execCommand, and cleans up', async () => {
	const { doc, log } = fakeDocument(true);
	const outcome = await copyText('orkctl run get ork-1', { clipboard: undefined, document: doc });
	expect(outcome).toBe('copied');
	expect(log).toEqual(['append', 'select:orkctl run get ork-1', 'exec:copy', 'remove', 'refocus']);
});

test('a refused Clipboard API also falls back', async () => {
	const { doc, log } = fakeDocument(true);
	const outcome = await copyText('x', {
		clipboard: { writeText: () => Promise.reject(new Error('denied')) },
		document: doc
	});
	expect(outcome).toBe('copied');
	expect(log).toContain('exec:copy');
});

test('when neither path copies, the outcome is manual', async () => {
	for (const result of [false, 'throw'] as const) {
		const { doc, log } = fakeDocument(result);
		expect(await copyText('x', { clipboard: undefined, document: doc })).toBe('manual');
		expect(log).toContain('remove');
	}
	expect(await copyText('x', {})).toBe('manual');
});
