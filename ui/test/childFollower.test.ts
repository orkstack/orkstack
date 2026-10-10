import { expect, test } from 'bun:test';
import { childFollower } from '../src/lib/childFollower';

// F-ui-perf-9 and F-ui-ux-7 for composed-task cards: no read and no socket
// until a card is on screen, one of each once it is, the socket closed when it
// leaves, and a stopped child stream reported with a way to follow again.

function harness(terminal = false) {
	const counts = { reads: 0, sockets: 0, open: 0 };
	const failures: string[] = [];
	let fail: ((detail: string) => void) | undefined;
	const make = () =>
		childFollower({
			read: async () => {
				counts.reads++;
				return !terminal;
			},
			watch: (_id, _current, onState) => {
				counts.sockets++;
				counts.open++;
				fail = (detail) => onState('failed', detail);
				let closed = false;
				return () => {
					if (!closed) counts.open--;
					closed = true;
				};
			},
			onFailure: (detail) => failures.push(detail)
		});
	return { counts, failures, make, fail: (detail: string) => fail?.(detail) };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

test('twenty composed cards read nothing and open nothing until one is visible, then one of each', async () => {
	const h = harness();
	const cards = Array.from({ length: 20 }, (_, i) => {
		const card = h.make();
		card.setRunId(`ork-child-${i}`);
		return card;
	});
	await settle();
	expect(h.counts).toEqual({ reads: 0, sockets: 0, open: 0 });
	cards[3].setVisible(true);
	await settle();
	expect(h.counts).toEqual({ reads: 1, sockets: 1, open: 1 });
});

test('leaving the screen closes the socket; returning reads once more and follows', async () => {
	const h = harness();
	const card = h.make();
	card.setRunId('ork-child');
	card.setVisible(true);
	await settle();
	card.setVisible(false);
	expect(h.counts.open).toBe(0);
	card.setVisible(true);
	await settle();
	expect(h.counts).toEqual({ reads: 2, sockets: 2, open: 1 });
	card.dispose();
	expect(h.counts.open).toBe(0);
});

test('a terminal child is read and never followed', async () => {
	const h = harness(true);
	const card = h.make();
	card.setVisible(true);
	card.setRunId('ork-child');
	await settle();
	expect(h.counts).toEqual({ reads: 1, sockets: 0, open: 0 });
});

test('a child stream that stopped is reported, and retry follows again', async () => {
	const h = harness();
	const card = h.make();
	card.setRunId('ork-child');
	card.setVisible(true);
	await settle();
	h.fail('the event stream could not be opened after 7 attempts');
	expect(h.failures[h.failures.length - 1]).toBe('the event stream could not be opened after 7 attempts');
	card.retry();
	expect(h.failures[h.failures.length - 1]).toBe('');
	await settle();
	expect(h.counts.sockets).toBe(2);
	expect(h.counts.open).toBe(1);
});
