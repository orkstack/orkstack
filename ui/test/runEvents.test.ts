import { expect, test } from 'bun:test';
import { watchRunEvents, type RunSocketState } from '../src/lib/runEvents';

// F-ui-ux-7: a run whose socket dies stops saying CONNECTING. Reconnects that
// deliver no frame are bounded, a delivered frame resets the budget, the
// backoff doubles until one arrives, and Retry resumes from the last sequence.

class fakeSocket {
	static opened: fakeSocket[] = [];
	onopen: (() => void) | null = null;
	onmessage: ((event: { data: string }) => void) | null = null;
	onerror: (() => void) | null = null;
	onclose: ((event?: { code?: number; reason?: string }) => void) | null = null;
	closed = false;
	constructor(readonly url: string) {
		fakeSocket.opened.push(this);
	}
	open() {
		this.onopen?.();
	}
	close(event?: { code?: number; reason?: string }) {
		if (this.closed) return;
		this.closed = true;
		this.onclose?.(event);
	}
	frame(sequence: number, type = 'TASK_STARTED') {
		this.onmessage?.({ data: JSON.stringify({ runId: 'ork-1', sequence: String(sequence), type, taskId: 'a' }) });
	}
}

function watcher(opts: { fromSequence?: bigint } = {}) {
	fakeSocket.opened = [];
	(globalThis as { WebSocket?: unknown }).WebSocket = fakeSocket;
	const states: RunSocketState[] = [];
	const failures: string[] = [];
	const delays: number[] = [];
	const events: bigint[] = [];
	let pending: (() => void) | null = null;
	const stop = watchRunEvents(
		'ork-1',
		'alpha',
		(event) => events.push(event.sequence),
		(state, detail) => {
			states.push(state);
			if (state === 'failed') failures.push(detail ?? '');
		},
		{
			...opts,
			retryBaseMs: 100,
			schedule: (fn, ms) => {
				delays.push(ms);
				pending = fn;
				return 1;
			},
			cancel: () => (pending = null)
		}
	);
	return {
		states,
		failures,
		delays,
		events,
		stop,
		socket: () => fakeSocket.opened[fakeSocket.opened.length - 1],
		count: () => fakeSocket.opened.length,
		/** The reconnect timer fires. */
		elapse: () => {
			const fn = pending;
			pending = null;
			fn?.();
		}
	};
}

test('six reconnects that deliver nothing end in failed, with a reason, and stop', () => {
	const w = watcher();
	for (let i = 0; i < 7; i++) {
		w.socket().close({ code: 1006 });
		w.elapse();
	}
	expect(w.count()).toBe(7);
	expect(w.states[w.states.length - 1]).toBe('failed');
	expect(w.failures).toHaveLength(1);
	expect(w.failures[0]).toContain('could not be opened after 7 attempts (close 1006)');
	// Nothing reconnects after failed.
	w.elapse();
	expect(w.count()).toBe(7);
});

test('opening is not progress: open-then-drop also ends in failed', () => {
	const w = watcher();
	for (let i = 0; i < 7; i++) {
		w.socket().open();
		w.socket().close();
		w.elapse();
	}
	expect(w.states[w.states.length - 1]).toBe('failed');
	expect(w.failures[0]).toContain('opened and closed 7 times without delivering anything');
});

test('the backoff doubles until a frame arrives, and a frame resets the count', () => {
	const w = watcher();
	for (let i = 0; i < 4; i++) {
		w.socket().close();
		w.elapse();
	}
	expect(w.delays).toEqual([100, 200, 400, 800]);
	w.socket().open();
	w.socket().frame(1);
	w.socket().close();
	w.elapse();
	expect(w.delays[w.delays.length - 1]).toBe(100);
	// The budget is whole again: six reconnects that deliver nothing (the
	// one just made and five more) before the next close gives up.
	for (let i = 0; i < 5; i++) {
		w.socket().close();
		w.elapse();
	}
	expect(w.states.includes('failed')).toBe(false);
	w.socket().close();
	expect(w.states[w.states.length - 1]).toBe('failed');
	expect(w.events).toEqual([1n]);
});

test('a reconnect resumes from the last sequence, and dedupes', () => {
	const w = watcher();
	w.socket().frame(1);
	w.socket().frame(2);
	w.socket().close();
	w.elapse();
	expect(w.socket().url).toContain('from_sequence=2');
	expect(w.socket().url).toContain('namespace=alpha');
	w.socket().frame(2);
	w.socket().frame(3);
	expect(w.events).toEqual([1n, 2n, 3n]);
});

test('Retry starts from the sequence the stopped stream reached', () => {
	const w = watcher({ fromSequence: 41n });
	expect(w.socket().url).toContain('from_sequence=41');
	w.socket().frame(41);
	w.socket().frame(42);
	expect(w.events).toEqual([42n]);
});

test('stopping cancels a pending reconnect', () => {
	const w = watcher();
	w.socket().close();
	w.stop();
	w.elapse();
	expect(w.count()).toBe(1);
});
