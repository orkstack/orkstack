import { expect, test } from 'bun:test';
import { focusTrap, nextTabStop } from '../src/lib/focusTrap';

// F-ui-ux-2: a modal takes the focus, keeps Tab inside, and gives the focus
// back. Tested against a hand-built DOM slice (the unit suite has no DOM).

test('Tab from the last control wraps to the first, Shift+Tab from the first to the last', () => {
	const stops = ['close', 'name', 'cancel', 'accept'];
	expect(nextTabStop(stops, 'accept', false)).toBe('close');
	expect(nextTabStop(stops, 'close', true)).toBe('accept');
	// Between the ends the browser's own move is already inside.
	expect(nextTabStop(stops, 'name', false)).toBeNull();
	expect(nextTabStop(stops, 'cancel', true)).toBeNull();
	// Focus on the panel itself (or anything outside) enters at an end.
	expect(nextTabStop(stops, 'panel', false)).toBe('close');
	expect(nextTabStop(stops, 'panel', true)).toBe('accept');
	expect(nextTabStop([], 'panel', false)).toBeNull();
});

interface Fake {
	name: string;
	focus(): void;
	getClientRects(): unknown[];
	isConnected: boolean;
}

function fakeDom(controls: string[], opts: { hidden?: string[]; initial?: string } = {}) {
	const doc = { activeElement: null as unknown };
	const make = (name: string): Fake => ({
		name,
		isConnected: true,
		focus() {
			doc.activeElement = this;
		},
		getClientRects() {
			return opts.hidden?.includes(name) ? [] : [{}];
		}
	});
	const opener = make('opener');
	opener.focus();
	const elements = controls.map(make);
	const listeners: ((event: KeyboardEvent) => void)[] = [];
	const node = {
		...make('panel'),
		ownerDocument: doc,
		querySelectorAll: () => elements,
		querySelector: (selector: string) => elements.find((e) => selector === `[name=${e.name}]`) ?? null,
		contains: (other: unknown) => elements.includes(other as Fake),
		addEventListener: (_: string, fn: (event: KeyboardEvent) => void) => listeners.push(fn),
		removeEventListener: (_: string, fn: (event: KeyboardEvent) => void) =>
			listeners.splice(listeners.indexOf(fn), 1)
	};
	node.focus = () => {
		doc.activeElement = node;
	};
	const tab = (shiftKey = false) => {
		let prevented = false;
		const event = {
			key: 'Tab',
			shiftKey,
			get defaultPrevented() {
				return prevented;
			},
			preventDefault() {
				prevented = true;
			}
		} as unknown as KeyboardEvent;
		for (const fn of listeners) fn(event);
		return prevented;
	};
	const active = () => (doc.activeElement as Fake | null)?.name;
	return { node: node as unknown as HTMLElement, opener, elements, tab, active, listeners };
}

test('a destructive confirmation starts on Cancel, and Tab never leaves the dialog', () => {
	const dom = fakeDom(['body-link', 'cancel', 'accept']);
	const trap = focusTrap(dom.node, { initial: '[name=cancel]' });
	expect(dom.active()).toBe('cancel');
	dom.elements[2].focus(); // accept
	expect(dom.tab()).toBe(true);
	expect(dom.active()).toBe('body-link');
	expect(dom.tab(true)).toBe(true);
	expect(dom.active()).toBe('accept');
	trap.destroy();
});

test('with no initial target, the first visible control takes the focus', () => {
	const dom = fakeDom(['hidden-thing', 'name', 'save'], { hidden: ['hidden-thing'] });
	focusTrap(dom.node);
	expect(dom.active()).toBe('name');
});

test('an initial target that cannot take the focus (busy, disabled) falls back', () => {
	const dom = fakeDom(['cancel', 'accept'], { hidden: ['cancel'] });
	focusTrap(dom.node, { initial: '[name=cancel]' });
	expect(dom.active()).toBe('accept');
});

test('focusContainer focuses the panel, and Tab then enters at the first control', () => {
	const dom = fakeDom(['close', 'save']);
	focusTrap(dom.node, { focusContainer: true });
	expect(dom.active()).toBe('panel');
	expect(dom.tab()).toBe(true);
	expect(dom.active()).toBe('close');
});

test('closing gives the focus back to the opener, if it is still on the page', () => {
	const dom = fakeDom(['cancel', 'accept']);
	const trap = focusTrap(dom.node, { initial: '[name=cancel]' });
	expect(dom.active()).toBe('cancel');
	trap.destroy();
	expect(dom.active()).toBe('opener');
	expect(dom.listeners).toHaveLength(0);

	const gone = fakeDom(['cancel', 'accept']);
	const trap2 = focusTrap(gone.node, { initial: '[name=cancel]' });
	gone.opener.isConnected = false;
	trap2.destroy();
	expect(gone.active()).toBe('cancel');
});

test('an inner trap that already moved the focus is not overridden by an outer one', () => {
	const dom = fakeDom(['a', 'b']);
	focusTrap(dom.node);
	dom.elements[1].focus();
	const event = { key: 'Tab', shiftKey: false, defaultPrevented: true, preventDefault() {} } as unknown as KeyboardEvent;
	for (const fn of dom.listeners) fn(event);
	expect(dom.active()).toBe('b');
});
