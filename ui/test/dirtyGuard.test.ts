import { expect, test } from 'bun:test';
import { guardUnsaved, leavesRoute, unsavedVerdict } from '../src/lib/dirtyGuard';

// F-ui-ux-3: a form with typing in it never loses it to a navigation the
// reader did not confirm.

test('a clean form leaves without a question', () => {
	let asked = 0;
	expect(unsavedVerdict(false, 'link', () => (asked++, false))).toBe('proceed');
	expect(unsavedVerdict(false, 'leave', () => (asked++, false))).toBe('proceed');
	expect(asked).toBe(0);
});

test('a dirty form asks before an in-app navigation, and stays when refused', () => {
	expect(unsavedVerdict(true, 'link', () => false)).toBe('cancel');
	expect(unsavedVerdict(true, 'goto', () => true)).toBe('proceed');
	expect(unsavedVerdict(true, 'popstate', () => false)).toBe('cancel');
});

test('leaving the document hands over to the browser prompt, never confirm()', () => {
	let asked = 0;
	expect(unsavedVerdict(true, 'leave', () => (asked++, true))).toBe('cancel');
	expect(asked).toBe(0);
});

type Navigation = { type: string; cancel(): void; to: unknown; from: unknown };
function play(type: string): { cancelled: boolean } {
	const callbacks = (globalThis as unknown as { __beforeNavigate: ((n: Navigation) => void)[] }).__beforeNavigate;
	const fn = callbacks[callbacks.length - 1];
	const outcome = { cancelled: false };
	fn({ type, cancel: () => (outcome.cancelled = true), to: null, from: null });
	return outcome;
}

test('guardUnsaved reads isDirty at the moment of leaving, and asks the message', () => {
	let dirty = true;
	const asked: string[] = [];
	let answer = false;
	guardUnsaved(
		() => dirty,
		'Discard the namespace you were creating?',
		(message) => (asked.push(message), answer)
	);
	expect(play('link').cancelled).toBe(true);
	expect(asked).toEqual(['Discard the namespace you were creating?']);
	answer = true;
	expect(play('link').cancelled).toBe(false);
	// Saved: the same guard now lets everything through, unasked.
	dirty = false;
	expect(play('link').cancelled).toBe(false);
	expect(play('leave').cancelled).toBe(false);
	expect(asked).toHaveLength(2);
});

test('leavesRoute: the same route (a filter, a page, the switcher) is staying', () => {
	const at = (id: string | null) => ({ route: { id } });
	const nav = (from: ReturnType<typeof at> | null, to: ReturnType<typeof at> | null) =>
		({ from, to }) as unknown as Parameters<typeof leavesRoute>[0];
	expect(leavesRoute(nav(at('/admin/namespaces'), at('/admin/namespaces')))).toBe(false);
	expect(leavesRoute(nav(at('/admin/namespaces'), at('/runs')))).toBe(true);
	// Leaving the document has no destination route.
	expect(leavesRoute(nav(at('/admin/namespaces'), null))).toBe(true);
});
