import { expect, test } from 'bun:test';
import { grouped, match, rank, MATCH_EXACT, MATCH_PREFIX, MATCH_SUBSEQUENCE, MATCH_SUBSTRING, MATCH_WORD, PER_GROUP, TOTAL } from '../src/lib/palette/rank';
import { paletteKey, isApplePlatform, shortcutHint, shortcutAria } from '../src/lib/palette/keys';
import { paletteKeydown, PAGE_STEP } from '../src/lib/palette/keyboard';
import { commandId, nameQuery, namespaceCommands, openRunCommand, pageCommands, themeCommands, type Command } from '../src/lib/palette/commands';
import { providedCommands, providerCount, registerProvider, PROVIDER_LIMIT } from '../src/lib/palette/context';
import { readRecents, rememberRecent, RECENT_KEY, RECENT_LIMIT } from '../src/lib/palette/recent';

// The command palette's logic, all of it pure: ranking, the keyboard, the
// shortcut per platform, the page providers, the recents, and the rule that
// the palette never writes.

const cmd = (id: string, label: string, group: Command['group'] = 'Pages'): Command => ({
	id,
	label,
	group,
	action: { kind: 'navigate', href: `/${id}` }
});

test('match grades exact > prefix > word start > substring > subsequence, ignoring case', () => {
	expect(match('Runs', 'runs')?.grade).toBe(MATCH_EXACT);
	expect(match('Runs list', 'RUN')?.grade).toBe(MATCH_PREFIX);
	expect(match('Go to Runs', 'run')?.grade).toBe(MATCH_WORD);
	expect(match('Overruns', 'run')?.grade).toBe(MATCH_SUBSTRING);
	expect(match('Image policy', 'impo')?.grade).toBe(MATCH_SUBSEQUENCE);
	expect(match('Image policy', 'xyz')).toBeNull();
	// A shorter subsequence span ranks first.
	expect((match('a-b-c', 'abc')?.span ?? 0) < (match('a---b---c', 'abc')?.span ?? 0)).toBe(true);
});

test('rank orders by grade, then span, then group order, then recency, then source order', () => {
	const commands = [cmd('b', 'Go to Overruns'), cmd('a', 'Runs'), cmd('c', 'Go to Runs', 'Actions'), cmd('d', 'Go to Runs')];
	const out = rank(commands, 'runs', ['Actions', 'Pages']);
	expect(out.map((c) => c.id)).toEqual(['a', 'c', 'd', 'b']);
	const recency = new Map([['d', 10]]);
	expect(rank([cmd('x', 'Go to Runs'), cmd('d', 'Go to Runs')], 'runs', ['Pages'], recency).map((c) => c.id)).toEqual(['d', 'x']);
	// With nothing typed, the order the sources gave is kept (the navigation's own order).
	expect(rank([cmd('z', 'Zeta'), cmd('a', 'Alpha')], '', ['Pages']).map((c) => c.id)).toEqual(['z', 'a']);
});

test('rank keeps at most PER_GROUP per group and TOTAL in all', () => {
	const many = Array.from({ length: 30 }, (_, i) => cmd(`p${i}`, `item ${i}`));
	expect(rank(many, 'item', ['Pages']).length).toBe(PER_GROUP);
	const groups: Command['group'][] = ['Pages', 'Runs', 'Orkflows', 'Tasks', 'Actions', 'Recent', 'Changed runs'];
	const spread = groups.flatMap((g) => Array.from({ length: 10 }, (_, i) => cmd(`${g}-${i}`, `item ${i}`, g)));
	expect(rank(spread, 'item', groups).length).toBe(TOTAL);
});

test('grouped places each group where its best match ranked', () => {
	const ranked = [cmd('a', 'x', 'Runs'), cmd('b', 'x', 'Pages'), cmd('c', 'x', 'Runs')];
	expect(grouped(ranked).map((g) => [g.group, g.items.map((c) => c.id)])).toEqual([
		['Runs', ['a', 'c']],
		['Pages', ['b']]
	]);
});

test('the keyboard: arrows wrap, PageUp/Down step five, Enter runs, Ctrl/Cmd+Enter opens a new tab, Escape closes', () => {
	const none = { ctrlKey: false, metaKey: false };
	expect(paletteKeydown('ArrowDown', none, -1, 3)).toEqual({ kind: 'move', active: 0 });
	expect(paletteKeydown('ArrowDown', none, 2, 3)).toEqual({ kind: 'move', active: 0 });
	expect(paletteKeydown('ArrowUp', none, 0, 3)).toEqual({ kind: 'move', active: 2 });
	expect(paletteKeydown('PageDown', none, 1, 20)).toEqual({ kind: 'move', active: 1 + PAGE_STEP });
	expect(paletteKeydown('PageDown', none, 18, 20)).toEqual({ kind: 'move', active: 19 });
	expect(paletteKeydown('PageUp', none, 3, 20)).toEqual({ kind: 'move', active: 0 });
	expect(paletteKeydown('Enter', none, 1, 3)).toEqual({ kind: 'run', active: 1, newTab: false });
	expect(paletteKeydown('Enter', { ctrlKey: true, metaKey: false }, 1, 3)).toEqual({ kind: 'run', active: 1, newTab: true });
	expect(paletteKeydown('Enter', { ctrlKey: false, metaKey: true }, 1, 3)).toEqual({ kind: 'run', active: 1, newTab: true });
	expect(paletteKeydown('Enter', none, -1, 0)).toEqual({ kind: 'stay' });
	expect(paletteKeydown('Escape', none, 0, 3)).toEqual({ kind: 'close' });
	expect(paletteKeydown('Tab', none, 0, 3)).toEqual({ kind: 'stay' });
	expect(paletteKeydown('Home', none, 0, 3)).toEqual({ kind: 'pass' });
	expect(paletteKeydown('a', none, 0, 3)).toEqual({ kind: 'pass' });
});

const k = (over: Partial<{ key: string; metaKey: boolean; ctrlKey: boolean; altKey: boolean; shiftKey: boolean }>) => ({
	key: 'k',
	metaKey: false,
	ctrlKey: false,
	altKey: false,
	shiftKey: false,
	...over
});

test('on an Apple platform only ⌘K opens it, and Ctrl+K is left to the text field', () => {
	expect(isApplePlatform('MacIntel')).toBe(true);
	expect(isApplePlatform('iPhone')).toBe(true);
	expect(isApplePlatform('macOS')).toBe(true);
	expect(paletteKey(k({ metaKey: true }), true, false)).toBe('toggle');
	expect(paletteKey(k({ ctrlKey: true }), true, false)).toBe('ignore');
	expect(shortcutHint(true)).toBe('⌘K');
	expect(shortcutAria(true)).toBe('Meta+K');
});

test('elsewhere only Ctrl+K opens it', () => {
	expect(isApplePlatform('Linux x86_64')).toBe(false);
	expect(isApplePlatform('Win32')).toBe(false);
	expect(paletteKey(k({ ctrlKey: true }), false, false)).toBe('toggle');
	expect(paletteKey(k({ ctrlKey: true, key: 'K' }), false, false)).toBe('toggle');
	expect(paletteKey(k({ metaKey: true }), false, false)).toBe('ignore');
	expect(paletteKey(k({ ctrlKey: true, shiftKey: true }), false, false)).toBe('ignore');
	expect(paletteKey(k({ ctrlKey: true, key: 'j' }), false, false)).toBe('ignore');
	expect(shortcutHint(false)).toBe('Ctrl K');
	expect(shortcutAria(false)).toBe('Control+K');
});

test('over another open overlay the shortcut is consumed and opens nothing, on both platforms', () => {
	expect(paletteKey(k({ metaKey: true }), true, true)).toBe('consume');
	expect(paletteKey(k({ ctrlKey: true }), false, true)).toBe('consume');
	// A key that is not the shortcut is still not ours to swallow.
	expect(paletteKey(k({ ctrlKey: true }), true, true)).toBe('ignore');
});

test('"Open run q" is offered for any one-word query, navigates, and parses nothing', () => {
	for (const q of ['ork-1792204627744865045-1', 'abc', 'RUN_42', 'x']) {
		const c = openRunCommand(q, 'payments');
		expect(c?.action).toEqual({ kind: 'navigate', href: `/runs/${encodeURIComponent(q)}?ns=payments` });
	}
	expect(openRunCommand('two words', 'default')).toBeNull();
	expect(openRunCommand('  ', 'default')).toBeNull();
	expect(openRunCommand('a/b', 'default')?.action).toEqual({ kind: 'navigate', href: '/runs/a%2Fb?ns=default' });
});

test('only a query the engine would take as a name is sent as a name filter', () => {
	expect(nameQuery('Deploy')).toBe('deploy');
	expect(nameQuery('d')).toBeNull();
	expect(nameQuery('9lives')).toBeNull();
	expect(nameQuery('ork-17922')).toBe('ork-17922');
	expect(nameQuery('a b')).toBeNull();
});

test('no static command writes: each navigates, switches appearance or namespace', () => {
	const statics = [...pageCommands('default'), ...namespaceCommands(['default', 'payments'], 'default'), ...themeCommands('system')];
	expect(statics.length).toBe(16 + 1 + 3);
	for (const c of statics) expect(['navigate', 'theme', 'namespace']).toContain(c.action.kind);
	expect(new Set(statics.map((c) => c.id)).size).toBe(statics.length);
	for (const c of statics) expect(/^[a-z0-9-]+$/.test(c.id)).toBe(true);
	expect(namespaceCommands(['default', 'payments'], 'default').map((c) => c.label)).toEqual(['Switch to namespace payments']);
	expect(commandId('page', 'Image policy')).toBe('page-image-policy');
});

test('a page provider builds nothing until asked, and returns at most PROVIDER_LIMIT', () => {
	const tasks = Array.from({ length: 4000 }, (_, i) => `t${i}`);
	let calls = 0;
	const stop = registerProvider({
		id: 'run-page',
		commands: (q) => {
			calls++;
			const out: Command[] = [];
			for (const id of tasks) {
				if (out.length >= PROVIDER_LIMIT) break;
				if (match(id, q)) out.push(cmd(`task-${id}`, `Jump to task ${id}`, 'Tasks'));
			}
			return out;
		}
	});
	expect(calls).toBe(0);
	expect(providerCount()).toBe(1);
	const offered = providedCommands('t3');
	expect(calls).toBe(1);
	expect(offered.length <= PROVIDER_LIMIT).toBe(true);
	stop();
	expect(providerCount()).toBe(0);
	expect(providedCommands('t3')).toEqual([]);
});

test('a provider replaced before the old one is removed keeps the new one', () => {
	const stopOld = registerProvider({ id: 'p', commands: () => [cmd('old', 'old')] });
	const stopNew = registerProvider({ id: 'p', commands: () => [cmd('new', 'new')] });
	stopOld();
	expect(providedCommands('').map((c) => c.id)).toEqual(['new']);
	stopNew();
	// A provider that throws offers nothing and breaks nothing.
	const stopBad = registerProvider({ id: 'bad', commands: () => { throw new Error('x'); } });
	expect(providedCommands('')).toEqual([]);
	stopBad();
});

function memoryStore(throws = false) {
	const m = new Map<string, string>();
	return {
		m,
		getItem: (key: string) => {
			if (throws) throw new Error('SecurityError');
			return m.get(key) ?? null;
		},
		setItem: (key: string, v: string) => {
			if (throws) throw new Error('SecurityError');
			m.set(key, v);
		}
	};
}

test('recents keep ids and names only, newest first, at most RECENT_LIMIT', () => {
	const store = memoryStore();
	for (let i = 0; i < 12; i++) rememberRecent({ kind: 'run', id: `ork-${i}`, namespace: 'default', orkflow: 'deploy' }, i, store);
	rememberRecent({ kind: 'orkflow', id: 'deploy', namespace: 'default' }, 99, store);
	const recents = readRecents(store);
	expect(recents.length).toBe(RECENT_LIMIT);
	expect(recents[0]).toEqual({ kind: 'orkflow', id: 'deploy', namespace: 'default', orkflow: undefined, at: 99 });
	// Only the declared fields are written, whatever the caller passes.
	rememberRecent({ kind: 'run', id: 'ork-x', namespace: 'default', inputs: { secret: 1 } } as never, 100, store);
	expect(store.m.get(RECENT_KEY)?.includes('secret')).toBe(false);
	// Re-opening moves an entry to the front rather than duplicating it.
	rememberRecent({ kind: 'run', id: 'ork-11', namespace: 'default' }, 101, store);
	expect(readRecents(store).filter((r) => r.id === 'ork-11').length).toBe(1);
});

test('recents survive a throwing or corrupt storage as none', () => {
	const bad = memoryStore(true);
	expect(readRecents(bad)).toEqual([]);
	rememberRecent({ kind: 'run', id: 'x', namespace: 'default' }, 1, bad);
	const corrupt = memoryStore();
	corrupt.m.set(RECENT_KEY, '{not json');
	expect(readRecents(corrupt)).toEqual([]);
	corrupt.m.set(RECENT_KEY, JSON.stringify([{ kind: 'nope' }, 7, null]));
	expect(readRecents(corrupt)).toEqual([]);
});
