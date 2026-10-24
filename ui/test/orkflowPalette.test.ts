import { expect, test } from 'bun:test';
import { orkflowPageCommands, ORKFLOW_TABS } from '../src/lib/palette/orkflowPage';
import { PROVIDER_LIMIT } from '../src/lib/palette/context';

// The Orkflow page's palette provider: what the page adds while the palette
// is open. Submit a run… hands control to the page (its own dialog, with its
// confirmation and CLI twin) and never submits; the tabs are navigations; a
// task id jumps to its row in the task register. It builds nothing per task
// up front and never offers more than the provider limit.

function setup(tasks: string[], tab = 'overview') {
	const opened: string[] = [];
	const options = {
		name: 'order-saga',
		tab,
		tasks: () => tasks,
		hrefOf: (t: string) => `/orkflows/order-saga?ns=default${t === 'overview' ? '' : `&tab=${t}`}`,
		canSubmit: true,
		openSubmit: () => opened.push('submit')
	};
	return { options, opened };
}

test('with nothing typed: Submit a run… and the other tabs, never the tab you are on, within the limit', () => {
	const { options, opened } = setup(['a', 'b']);
	const out = orkflowPageCommands(options, '');
	expect(out.map((c) => c.label)).toEqual(['Submit a run…', 'Show versions', 'Show triggers', 'Show schedules', 'Edit source']);
	expect(out.length <= PROVIDER_LIMIT).toBe(true);
	// Nothing ran while the commands were built.
	expect(opened).toEqual([]);
	const submit = out[0];
	expect(submit.action.kind).toBe('page');
	if (submit.action.kind === 'page') submit.action.run();
	expect(opened).toEqual(['submit']);
	// The tabs navigate, keeping the namespace.
	const versions = out.find((c) => c.label === 'Show versions')!;
	expect(versions.action).toEqual({ kind: 'navigate', href: '/orkflows/order-saga?ns=default&tab=versions' });
	expect(versions.href).toBe('/orkflows/order-saga?ns=default&tab=versions');
	// On another tab, the overview is offered instead of that tab.
	const fromVersions = orkflowPageCommands(setup(['a'], 'versions').options, '').map((c) => c.label);
	expect(fromVersions).toContain('Show overview');
	expect(fromVersions).not.toContain('Show versions');
	expect(ORKFLOW_TABS).toEqual(['overview', 'versions', 'triggers', 'schedules', 'edit']);
});

test('no Submit a run… when the page has no document to submit', () => {
	const { options } = setup(['a']);
	expect(orkflowPageCommands({ ...options, canSubmit: false }, '').map((c) => c.label)).not.toContain('Submit a run…');
});

test('a task id jumps to its row in the register, on the overview', () => {
	const { options } = setup(['validate', 'reserve-stock', 'charge', 'ship']);
	const out = orkflowPageCommands(options, 'charge');
	const jump = out.find((c) => c.id === 'orkflow-task-charge')!;
	expect(jump.label).toBe('Jump to task charge');
	expect(jump.group).toBe('Tasks');
	expect(jump.action).toEqual({ kind: 'navigate', href: '/orkflows/order-saga?ns=default#task-row-charge' });
	// From another tab the jump goes back to the overview's register.
	const fromEdit = orkflowPageCommands(setup(['charge'], 'edit').options, 'charge');
	expect(fromEdit.find((c) => c.id === 'orkflow-task-charge')!.action).toEqual({
		kind: 'navigate',
		href: '/orkflows/order-saga?ns=default#task-row-charge'
	});
});

test('4,000 tasks: the best eight ids, the exact one first, and the scan only when asked', () => {
	const ids = Array.from({ length: 4000 }, (_, i) => `t${i}`);
	let reads = 0;
	const { options } = setup([]);
	const counted = { ...options, tasks: () => (reads++, ids) };
	// Nothing typed: no task is listed and the definition is not scanned.
	expect(orkflowPageCommands(counted, '').some((c) => c.group === 'Tasks')).toBe(false);
	expect(reads).toBe(0);
	const out = orkflowPageCommands(counted, 't3999');
	expect(reads).toBe(1);
	expect(out.length <= PROVIDER_LIMIT).toBe(true);
	expect(out[0].id).toBe('orkflow-task-t3999');
	const some = orkflowPageCommands(counted, 't1');
	expect(some.filter((c) => c.group === 'Tasks')).toHaveLength(PROVIDER_LIMIT);
	expect(some[0].id).toBe('orkflow-task-t1');
});

test('a query that names a command and a task keeps the command first', () => {
	const { options } = setup(['schedule-sync']);
	const out = orkflowPageCommands(options, 'schedule');
	expect(out[0].label).toBe('Show schedules');
	expect(out.map((c) => c.id)).toContain('orkflow-task-schedule-sync');
});
