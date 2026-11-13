import { expect, test } from 'bun:test';
import { EventType } from '../src/lib/gen/orkstack/v1/run_pb';
import events from '../src/lib/webhook-events.json';
import { OPT_IN_RUN_EVENTS, RUN_EVENTS, TASK_EVENTS } from '../src/lib/webhooks';

// The page's lists are held to pkg/webhooks' (generated into webhook-events.json
// by internal/schemas/cmd/genschemas), so an event added there cannot be
// missing from, or extra on, the Admin page.
const names = (list: EventType[]) => list.map((event) => EventType[event]).sort();

test('RUN_EVENTS is the empty-subscription set', () => {
	expect(names(RUN_EVENTS)).toEqual([...events.run].sort());
});

test('TASK_EVENTS is the task events', () => {
	expect(names(TASK_EVENTS)).toEqual([...events.task].sort());
});

test('the three lists together are every subscribable event, each once', () => {
	const all = names([...RUN_EVENTS, ...OPT_IN_RUN_EVENTS, ...TASK_EVENTS]);
	expect(new Set(all).size).toBe(all.length);
	expect(all).toEqual([...events.known].sort());
});

test('OPT_IN_RUN_EVENTS holds no run transition and no task event', () => {
	for (const name of names(OPT_IN_RUN_EVENTS)) {
		expect(events.run).not.toContain(name);
		expect(events.task).not.toContain(name);
	}
});
