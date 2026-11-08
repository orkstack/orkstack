import { expect, test } from 'bun:test';
import { PANEL_EDGE, PANEL_GAP, placePanel, waitRoomNote } from '../src/lib/placement.ts';

const view = { width: 1280, height: 800 };
const panel = { width: 590, height: 200 };
const box = (left: number, top: number, size = 32) => ({ left, right: left + size, top, bottom: top + size });

const cases = [
	['below, end-aligned, in open space', box(1000, 100), 'end', { left: 442, top: 138 }],
	['below, start-aligned, in open space', box(300, 100), 'start', { left: 300, top: 138 }],
	['above a trigger at the bottom edge', box(1000, 760), 'end', { left: 442, top: 554 }],
	['moved right of an end-aligned trigger at the left edge', box(0, 100), 'end', { left: PANEL_EDGE, top: 138 }],
	['moved left of a start-aligned trigger at the right edge', box(1248, 100), 'start', { left: 682, top: 138 }]
] as const;
for (const [name, anchor, align, want] of cases) {
	test(name, () => {
		const { left, top } = placePanel(anchor, panel, view, align);
		expect({ left, top }).toEqual(want);
	});
}

test('a panel too tall for either side takes the larger side and scrolls', () => {
	const tall = { width: 590, height: 900 };
	expect(placePanel(box(600, 500), tall, view)).toEqual({ left: 600, top: PANEL_EDGE, maxHeight: 500 - PANEL_GAP - PANEL_EDGE });
	expect(placePanel(box(600, 100), tall, view)).toEqual({ left: 600, top: 138, maxHeight: 800 - PANEL_EDGE - 132 - PANEL_GAP });
});

test('a trigger scrolled out of sight keeps its panel in the viewport', () => {
	const above = placePanel(box(600, -400), panel, view);
	expect(above.top).toBe(PANEL_EDGE);
	const under = placePanel(box(600, 1500), panel, view);
	expect(under.top + panel.height).toBeLessThanOrEqual(view.height - PANEL_EDGE);
});

test('a panel wider than the viewport keeps its start on screen', () => {
	expect(placePanel(box(100, 100), { width: 500, height: 100 }, { width: 320, height: 640 }, 'end').left).toBe(PANEL_EDGE);
});

test('a viewport shorter than the panel never asks for a negative height', () => {
	const tiny = placePanel(box(10, 2, 30), { width: 100, height: 400 }, { width: 400, height: 40 });
	expect(tiny.maxHeight < 0).toBe(false);
});

test('what a run holds of a pool it waits on reads as one sentence', () => {
	expect(waitRoomNote(0, '')).toBe('');
	expect(waitRoomNote(2, '')).toBe("2 of the tasks there are this run's");
	expect(waitRoomNote(0, '4s')).toBe('its finished ones took 4s');
	expect(waitRoomNote(2, '4s')).toBe("2 of the tasks there are this run's; its finished ones took 4s");
});
