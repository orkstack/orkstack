import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { STATUS_MARKS, statusMark } from '../src/lib/statusMarks';
import {
	CELL_GAP,
	KIND_ITEM,
	KIND_STACK,
	KIND_TASK,
	buildModel,
	cellAt,
	countStatuses,
	createTaskMap,
	fillStatuses,
	mapGeometry,
	mapKeydown,
	mapSummary,
	paintCells,
	readoutPosition,
	rectOf,
	scrollForCursor,
	scrollToCard,
	worstStatus,
	type MapColors,
	type MapTask
} from '../src/lib/taskMap';

const one = (cells: number, w: number, h: number) => mapGeometry([{ count: cells, labelled: false }], w, h);

// The plan's map areas (frame minus its 1.5 px rule, the 2 rem patch and the
// 0.5 rem padding) at the viewports the specs and DESIGN.md cite.
test('pitch: the largest of 16…8 px whose rows fit, else 8 px and the area scrolls', () => {
	const cases: [string, number, number, number, number, number, number, number][] = [
		// name, cells, width, height, pitch, columns, rows, scroll
		['1280x720 one', 1, 829, 251, 16, 51, 1, 0],
		['1280x720 80', 80, 829, 251, 16, 51, 2, 0],
		['1280x720 200', 200, 829, 251, 16, 51, 4, 0],
		['1280x720 4000', 4000, 829, 251, 8, 103, 39, 61],
		['1280x720 14000', 14000, 829, 251, 8, 103, 136, 136 * 8 - 251],
		['1280x360 200', 200, 829, 189, 16, 51, 4, 0],
		['1280x360 4000', 4000, 829, 189, 8, 103, 39, 123],
		['1440x900 200', 200, 989, 327, 16, 61, 4, 0],
		['1440x900 4000', 4000, 989, 327, 8, 123, 33, 0],
		['1440x900 14000', 14000, 989, 327, 8, 123, 114, 114 * 8 - 327],
		['1920x1080 200', 200, 1469, 403, 16, 91, 3, 0],
		['1920x1080 4000', 4000, 1469, 403, 12, 122, 33, 0],
		['1920x1080 14000', 14000, 1469, 403, 8, 183, 77, 77 * 8 - 403],
		['390x844 200', 200, 339, 303, 16, 21, 10, 0],
		['390x844 4000', 4000, 339, 303, 8, 42, 96, 465]
	];
	for (const [name, cells, w, h, pitch, columns, rows, scroll] of cases) {
		const g = one(cells, w, h);
		expect({ name, pitch: g.pitch, columns: g.columns, rows: g.segments[0].rows, scroll: g.scroll }).toEqual({
			name,
			pitch,
			columns,
			rows,
			scroll
		});
	}
});

test('cellAt(centre of rectOf(i)) is i for every cell, and the gaps hit nothing', () => {
	const g = mapGeometry(
		[
			{ count: 37, labelled: false },
			{ count: 23, labelled: true },
			{ count: 5, labelled: true }
		],
		200,
		120
	);
	const total = 65;
	for (let i = 0; i < total; i++) {
		const r = rectOf(g, i);
		expect(r.w).toBe(g.pitch - CELL_GAP);
		expect(cellAt(g, r.x + r.w / 2, r.y + r.h / 2)).toBe(i);
		// The gap to the right of and below a cell is no cell.
		expect(cellAt(g, r.x + r.w + 0.5, r.y + r.h / 2)).toBe(-1);
		expect(cellAt(g, r.x + r.w / 2, r.y + r.h + 0.5)).toBe(-1);
	}
	// A section's label band, and past the last cell of a short row, are nothing.
	expect(cellAt(g, 1, g.segments[1].y - 2)).toBe(-1);
	expect(cellAt(g, g.pitch * (g.columns - 1) + 1, g.segments[2].y + 1)).toBe(-1);
	expect(cellAt(g, -1, 1)).toBe(-1);
});

const map = () =>
	mapGeometry(
		[
			{ count: 10, labelled: false },
			{ count: 7, labelled: true }
		],
		4 * 16,
		400
	); // 4 columns at 16 px: rows of 4,4,2 then 4,3

const st = (n: number, failed: number[] = []) => {
	const s = new Uint8Array(n).fill(Status.SUCCEEDED);
	for (const i of failed) s[i] = Status.FAILED;
	return s;
};

test('keyboard: arrows, Home/End, Ctrl+Home/End and PageUp/Down at every boundary', () => {
	const g = map();
	const s = st(17);
	const k = (cursor: number, key: string, mods: { ctrlKey?: boolean; shiftKey?: boolean } = {}) =>
		mapKeydown(g, cursor, { key, ...mods }, { statuses: s, visibleRows: 2 })?.cursor;
	expect(k(0, 'ArrowLeft')).toBe(0);
	expect(k(0, 'ArrowRight')).toBe(1);
	expect(k(16, 'ArrowRight')).toBe(16);
	// Down within a segment, onto a short row (clamped), then into the next segment.
	expect(k(1, 'ArrowDown')).toBe(5);
	expect(k(7, 'ArrowDown')).toBe(9);
	expect(k(9, 'ArrowDown')).toBe(11);
	expect(k(8, 'ArrowDown')).toBe(10);
	expect(k(15, 'ArrowDown')).toBe(15);
	expect(k(16, 'ArrowDown')).toBe(16);
	// Up into the previous segment's last row, clamped to its last cell.
	expect(k(13, 'ArrowUp')).toBe(9);
	expect(k(10, 'ArrowUp')).toBe(8);
	expect(k(0, 'ArrowUp')).toBe(0);
	expect(k(6, 'Home')).toBe(4);
	expect(k(5, 'End')).toBe(7);
	expect(k(8, 'End')).toBe(9);
	expect(k(6, 'Home', { ctrlKey: true })).toBe(0);
	expect(k(6, 'End', { ctrlKey: true })).toBe(16);
	expect(k(1, 'PageDown')).toBe(9);
	expect(k(16, 'PageUp')).toBe(9);
	expect(mapKeydown(g, 3, { key: 'q' }, { statuses: s, visibleRows: 2 })).toBeNull();
	expect(mapKeydown(g, 3, { key: 'Enter' }, { statuses: s, visibleRows: 2 })).toEqual({ cursor: 3, select: true });
	expect(mapKeydown(g, 3, { key: ' ' }, { statuses: s, visibleRows: 2 })).toEqual({ cursor: 3, select: true });
});

test('F and Shift+F jump to the next and previous failed cell, wrapping', () => {
	const g = map();
	const s = st(17, [2, 12]);
	const k = (cursor: number, key: string, shiftKey = false) =>
		mapKeydown(g, cursor, { key, shiftKey }, { statuses: s, visibleRows: 2 })?.cursor;
	expect(k(0, 'f')).toBe(2);
	expect(k(2, 'f')).toBe(12);
	expect(k(12, 'f')).toBe(2);
	expect(k(0, 'F', true)).toBe(12);
	expect(k(12, 'F', true)).toBe(2);
	// No failed cell: the cursor stays.
	expect(mapKeydown(g, 5, { key: 'f' }, { statuses: st(17), visibleRows: 2 })?.cursor).toBe(5);
});

test('the cursor row is kept inside the scrolled area', () => {
	const g = one(4000, 829, 251); // 8 px, 39 rows, scrolls 61
	expect(scrollForCursor(g, 0, 0, 251)).toBe(0);
	const last = 3999;
	const r = rectOf(g, last);
	expect(scrollForCursor(g, last, 0, 251)).toBe(r.y + g.pitch - 251);
	expect(scrollForCursor(g, 0, 61, 251)).toBe(0);
	// Already in view: untouched.
	expect(scrollForCursor(g, 103 * 10, 20, 251)).toBe(20);
});

const tasks = (): MapTask[] => [
	{ id: 'report', status: Status.PENDING, needs: ['convert', 'charge'] },
	{ id: 'validate', status: Status.SUCCEEDED, needs: [] },
	{
		id: 'convert',
		status: Status.RUNNING,
		needs: ['validate'],
		items: [
			{ index: 0, status: Status.SUCCEEDED },
			{ index: 1, status: Status.FAILED },
			{ index: 2, status: Status.RUNNING }
		]
	},
	{ id: 'charge', status: Status.FAILED, needs: ['validate'] }
];

test('the model: tasks in DAG order, a fan-out as a stack cell, then a section of its items', () => {
	const m = buildModel(tasks(), true);
	expect(m.order).toBe('dag');
	expect(m.cells.map((c) => c.taskId + (c.item === undefined ? '' : `[${c.item}]`))).toEqual([
		'validate',
		'convert',
		'charge',
		'report',
		'convert[0]',
		'convert[1]',
		'convert[2]'
	]);
	expect(m.kinds[1]).toBe(KIND_STACK);
	expect(m.kinds[0]).toBe(KIND_TASK);
	expect(m.kinds[5]).toBe(KIND_ITEM);
	expect(m.segments).toEqual([
		{ start: 0, count: 4, labelled: false },
		{ start: 4, count: 3, labelled: true, taskId: 'convert' }
	]);
	expect(m.indexOf('convert', 1)).toBe(5);
	expect(m.indexOf('charge')).toBe(2);
	expect(m.indexOf('nope')).toBe(-1);
});

test('without a definition the map follows the run order and says so', () => {
	const m = buildModel(tasks(), false);
	expect(m.order).toBe('run');
	expect(m.cells.slice(0, 4).map((c) => c.taskId)).toEqual(['report', 'validate', 'convert', 'charge']);
	expect(mapSummary(countStatuses(new Uint8Array([Status.SUCCEEDED]), new Uint8Array([KIND_TASK])), 1, 'run')).toContain(
		"in run order: this run's Orkflow definition could not be read"
	);
});

test('statuses: a stack cell shows its worst item; a live batch dirties only what moved', () => {
	const ts = tasks();
	const m = buildModel(ts, true);
	const statuses = new Uint8Array(m.cells.length);
	const first = fillStatuses(m, ts, statuses);
	expect(first.length).toBe(m.cells.length);
	expect(statuses[1]).toBe(Status.FAILED);
	expect(worstStatus([Status.SUCCEEDED, Status.RUNNING, Status.CANCELED])).toBe(Status.RUNNING);
	expect(worstStatus([])).toBeUndefined();
	ts[3].status = Status.FAILED; // unchanged
	ts[0].status = Status.RUNNING;
	ts[2].items![2].status = Status.SUCCEEDED;
	expect(fillStatuses(m, ts, statuses).sort((a, b) => a - b)).toEqual([3, 6]);
});

test('counts and the description name every present status, items included, stacks not twice', () => {
	const ts = tasks();
	const m = buildModel(ts, true);
	const statuses = new Uint8Array(m.cells.length);
	fillStatuses(m, ts, statuses);
	const counts = countStatuses(statuses, m.kinds);
	expect(counts.get(Status.SUCCEEDED)).toBe(2);
	expect(counts.get(Status.FAILED)).toBe(2);
	expect(counts.get(Status.RUNNING)).toBe(1);
	expect(counts.get(Status.PENDING)).toBe(1);
	expect(mapSummary(counts, 4, 'dag')).toBe(
		'2 succeeded, 1 running, 2 failed, 1 pending; arrows move, Enter selects, F jumps to the next failed task.'
	);
	const big = new Map([[Status.SUCCEEDED, 3412]]);
	expect(mapSummary(big, 3412, 'dag').startsWith('3,412 succeeded')).toBe(true);
});

test('the readout sits beside its cell, flipped at the frame edges', () => {
	const size = { w: 120, h: 24 };
	const frame = { w: 400, h: 200 };
	expect(readoutPosition({ x: 10, y: 10, w: 14, h: 14 }, size, frame)).toEqual({ left: 32, top: 32 });
	// At the right edge it flips to the cell's left; at the bottom, above it.
	expect(readoutPosition({ x: 380, y: 10, w: 14, h: 14 }, size, frame)).toEqual({ left: 252, top: 32 });
	expect(readoutPosition({ x: 10, y: 190, w: 8, h: 8 }, size, frame)).toEqual({ left: 26, top: 158 });
	// Never outside the frame.
	expect(readoutPosition({ x: 2, y: 2, w: 6, h: 6 }, { w: 500, h: 24 }, frame).left).toBe(0);
});

/** A fake 2D context that counts what was drawn and where. */
function fakeContext() {
	const calls: string[] = [];
	const touched = new Set<string>();
	const ctx = {
		fillStyle: '',
		strokeStyle: '',
		lineWidth: 1,
		fillRect(x: number, y: number) {
			calls.push('fillRect');
			touched.add(`${x},${y}`);
		},
		strokeRect() {
			calls.push('strokeRect');
		},
		clearRect() {
			calls.push('clearRect');
		},
		beginPath() {
			calls.push('beginPath');
		},
		moveTo() {},
		lineTo() {},
		closePath() {},
		arc() {},
		stroke() {
			calls.push('stroke');
		},
		fill() {
			calls.push('fill');
		},
		setTransform() {}
	};
	return { ctx, calls, touched };
}

const colors: MapColors = {
	paper: 'P',
	ink: 'I',
	focus: 'F',
	status: Object.fromEntries(
		Object.keys(STATUS_MARKS).map((s) => [Number(s), { bg: `bg${s}`, solid: `solid${s}` }])
	) as MapColors['status']
};

test('a 4,000-cell paint is O(n) calls and reads no computed style; a dirty repaint touches only changed cells', () => {
	const g = one(4000, 829, 251);
	const statuses = new Uint8Array(4000).fill(Status.SUCCEEDED);
	const kinds = new Uint8Array(4000).fill(KIND_TASK);
	const original = globalThis.getComputedStyle;
	let reads = 0;
	globalThis.getComputedStyle = ((...a: Parameters<typeof getComputedStyle>) => {
		reads++;
		return original?.(...a);
	}) as typeof getComputedStyle;
	try {
		const all = fakeContext();
		paintCells(all.ctx as unknown as CanvasRenderingContext2D, g, statuses, kinds, colors);
		expect(all.calls.length < 4000 * 6).toBe(true);
		expect(all.calls.length >= 4000).toBe(true);
		const dirty = fakeContext();
		paintCells(dirty.ctx as unknown as CanvasRenderingContext2D, g, statuses, kinds, colors, [5, 3999]);
		const r5 = rectOf(g, 5);
		const rLast = rectOf(g, 3999);
		expect([...dirty.touched].sort()).toEqual([`${r5.x},${r5.y}`, `${rLast.x},${rLast.y}`].sort());
		expect(reads).toBe(0);
	} finally {
		globalThis.getComputedStyle = original;
	}
});

test('every status has a CSS mark and a canvas mark, and no two share a (fill, mark) pair', () => {
	const all = Object.keys(STATUS_MARKS).map(Number) as Status[];
	expect(all.length).toBe(8);
	const pairs = new Set(all.map((s) => `${statusMark(s).fill}/${statusMark(s).shape}`));
	expect(pairs.size).toBe(8);
	const css = readFileSync(`${import.meta.dir}/../src/app.css`, 'utf8');
	// One colour for every status: the canvas marks must still differ by shape alone.
	const mono: MapColors = {
		paper: 'P',
		ink: 'I',
		focus: 'F',
		status: Object.fromEntries(all.map((s) => [s, { bg: 'B', solid: 'S' }])) as MapColors['status']
	};
	const traces = new Set<string>();
	for (const s of all) {
		const g = one(1, 16, 16);
		const fake = tracingContext();
		paintCells(fake.ctx as unknown as CanvasRenderingContext2D, g, new Uint8Array([s]), new Uint8Array([KIND_TASK]), mono);
		// A canvas mark draws more than the cell's paper.
		expect(fake.trace.length > 2).toBe(true);
		traces.add(fake.trace.join(';'));
		expect(css.includes(`.${statusMark(s).className}`)).toBe(true);
	}
	expect(traces.size).toBe(8);
});

function tracingContext() {
	const trace: string[] = [];
	const state = { fillStyle: '', strokeStyle: '', lineWidth: 1 };
	const log =
		(name: string, styled?: 'fill' | 'stroke') =>
		(...args: unknown[]) =>
			trace.push(`${name}(${args.join(',')})${styled === 'fill' ? state.fillStyle : styled === 'stroke' ? `${state.strokeStyle}/${state.lineWidth}` : ''}`);
	const ctx = Object.assign(state, {
		fillRect: log('fillRect', 'fill'),
		strokeRect: log('strokeRect', 'stroke'),
		clearRect: log('clearRect'),
		beginPath: log('beginPath'),
		moveTo: log('moveTo'),
		lineTo: log('lineTo'),
		closePath: log('closePath'),
		arc: log('arc'),
		stroke: log('stroke', 'stroke'),
		fill: log('fill', 'fill'),
		setLineDash: log('setLineDash'),
		setTransform: log('setTransform'),
		save: log('save'),
		restore: log('restore')
	});
	return { ctx, trace };
}

test('the map builds nothing on init: the model and the paint run in one idle callback, and never layoutDag', () => {
	let layered = 0;
	let painted = 0;
	let idle: (() => void) | null = null;
	const map = createTaskMap({
		layerOrder: (t) => {
			layered++;
			return { layer: new Int32Array(t.length), order: Int32Array.from(t.keys()) };
		},
		paint: () => {
			painted++;
		},
		idle: (fn) => {
			idle = fn;
			return () => {};
		}
	});
	expect(layered).toBe(0);
	expect(painted).toBe(0);
	map.schedule(() => ({ tasks: tasks(), definition: true }));
	expect(layered).toBe(0);
	expect(painted).toBe(0);
	expect(idle === null).toBe(false);
	idle!();
	expect(layered).toBe(1);
	expect(painted).toBe(1);
	expect(map.model()?.cells.length).toBe(7);
	const source = readFileSync(`${import.meta.dir}/../src/lib/taskMap.ts`, 'utf8');
	expect(/\blayoutDag\b/.test(source.replace(/\/\/.*$/gm, ''))).toBe(false);
});

test('scrollToCard converges within three passes on cards that change height, and never makes a fourth', async () => {
	// A card estimated at 7 rem that turns out taller once rendered: the first
	// scroll lands short, the second lands it.
	let top = 3000 * 112;
	let scrolls = 0;
	const env = {
		rect: () => ({ top: top - scrolled, bottom: top - scrolled + 120 }),
		scroll: () => {
			scrolls++;
			scrolled = top - 300;
			if (scrolls === 1) top += 900; // cards around the target took their real heights
		},
		viewport: () => ({ top: 44, bottom: 720 - 64 }),
		frames: async () => {}
	};
	let scrolled = 0;
	expect(await scrollToCard(env)).toBe(2);
	expect(scrolls).toBe(2);
	// A target that never settles is given up after the third pass.
	let n = 0;
	const moving = {
		rect: () => ({ top: 10_000 + n * 1000, bottom: 10_120 + n * 1000 }),
		scroll: () => {
			n++;
		},
		viewport: () => ({ top: 44, bottom: 656 }),
		frames: async () => {}
	};
	expect(await scrollToCard(moving)).toBe(3);
	expect(n).toBe(3);
	// Already in view: one look, no scroll.
	const still = { rect: () => ({ top: 100, bottom: 200 }), scroll: () => (n = -1), viewport: () => ({ top: 44, bottom: 656 }), frames: async () => {} };
	expect(await scrollToCard(still)).toBe(0);
	expect(n).toBe(3);
});
