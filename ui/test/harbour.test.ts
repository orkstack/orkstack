import { docLong, docParts } from '../src/lib/docText';
import { forgetActivities, loadActivities, type ActivityReads } from '../src/lib/activityMeta';
import type { ActivityInfo } from '../src/lib/gen/orkstack/v1/activity_service_pb';
import { JoinPolicy } from '../src/lib/gen/orkstack/v1/activity_pb';
import { taskTimelineBars, timelineAxis } from '../src/lib/taskTimeline';
import { phaseGroups } from '../src/lib/timing';
import { fmtQuantity, reservedOf } from '../src/lib/resources';
import { DAG_NODE_H, DAG_NODE_W, layoutDag } from '../src/lib/dag';
import { SIDE_COL, buildModel, cellAt, fillStatuses, mapGeometry, mapSectionsOf, rectOf } from '../src/lib/taskMap';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { humanFailure } from '../src/lib/failureText';
import { applyLiveItems, eachCounts } from '../src/lib/each';
import { expect, test } from 'bun:test';
import { runProgress, progressWords } from '../src/lib/progress';
import { placeTip, shouldDescribe, TIP_EDGE, TIP_GAP } from '../src/lib/tooltip';
import { activityKind, safeIcon, type IconNode } from '../src/lib/activityIcon';
import { relativeTime } from '../src/lib/when';
import { taskAt } from '../src/lib/yaml/taskAt';

// Harbour's pure rules: what a run's progress counts, where the one tooltip
// goes and when it joins a description, which declared icons are drawn, the
// task the editor's caret is in, and how a relative time reads. Each test
// names the regression it catches.

const t = (status: Status, items: Status[] = [], count = 0) => ({
	status,
	items: items.map((s) => ({ status: s })),
	each: count ? { expanded: true, count } : undefined
});

test('progress counts ended tasks plus fan-out items over all of them (14 of 30 is 47%)', () => {
	// Regression: a fan-out counted as one unit, so 24 running items read as one task.
	const tasks = [
		...Array.from({ length: 5 }, () => t(Status.SUCCEEDED)),
		t(Status.PENDING),
		t(Status.RUNNING, [...Array(9).fill(Status.SUCCEEDED), ...Array(15).fill(Status.RUNNING)])
	];
	const p = runProgress(tasks);
	expect([p.ended, p.total, p.percent, p.what]).toEqual([14, 30, 47, 'tasks and items']);
	expect(progressWords(p)).toBe('14 of 30 tasks and items ended');
});

test('a failed or canceled run reads 100%: skipped and canceled tasks have ended', () => {
	// Regression: a failed run stuck below 100% because skipped tasks never "finished".
	const failed = runProgress([...Array(8).fill(t(Status.SUCCEEDED)), t(Status.FAILED), t(Status.SKIPPED), t(Status.SKIPPED)]);
	expect([failed.percent, failed.ran, failed.skipped]).toEqual([100, 9, 2]);
	expect(runProgress([t(Status.SUCCEEDED), t(Status.CANCELED), t(Status.CANCELED)]).percent).toBe(100);
});

test('a listed fan-out with no items counts its items as ended exactly when the task has', () => {
	// Regression: a runs-list row (no items on the wire) read 100% for a fan-out still going.
	expect(runProgress([t(Status.SUCCEEDED), t(Status.RUNNING, [], 4)]).percent).toBe(20);
	expect(runProgress([t(Status.SUCCEEDED), t(Status.SUCCEEDED, [], 4)]).percent).toBe(100);
});

const view = { width: 1000, height: 800 };

test('a tip sits 8 px below its control, centred, and never covers it', () => {
	const p = placeTip({ left: 400, top: 100, width: 100, height: 30 }, { width: 200, height: 40 }, view);
	expect(p).toEqual({ left: 350, top: 130 + TIP_GAP, up: false });
});

test('a tip with no room below flips above its control', () => {
	// Regression: a tip near the bottom edge was pushed off screen or over its control.
	const p = placeTip({ left: 400, top: 760, width: 100, height: 30 }, { width: 200, height: 40 }, view);
	expect(p.up).toBe(true);
	expect(p.top + 40).toBe(760 - TIP_GAP);
});

test('a tip is clamped 12 px inside the viewport on either side', () => {
	expect(placeTip({ left: 0, top: 10, width: 20, height: 20 }, { width: 300, height: 40 }, view).left).toBe(TIP_EDGE);
	expect(placeTip({ left: 990, top: 10, width: 10, height: 20 }, { width: 300, height: 40 }, view).left).toBe(1000 - TIP_EDGE - 300);
});

test('a tip joins the description only when the control does not already say its words', () => {
	// Regression: "Copy run id, Copy run id" — the name and the tip read twice.
	expect(shouldDescribe('Copy run id', { name: 'Copy run id' })).toBe(false);
	expect(shouldDescribe('Copied', { name: 'copy run id', text: '' })).toBe(true);
	expect(shouldDescribe('Pause live updates', { text: '  pause   live updates to this page ' })).toBe(false);
	expect(shouldDescribe('Asserted by your gateway', { description: 'asserted by your gateway, not verified' })).toBe(false);
	expect(shouldDescribe('', { name: 'x' })).toBe(false);
});

const el = (name: string, attrs: [string, string][], children: IconNode[] = []): IconNode => ({
	name,
	attributes: attrs.map(([n, v]) => ({ name: n, value: v })),
	children
});

test('a declared icon inside the allowed set renders as its own elements', () => {
	const icon = safeIcon({ attributes: [{ name: 'fill', value: 'none' }], elements: [el('g', [['transform', 'translate(1 1)']], [el('circle', [['cx', '8'], ['cy', '8'], ['r', '6'], ['stroke', 'currentColor']])])] });
	expect(icon?.attrs).toEqual({ fill: 'none' });
	expect(icon?.nodes[0].tag).toBe('g');
	expect(icon?.nodes[0].children[0]).toEqual({ tag: 'circle', attrs: { cx: '8', cy: '8', r: '6', stroke: 'currentColor' }, children: [] });
});

test('anything outside the allowed set draws the default icon, never a partial one', () => {
	// Regression: an icon with one bad attribute rendered its other elements.
	const good = el('path', [['d', 'M0 0h4']]);
	const cases: IconNode[][] = [
		[good, el('script', [])],
		[good, el('path', [['onclick', 'x()']])],
		[good, el('path', [['fill', '#f00']])],
		[good, el('path', [['style', 'fill:red']])],
		[el('circle', [['d', 'M0 0']])],
		[el('path', [['d', 'M0 0']], [good])],
		[el('g', [], [el('g', [], [el('g', [], [el('g', [], [el('g', [], [good])])])])])]
	];
	for (const elements of cases) expect(safeIcon({ attributes: [], elements })).toBeNull();
	expect(safeIcon({ attributes: [{ name: 'href', value: 'x' }], elements: [good] })).toBeNull();
	expect(safeIcon({ attributes: [], elements: Array.from({ length: 33 }, () => good) })).toBeNull();
	expect(safeIcon({ attributes: [], elements: [] })).toBeNull();
	expect(safeIcon(undefined)).toBeNull();
});

test("the UI's own drawings: composed and awaited tasks; any image is the default until it declares its own", () => {
	// Regression: a first-party name must not earn a drawing the image did not declare.
	expect(activityKind('http')).toBe('default');
	expect(activityKind('json@sha256:abc')).toBe('default');
	expect(activityKind('orkflow:release-block@0.1.0')).toBe('orkflow');
	expect(activityKind('shell', { awaited: true })).toBe('await');
	expect(activityKind('ghcr.io/acme/migrate:1.2')).toBe('default');
	expect(activityKind(undefined)).toBe('default');
});

test('a relative time reads in the coarsest unit that still says it', () => {
	const now = Date.UTC(2026, 10, 1, 12, 0, 0);
	const ago = (ms: number) => relativeTime(new Date(now - ms), now);
	expect(ago(20_000)).toBe('just now');
	expect(ago(2 * 60_000)).toBe('2 min ago');
	expect(ago(3 * 3_600_000)).toBe('3 h ago');
	expect(ago(26 * 3_600_000)).toBe('yesterday');
	expect(ago(3 * 86_400_000)).toBe('3 days ago');
	// A clock a little ahead never reads as the future.
	expect(ago(-5_000)).toBe('just now');
});

const DOC = `apiVersion: orkstack/v1
kind: Orkflow
name: demo
version: "0.1.0"
tasks:
  - id: fetch
    uses: http
    with:
      op: get
      url: https://example.test
  - id: render
    uses: template
    needs: [fetch]
    with:
      template: hi
`;

test('the task under the caret: its id, uses, op and the with: keys it sets', () => {
	// Regression: the docs rail showed the previous task once the caret moved on.
	const inRender = DOC.indexOf('template: hi');
	expect(taskAt(DOC, inRender)).toEqual({ id: 'render', uses: 'template', op: '', set: ['template'], line: 11 });
	expect(taskAt(DOC, DOC.indexOf('url:'))).toEqual({ id: 'fetch', uses: 'http', op: 'get', set: ['op', 'url'], line: 6 });
	expect(taskAt(DOC, DOC.indexOf('name: demo'))).toBeNull();
	expect(taskAt('tasks: [', 3)).toBeNull();
});

test('a description is plain text: only a paired backtick is code', () => {
	// Regression: an unmatched backtick must stay a character, not swallow the rest as code.
	expect(docParts('takes `src` and `as`')).toEqual([
		{ text: 'takes ', code: false },
		{ text: 'src', code: true },
		{ text: ' and ', code: false },
		{ text: 'as', code: true }
	]);
	expect(docParts('a `lone tick')).toEqual([
		{ text: 'a ', code: false },
		{ text: '`lone tick', code: false }
	]);
	expect(docParts('<b>not html</b>')).toEqual([{ text: '<b>not html</b>', code: false }]);
	expect(docLong('é'.repeat(160), 160)).toBe(false);
	expect(docLong('é'.repeat(161), 160)).toBe(true);
});

test('one read per image per session: the catalog first, then each pin it does not hold', async () => {
	forgetActivities();
	let lists = 0;
	const inspected: string[] = [];
	const info = (name: string, indexDigest: string) => ({ name, indexDigest }) as unknown as ActivityInfo;
	const reads: ActivityReads = {
		listActivities: async () => {
			lists++;
			return { activities: [info('http', 'sha256:h')] };
		},
		inspectActivity: async ({ reference, allowCached }) => {
			expect(allowCached).toBe(true);
			inspected.push(reference);
			return { activity: info('', reference) };
		}
	};
	const tasks = [
		{ id: 'a', uses: 'http', image: { pinned: 'r/http@sha256:h', indexDigest: 'sha256:h' } },
		{ id: 'b', uses: 'r/x', image: { pinned: 'r/x@sha256:x', indexDigest: 'sha256:x' } },
		{ id: 'c', uses: 'r/x', image: { pinned: 'r/x@sha256:x', indexDigest: 'sha256:x' } },
		{ id: 'd', uses: 'http' },
		{ id: 'e', uses: 'orkflow:child@1', orkflow: {} },
		{ id: 'f', uses: '', await: {} }
	];
	const first = await loadActivities(tasks, reads);
	expect([...first.keys()].sort()).toEqual(['a', 'b', 'c', 'd']);
	expect(inspected).toEqual(['r/x@sha256:x']);
	await loadActivities(tasks, reads);
	expect(lists).toBe(1);
	expect(inspected).toEqual(['r/x@sha256:x']);
});

test('a read that fails draws the default and is asked again', async () => {
	forgetActivities();
	let inspects = 0;
	const reads: ActivityReads = {
		listActivities: async () => ({ activities: [] }),
		inspectActivity: async () => {
			inspects++;
			throw new Error('refused');
		}
	};
	const tasks = [{ id: 'b', uses: 'r/x', image: { pinned: 'r/x@sha256:x', indexDigest: 'sha256:x' } }];
	expect((await loadActivities(tasks, reads)).size).toBe(0);
	await loadActivities(tasks, reads);
	expect(inspects).toBe(2);
});

test("a task's bars: its wait, its failed attempts, what ran, all on the run's axis", () => {
	const run = { start: 0, end: 10_000 };
	const plain = taskTimelineBars({ run, scheduled: 1000, started: 2000, finished: 4000, running: false, now: 10_000, attempts: [] });
	expect(plain.bars).toEqual([
		{ kind: 'wait', left: 0.1, width: 0.1 },
		{ kind: 'ran', left: 0.2, width: 0.2 }
	]);
	expect(plain.words).toBe('waited 1.0s, ran 2.0s');
	// Regression: a retried task must show the attempt that failed, not one long bar.
	const retried = taskTimelineBars({
		run,
		scheduled: 0,
		started: 3000,
		finished: 5000,
		running: false,
		now: 10_000,
		attempts: [
			{ attempt: 1, failed: true, started: 0, finished: 1000 },
			{ attempt: 2, failed: false, started: 3000, finished: 5000 }
		]
	});
	expect(retried.bars.map((b) => b.kind)).toEqual(['failed', 'wait', 'ran']);
	// A dispatched task no Orker has started is all waiting, never drawn as running.
	// Regression: a 30 ms wait drew a 4 px head on every bar of a 25 s run.
	const span = { start: 0, end: 25_000 };
	const quick = taskTimelineBars({ run: span, scheduled: 1000, started: 1030, finished: 5000, running: false, now: 25_000, attempts: [] });
	expect(quick.bars.map((bar) => bar.kind)).toEqual(['ran']);
	expect(quick.words).toContain('waited');
	// Regression: a wait of half a percent of the run, which the design draws, had no head and no key.
	const seen = taskTimelineBars({ run: span, scheduled: 1000, started: 1125, finished: 5000, running: false, now: 25_000, attempts: [] });
	expect(seen.bars.map((bar) => bar.kind)).toEqual(['wait', 'ran']);
	// Regression: a running fan-out drew one blue bar over items that had finished.
	const fan = taskTimelineBars({ run: span, scheduled: 1000, started: 1000, finished: null, running: true, now: 20_000, attempts: [], doneUntil: 12_000 });
	expect(fan.bars.map((bar) => bar.kind)).toEqual(['ran', 'running']);
	expect(fan.bars[1].left).toBe(12_000 / 25_000);
	const none = taskTimelineBars({ run: span, scheduled: 1000, started: 1000, finished: null, running: true, now: 20_000, attempts: [], doneUntil: null });
	expect(none.bars.map((bar) => bar.kind)).toEqual(['running']);
	const waiting = taskTimelineBars({ run, scheduled: 1000, started: null, finished: null, running: true, now: 6000, attempts: [] });
	expect(waiting.bars).toEqual([{ kind: 'wait', left: 0.1, width: 0.5 }]);
	// A run with no span yet draws nothing.
	expect(taskTimelineBars({ run: { start: null, end: null }, scheduled: 1, started: 2, finished: 3, running: false, now: 4, attempts: [] }).bars).toEqual([]);
});

test('the Tasks axis picks round marks that fit its span', () => {
	expect(timelineAxis(0)).toEqual([]);
	expect(timelineAxis(7700).map((m) => m.label)).toEqual(['0s', '2s', '4s', '6s']);
	expect(timelineAxis(252_000).map((m) => m.label)).toEqual(['0s', '1m', '2m', '3m', '4m']);
});

test('a task or item phases gather round the run', () => {
	const value = (ms: number) => ({ kind: 'value' as const, from: { seconds: 0n, nanos: 0 }, to: { seconds: 0n, nanos: 0 }, ms });
	const groups = phaseGroups([
		{ phase: 'image', duration: value(100) },
		{ phase: 'create', duration: value(200) },
		{ phase: 'run', duration: value(1000) },
		{ phase: 'commit', duration: value(50) }
	]);
	expect(groups.map((g) => [g.key, g.ms])).toEqual([['setup', 300], ['run', 1000], ['finish', 50]]);
	expect(groups[0].phases).toEqual(['image', 'create']);
});

test('a resource amount in the run page words', () => {
	expect(fmtQuantity({ cpu: 1, memoryBytes: 2n * 2n ** 30n })).toBe('1 cpu · 2 GiB');
	expect(fmtQuantity({ cpu: 0, memoryBytes: 0n })).toBe('');
	expect(reservedOf({ cpu: 12, memoryBytes: 0n } as never, { cpu: 12, memoryBytes: 0n } as never)).toBe('12 of 12 cpu');
	expect(reservedOf(undefined, undefined)).toBe('');
});

test('failure handlers keep to one row of their own, under the tasks they follow', () => {
	// Regression: handlers stacked under one another in the main drawing's layer.
	const task = (id: string, needs: string[], join = 0) => ({ id, needs, join }) as never;
	const layout = layoutDag([
		task('a', []),
		task('b', ['a']),
		task('c', ['b']),
		task('release', ['b'], JoinPolicy.ANY_FAILED),
		task('refund', ['b'], JoinPolicy.ANY_FAILED),
		task('report', ['c', 'release'], JoinPolicy.ALL_DONE)
	]);
	const at = (id: string) => layout.nodes.find((n) => n.task.id === id)!;
	expect(at('release').y).toBe(at('refund').y);
	expect(at('refund').y).toBe(at('report').y);
	expect(at('release').y > at('c').y).toBe(true);
	expect(at('release').x < at('refund').x && at('refund').x < at('report').x).toBe(true);
	// The first ones stand beneath the task that sets them off.
	expect(at('refund').x).toBe(at('b').x);
	// A drop from its need's bottom edge into the handler's top.
	const drop = layout.edges.find((e) => e.toId === 'refund')!;
	expect([drop.x1, drop.y1]).toEqual([at('b').x + DAG_NODE_W / 2, at('b').y + DAG_NODE_H]);
	expect([drop.x2, drop.y2]).toEqual([at('refund').x + DAG_NODE_W / 2, at('refund').y]);
	// A plain task that needs a handler keeps the layered drawing.
	const mixed = layoutDag([task('a', []), task('h', ['a'], JoinPolicy.ANY_FAILED), task('after', ['h'])]);
	expect(mixed.nodes.find((n) => n.task.id === 'after')!.x > mixed.nodes.find((n) => n.task.id === 'h')!.x).toBe(true);
});

test("a small run's task map reads row by row: a stage's tasks on one row, a fan-out's items on their own", () => {
	const tasks = [
		{ id: 'a', status: Status.SUCCEEDED, needs: [] },
		{ id: 'b', status: Status.SUCCEEDED, needs: ['a'] },
		{ id: 'c', status: Status.SUCCEEDED, needs: ['a'] },
		{ id: 'fan', status: Status.RUNNING, needs: ['b'], items: [{ index: 0, status: Status.RUNNING }, { index: 1, status: Status.PENDING }] }
	];
	const model = buildModel(tasks, true, undefined, true);
	expect(model.segments.map((s) => s.label)).toEqual(['a', 'b, c', 'fan']);
	expect(model.indexOf('fan')).toBe(-1);
	expect(model.indexOf('fan', 1)).toBe(4);
	// What sizes the box agrees with what is drawn.
	const sections = mapSectionsOf(tasks.map((t) => ({ id: t.id, needs: t.needs, itemCount: t.items?.length ?? 0 })), true);
	expect(sections.map((s) => s.count)).toEqual(model.segments.map((s) => s.count));
	const g = mapGeometry(sections, 300, 300);
	expect(rectOf(g, 0).x).toBe(SIDE_COL);
	expect(cellAt(g, rectOf(g, 2).x + 1, rectOf(g, 2).y + 1)).toBe(2);
	expect(cellAt(g, 10, rectOf(g, 0).y + 1)).toBe(-1);
	const statuses = new Uint8Array(model.cells.length);
	fillStatuses(model, tasks, statuses);
	expect(statuses[3]).toBe(Status.RUNNING);
});

test('a dispatched item reads as running before any event says so', () => {
	// Regression: items stayed Pending under a live attempt, and their log said no attempt yet.
	const items = [
		{ index: 0, status: Status.PENDING, attempt: 0 },
		{ index: 1, status: Status.SUCCEEDED, attempt: 1 },
		{ index: 2, status: Status.PENDING, attempt: 0 }
	] as never as Parameters<typeof applyLiveItems>[0];
	const moved = applyLiveItems(items, [{ item: 0, attempt: 2 }, { item: 1, attempt: 1 }]);
	expect(moved.map((i) => i.status)).toEqual([Status.RUNNING, Status.SUCCEEDED, Status.PENDING]);
	expect(moved[0].attempt).toBe(2);
	// Nothing moved: the same array, so a poll does not touch the page.
	expect(applyLiveItems(moved, [{ item: 0, attempt: 2 }])).toBe(moved);
	expect(applyLiveItems(items, [{ attempt: 1 }])).toBe(items);
});

test('an exit in the words of a page, and a grid line of counts', () => {
	expect(humanFailure('task "charge": the container exited 1')).toBe('The container of charge exited 1');
	expect(humanFailure('item 3 of 6 failed: task "convert": the container exited 2')).toBe('Item 3 of 6 failed: the container of convert exited 2');
	expect(humanFailure('image pull failed')).toBe('Image pull failed');
	const items = [
		{ index: 0, status: Status.SUCCEEDED, attempt: 1 },
		{ index: 1, status: Status.RUNNING, attempt: 1 },
		{ index: 2, status: Status.RUNNING, attempt: 1 },
		{ index: 3, status: Status.PENDING, attempt: 0 }
	] as never;
	expect(eachCounts(items, (i) => i === 2)).toBe('1 succeeded · 1 running · 1 waiting for capacity · 1 pending');
});
