import { expect, test } from 'bun:test';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
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

test("the UI's own drawings: first-party activities by name, composed and awaited tasks, else the default", () => {
	expect(activityKind('http')).toBe('http');
	expect(activityKind('json@sha256:abc')).toBe('json');
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
