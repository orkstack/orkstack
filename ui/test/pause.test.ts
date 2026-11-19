import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { JoinPolicy } from '../src/lib/gen/orkstack/v1/activity_pb';
import { InheritedPauseSchema, PauseEnd, RunPauseSchema, RunSchema, TaskRunSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import { ErrorSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import { endsAfterChoices, howLastPauseEnded, pauseControls, pausedChildCount, pausedLine } from '../src/lib/pause';

const allowed = { allowed: true };

test('a running, pausable run offers Pause run, and no Unpause', () => {
	const run = create(RunSchema, { pausable: true, status: Status.RUNNING });
	expect(pauseControls(run, Status.RUNNING, allowed)).toEqual({
		pause: { shown: true, disabled: '' },
		unpause: { shown: false, disabled: '' }
	});
});

test('each disabled state says why, in the design\'s words', () => {
	const cases: Array<[string, ReturnType<typeof create<typeof RunSchema>>, Status, string]> = [
		['older release', create(RunSchema, { pausable: false }), Status.RUNNING, 'Started before pausing was available'],
		['not started', create(RunSchema, { pausable: true }), Status.PENDING, 'Not started yet'],
		['queued', create(RunSchema, { pausable: true, queued: {} }), Status.PENDING, 'Waiting for its concurrency slot'],
		[
			'failing with every handler started',
			create(RunSchema, { pausable: true, error: create(ErrorSchema, { code: 'X' }),
				taskRuns: [create(TaskRunSchema, { join: JoinPolicy.ANY_FAILED, status: Status.RUNNING })] }),
			Status.RUNNING, 'Failing; its failure handlers have all started'
		]
	];
	for (const [name, run, status, why] of cases) {
		expect({ name, why: pauseControls(run, status, allowed).pause.disabled }).toEqual({ name, why });
	}
	const failing = create(RunSchema, { pausable: true, error: create(ErrorSchema, { code: 'X' }),
		taskRuns: [create(TaskRunSchema, { join: JoinPolicy.ALL_DONE, status: Status.PENDING })] });
	expect(pauseControls(failing, Status.RUNNING, allowed).pause.disabled).toBe('');
	expect(pauseControls(create(RunSchema, { pausable: true }), Status.RUNNING, { allowed: true, inFlight: 'pause' }).pause.disabled).toBe('Pausing…');
});

test('a paused run offers Unpause; a run paused by its parent links the parent and offers neither', () => {
	const own = create(RunSchema, { pausable: true, pause: create(RunPauseSchema, { pausedBy: 'alice' }) });
	expect(pauseControls(own, Status.PAUSED, allowed)).toEqual({ pause: { shown: false, disabled: '' }, unpause: { shown: true, disabled: '' } });
	expect(pauseControls(own, Status.PAUSED, { allowed: true, inFlight: 'unpause' }).unpause.disabled).toBe('Unpausing…');
	const inherited = create(RunSchema, { pausable: true, pausedByParent: create(InheritedPauseSchema, { parentRunId: 'ork-p' }) });
	expect(pauseControls(inherited, Status.PAUSED, allowed)).toEqual({
		pause: { shown: false, disabled: '' },
		unpause: { shown: true, disabled: 'Paused because parent run ork-p is paused', parentRunId: 'ork-p' }
	});
	const both = create(RunSchema, { pausable: true, pause: create(RunPauseSchema, {}), pausedByParent: create(InheritedPauseSchema, { parentRunId: 'ork-p' }) });
	expect(pauseControls(both, Status.PAUSED, allowed).unpause.disabled).toBe('Paused because parent run ork-p is paused');
	// The live status decides: a snapshot still carrying a pause on a running run offers Pause, not Unpause.
	expect(pauseControls(own, Status.RUNNING, allowed).unpause.shown).toBe(false);
	expect(pauseControls(own, Status.RUNNING, allowed).pause).toEqual({ shown: true, disabled: '' });
	// A PAUSED event ahead of its snapshot holds Unpause until the snapshot says by whom.
	expect(pauseControls(create(RunSchema, { pausable: true }), Status.PAUSED, allowed).unpause.disabled).toBe('Reading the pause…');
});

test('an ended run, or a caller without run.pause, sees neither control', () => {
	const run = create(RunSchema, { pausable: true });
	for (const status of [Status.SUCCEEDED, Status.FAILED, Status.CANCELED]) {
		const c = pauseControls(run, status, allowed);
		expect(c.pause.shown || c.unpause.shown).toBe(false);
	}
	expect(pauseControls(run, Status.RUNNING, { allowed: false }).pause.shown).toBe(false);
});

test('Ends after starts at the default and goes up to 31 days', () => {
	const choices = endsAfterChoices();
	expect(choices[0].seconds).toBeNull();
	expect(choices.at(-1)?.seconds).toBe(31 * 86_400);
	expect(new Set(choices.map((c) => c.label)).size).toBe(choices.length);
});

test('the Paused card\'s words', () => {
	expect(pausedLine(2, false)).toBe('2 tasks still running finish first');
	expect(pausedLine(1, true)).toBe('1 task still running finishes first; its failure handlers wait until it is unpaused');
	expect(pausedLine(0, false)).toBe('Nothing is running; nothing new starts');
	expect(pausedChildCount([{ childRunId: 'ork-c', status: Status.PAUSED }, { childRunId: 'ork-d', status: Status.SUCCEEDED }, { status: Status.RUNNING }])).toBe(1);
	expect(howLastPauseEnded(PauseEnd.TIMED_OUT)).toBe('Its pause timed out');
	expect(howLastPauseEnded(PauseEnd.UNPAUSED)).toBe('Unpaused');
});
