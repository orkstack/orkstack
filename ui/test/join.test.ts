import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { JoinPolicy } from '../src/lib/gen/orkstack/v1/activity_pb';
import { ErrorSchema, Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { EventSchema, EventType } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	failingBannerText,
	isFailingNote,
	isFailureHandler,
	joinTitle,
	joinWord,
	outstandingHandlers,
	runFailing
} from '../src/lib/join';

test('joinWord spells the DSL word, all_succeeded for UNSPECIFIED', () => {
	expect(joinWord(JoinPolicy.UNSPECIFIED)).toBe('all_succeeded');
	expect(joinWord(undefined)).toBe('all_succeeded');
	expect(joinWord(JoinPolicy.NONE_FAILED)).toBe('none_failed');
	expect(joinWord(JoinPolicy.ANY_FAILED)).toBe('any_failed');
	expect(joinWord(JoinPolicy.ALL_DONE)).toBe('all_done');
	expect(isFailureHandler(JoinPolicy.ANY_FAILED)).toBe(true);
	expect(isFailureHandler(JoinPolicy.ALL_DONE)).toBe(true);
	expect(isFailureHandler(JoinPolicy.NONE_FAILED)).toBe(false);
	expect(isFailureHandler(undefined)).toBe(false);
});

test('joinTitle says when the task starts, with its needs', () => {
	expect(joinTitle(JoinPolicy.NONE_FAILED, ['canary', 'full'])).toBe(
		'Starts when every need is terminal and none failed or was canceled — branches that were skipped are tolerated (needs: canary, full)'
	);
	expect(joinTitle(JoinPolicy.ANY_FAILED)).toBe('Starts only if one of its needs fails — a failure handler');
	expect(joinTitle(JoinPolicy.ALL_DONE, ['deploy'])).toBe(
		'Starts once every need is terminal, whatever it did — a failure handler (needs: deploy)'
	);
});

test('isFailingNote is true only for a task-less, item-less TASK_LOG', () => {
	const note = create(EventSchema, { type: EventType.TASK_LOG, status: Status.RUNNING, message: 'run failing after task "deploy" failed; waiting for its failure handlers: rollback' });
	expect(isFailingNote(note)).toBe(true);
	expect(isFailingNote(create(EventSchema, { type: EventType.TASK_LOG, taskId: 'verify', status: Status.SKIPPED }))).toBe(false);
	expect(isFailingNote(create(EventSchema, { type: EventType.TASK_LOG, taskId: 'convert', item: 3 }))).toBe(false);
	expect(isFailingNote(create(EventSchema, { type: EventType.TASK_LOG, item: 0 }))).toBe(false);
	for (const type of [EventType.RUN_FAILED, EventType.RUN_STARTED, EventType.TASK_FAILED, EventType.RUN_CANCELED]) {
		expect(isFailingNote(create(EventSchema, { type }))).toBe(false);
	}
});

test('runFailing reads the LIVE status, the snapshot error and the note', () => {
	const error = create(ErrorSchema, { code: 'RUN_FAILED', message: 'deploy exploded' });
	expect(runFailing(Status.RUNNING, error, '')).toBe(true);
	expect(runFailing(Status.RUNNING, undefined, 'run failing after task "deploy" failed')).toBe(true);
	expect(runFailing(Status.RUNNING, undefined, '')).toBe(false);
	expect(runFailing(Status.RUNNING, create(ErrorSchema, {}), '')).toBe(false);
	// A terminal status is never failing — a terminated failing run reads
	// CANCELED with its error kept.
	for (const status of [Status.FAILED, Status.CANCELED, Status.SUCCEEDED]) {
		expect(runFailing(status, error, 'note')).toBe(false);
	}
});

test('outstandingHandlers follows the live task views and prefers the definition', () => {
	const defs: Record<string, JoinPolicy> = { rollback: JoinPolicy.ANY_FAILED, notify: JoinPolicy.ALL_DONE };
	const snapshot: Record<string, JoinPolicy> = { escalate: JoinPolicy.ANY_FAILED, notify: JoinPolicy.NONE_FAILED };
	const joinOf = (id: string) => defs[id] || snapshot[id];
	const tasks = [
		{ id: 'deploy', status: Status.FAILED },
		{ id: 'rollback', status: Status.AWAITING },
		{ id: 'escalate', status: Status.PENDING },
		{ id: 'notify', status: Status.RUNNING }
	];
	expect(outstandingHandlers(tasks, joinOf)).toEqual([
		{ id: 'rollback', word: 'awaiting' },
		{ id: 'escalate', word: 'pending' },
		{ id: 'notify', word: 'running' }
	]);
	tasks[3].status = Status.SUCCEEDED;
	expect(outstandingHandlers(tasks, joinOf).map((h) => h.id)).toEqual(['rollback', 'escalate']);
	expect(failingBannerText(outstandingHandlers(tasks, joinOf))).toBe(
		'Failing — the run ends FAILED once its failure handlers finish: rollback (awaiting), escalate (pending). Cancel stops them; the run still ends FAILED.'
	);
	expect(failingBannerText([])).toBe('Failing — finishing.');
});
