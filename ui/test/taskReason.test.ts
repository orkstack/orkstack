import { expect, test } from 'bun:test';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { reasonWord } from '../src/lib/taskReason';

test('reasonWord gives a row a word for each sentence the engine writes', () => {
	const cases: [Status, string, string][] = [
		[Status.SKIPPED, 'task skipped: condition "{{ and (ne .tasks.decide.output.result.route \\"hold\\") .tasks.decide.output.result.notify }}" evaluated false', 'condition false'],
		[Status.SKIPPED, 'task skipped: upstream task "deploy" was skipped', 'need skipped'],
		[Status.SKIPPED, 'task skipped: join none_failed: every need was skipped (a, b)', 'every need skipped'],
		[Status.SKIPPED, 'task skipped: join none_failed: need "a" did not succeed (FAILED)', 'a need did not succeed'],
		[Status.SKIPPED, 'task skipped: join any_failed: none of its needs failed (deploy); it runs only when one of them fails', 'no need failed'],
		[Status.CANCELED, 'task canceled: the run is failing (task "deploy" failed) and this task is not a failure handler', 'run failing'],
		[Status.CANCELED, 'task canceled: the run was canceled; a canceled run starts no failure handler', 'run canceled'],
		[Status.CANCELED, "task canceled: the run ended before this task's needs all finished", 'run ended first'],
		[Status.FAILED, 'task failed: run ended with 3 of 40 items unfinished (1 were running, 2 never started)', '3 of 40 items unfinished'],
		[Status.FAILED, 'await rejected by ops: not during the freeze', 'rejected']
	];
	for (const [status, message, word] of cases) expect(reasonWord(status, message)).toBe(word);
});

test('reasonWord never prints a long message on a row, and adds nothing the pill says', () => {
	expect(reasonWord(Status.CANCELED, `task canceled: ${'an operator wrote a very long reason '.repeat(5)}`)).toBe('');
	expect(reasonWord(Status.SKIPPED, 'task skipped: a sentence from a newer engine')).toBe('');
	expect(reasonWord(Status.RUNNING, 'x'.repeat(41))).toBe('');
	// A short note of a live task reads as itself.
	expect(reasonWord(Status.AWAITING, 'waiting for approval')).toBe('waiting for approval');
	expect(reasonWord(Status.SUCCEEDED, '')).toBe('');
});
