import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { ArtifactRefSchema, PayloadSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import { artifactContentTypeLabel, artifactMetadata, downloadFilename, emptyPayload, formatDuration, inlinePayload, payloadJson, pinAccent, runCommand, shellQuote, shortRunId, taskCommands, taskRunCommand, timestampJson } from '../src/lib/runDetail';

test('copyable run commands quote every page-derived coordinate and name the namespace', () => {
	expect(shellQuote("a' b")).toBe("'a'\"'\"' b'");
	expect(runCommand('prod west', ['run', 'get', "ork-'42"])).toBe(
		"orkctl -n 'prod west' run get 'ork-'\"'\"'42'"
	);
	expect(runCommand('', ['run', 'cancel', 'ork-1'])).toContain('-n default ');
});

test('the local replay command names the namespace, the run and the task, and nothing else', () => {
	expect(taskRunCommand('', 'ork-1-2', 'publish')).toBe(
		"orkctl -n default task run --from-run ork-1-2 -t publish"
	);
	// Page data is quoted, never trusted as shell syntax.
	expect(taskRunCommand("it's", 'ork-1-2', "pub'lish")).toBe(
		"orkctl -n 'it'\"'\"'s' task run --from-run ork-1-2 -t 'pub'\"'\"'lish'"
	);
});

test('payloads and artifact metadata export values without treating storage URIs as links', () => {
	expect(payloadJson(create(PayloadSchema, { data: { answer: 42 } }))).toBe('{\n  "answer": 42\n}');
	expect(artifactMetadata(create(ArtifactRefSchema, {
		name: 'report.csv', uri: 's3://private/report.csv', contentType: 'text/csv', sizeBytes: 12n, sha256: 'abc', metadata: { source: 'task' }
	}))).toEqual({ name: 'report.csv', contentType: 'text/csv', sizeBytes: '12', sha256: 'abc', metadata: { source: 'task' } });
	expect(downloadFilename('../private\u0000report.csv')).toBe('.._private_report.csv');
});

test('one pin gets one accent, and an unpinned task gets none', () => {
	// The claim the run page rests on: members of one pin are tinted alike, so
	// "these ran together" is visible without reading each label.
	expect(pinAccent('build')).toBe(pinAccent('build'));
	// Stable across calls in any order, because it hashes the NAME and not the
	// task's position — a run streaming its tasks in must not recolour cards
	// already on screen as later pins arrive.
	const first = pinAccent('build');
	pinAccent('publish');
	expect(pinAccent('build')).toBe(first);

	// An unpinned task is not a group of one and is left untinted.
	expect(pinAccent('')).toBeUndefined();

	// Every accent is one of the declared --color-pin-* token families, so a
	// name can never compose a var() that resolves to nothing.
	const accents = ['pin-a', 'pin-b', 'pin-c', 'pin-d', 'pin-e', 'pin-f'];
	for (const name of ['build', 'publish', 'a', 'zzz', 'pin with spaces', '🍕', 'x'.repeat(500)]) {
		expect(accents).toContain(pinAccent(name));
	}

	// Two names must actually be able to land on different accents, or the
	// tint says nothing. `build` and `verify` specifically, because
	// ui/e2e/run-page-pins.spec.ts asserts on screen that those two pins are
	// drawn apart — if this pair ever collided, that spec would be testing a
	// coincidence.
	expect(pinAccent('build')).not.toBe(pinAccent('verify'));
});

test('durations only use two authoritative timestamps', () => {
	expect(formatDuration({ seconds: 10n, nanos: 0 }, { seconds: 11n, nanos: 500_000_000 })).toBe('1.5s');
	expect(formatDuration({ seconds: 11n, nanos: 0 }, { seconds: 10n, nanos: 0 })).toBe('—');
	const exported = { startedAt: timestampJson({ seconds: 10n, nanos: 0 }) };
	expect(JSON.stringify(exported)).toBe('{"startedAt":"1970-01-01T00:00:10.000Z"}');
});

test('a token is quoted only when a shell would read it as syntax', () => {
	for (const safe of ['default', 'ork-1792-6378', '-j', 'origin.kind=PARENT_RUN', 'status=FAILED,CANCELED', 'a/b:c@d%e+f', '1.2.3']) {
		expect(shellQuote(safe)).toBe(safe);
	}
	expect(shellQuote('')).toBe("''");
	for (const unsafe of ['a b', '=x', 'action^namespace.', '$(touch x)', '~/x', '*', 'tab\there', 'new\nline', '"q"', 'back\\slash']) {
		expect(shellQuote(unsafe).startsWith("'")).toBe(true);
	}
});

test('a run id is drawn short by its first four and last six characters, never parsed', () => {
	expect(shortRunId('ork-1792470266129284632-6378181302490759169')).toBe('ork-…759169');
	expect(shortRunId('ork-1-2')).toBe('ork-1-2');
	expect(shortRunId('abcdefghijklmn')).toBe('abcd…ijklmn');
});

test('a short value is said on one line, spaced like the pretty form; a long one is not', () => {
	const payload = (data: Record<string, unknown>) => create(PayloadSchema, { data: data as never });
	expect(inlinePayload(payload({ exitCode: 0 }))).toBe('{"exitCode": 0}');
	expect(inlinePayload(payload({ a: [1, 'two'], b: { c: null } }))).toBe('{"a": [1, "two"], "b": {"c": null}}');
	expect(inlinePayload(payload({ text: 'x'.repeat(80) }))).toBeNull();
	expect(inlinePayload(undefined)).toBeNull();
	expect(emptyPayload(undefined)).toBe(true);
	expect(emptyPayload(payload({}))).toBe(true);
	expect(emptyPayload(payload({ a: 1 }))).toBe(false);
});

test('a command splits into the words a shell sees, and joins back into itself', async () => {
	const { commandWords } = await import('../src/lib/runDetail');
	const cmd = "orkctl -n 'prod west' run get ork-1 -j";
	expect(commandWords(cmd)).toEqual(['orkctl', '-n', "'prod west'", 'run', 'get', 'ork-1', '-j']);
	expect(commandWords(cmd).join(' ')).toBe(cmd);
	expect(commandWords(`orkctl x --reason 'it'"'"'s ok'`)).toEqual(['orkctl', 'x', '--reason', `'it'"'"'s ok'`]);
});

test('artifactContentTypeLabel says when no content type was recorded', () => {
	expect(artifactContentTypeLabel(create(ArtifactRefSchema, { contentType: 'text/plain' }))).toBe('text/plain');
	expect(artifactContentTypeLabel(create(ArtifactRefSchema, { contentType: '' }))).toBe('no content type recorded');
});

test("a task's commands in the run's one icon name its namespace, its attempts and the earlier attempt's log", () => {
	const commands = taskCommands({ namespace: 'team-a', runId: 'ork-1', taskId: 'build', attempt: 2, canLog: true, failed: true, running: false, attempts: 2, timed: true });
	expect(commands.map((c) => c.label)).toEqual(['Its log', "The earlier attempt's log", 'Run it here, on your machine', 'Its attempts and cache key', 'Where its time went']);
	for (const c of commands) expect(c.command.startsWith('orkctl -n team-a '), c.label).toBe(true);
	const byLabel = Object.fromEntries(commands.map((c) => [c.label, c.command]));
	expect(byLabel['Its log']).toBe('orkctl -n team-a run logs ork-1 -t build --attempt 2');
	expect(byLabel["The earlier attempt's log"]).toBe('orkctl -n team-a run logs ork-1 -t build --attempt 1');
	expect(byLabel['Its attempts and cache key']).toBe('orkctl -n team-a run attempts ork-1 -t build');
	// One attempt: no earlier log. A task that never started: no log, no replay.
	expect(taskCommands({ namespace: 'team-a', runId: 'ork-1', taskId: 'build', attempt: 1, canLog: true, attempts: 1 }).map((c) => c.label)).toEqual(['Its log', 'Its attempts and cache key']);
	expect(taskCommands({ namespace: '', runId: 'ork-1', taskId: 'wait', attempt: 0, canLog: false, failed: true, attempts: 0 })).toEqual([]);
});

test("an item's commands carry --item, the earlier attempt's included", () => {
	const commands = taskCommands({ namespace: 'team-a', runId: 'ork-1', taskId: 'render', item: 3, attempt: 2, canLog: true, attempts: 2 });
	expect(commands.map((c) => c.label)).toEqual(["This item's logs", "The earlier attempt's log", "This item's artifacts", "This item's attempts"]);
	for (const c of commands) {
		expect(c.command.startsWith('orkctl -n team-a run '), c.label).toBe(true);
		expect(c.command, c.label).toContain(' -t render --item 3');
	}
	expect(commands[1].command).toBe('orkctl -n team-a run logs ork-1 -t render --item 3 --attempt 1');
});

test("an item's commands quote every argument: a quote in a task id cannot become shell syntax", () => {
	for (const c of taskCommands({ namespace: 'data', runId: 'ork-1', taskId: "x'; rm -rf / #", item: 1, attempt: 2, canLog: true, attempts: 2 })) {
		expect(c.command, c.label).toContain(`'x'"'"'; rm -rf / #'`);
		expect(c.command, c.label).not.toContain("'x'; rm");
	}
});
