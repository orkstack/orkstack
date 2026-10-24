import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { ArtifactRefSchema, PayloadSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import { artifactMetadata, downloadFilename, emptyPayload, formatDuration, inlinePayload, payloadJson, pinAccent, runCommand, shellQuote, shortRunId, taskRunCommand, timestampJson } from '../src/lib/runDetail';

test('copyable run commands quote every page-derived coordinate and name the namespace', () => {
	expect(shellQuote("a' b")).toBe("'a'\"'\"' b'");
	expect(runCommand('prod west', ['run', 'get', "ork-'42"])).toBe(
		"orkctl --namespace 'prod west' run get 'ork-'\"'\"'42'"
	);
	expect(runCommand('', ['run', 'cancel', 'ork-1'])).toContain('--namespace default ');
});

test('the local replay command names the namespace, the run and the task, and nothing else', () => {
	expect(taskRunCommand('', 'ork-1-2', 'publish')).toBe(
		"orkctl --namespace default task run --from-run ork-1-2 --task publish"
	);
	// Page data is quoted, never trusted as shell syntax.
	expect(taskRunCommand("it's", 'ork-1-2', "pub'lish")).toBe(
		"orkctl --namespace 'it'\"'\"'s' task run --from-run ork-1-2 --task 'pub'\"'\"'lish'"
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
	for (const safe of ['default', 'ork-1792-6378', '--json', 'origin.kind=PARENT_RUN', 'status=FAILED,CANCELED', 'a/b:c@d%e+f', '1.2.3']) {
		expect(shellQuote(safe)).toBe(safe);
	}
	expect(shellQuote('')).toBe("''");
	for (const unsafe of ['a b', 'action^namespace.', '$(touch x)', '~/x', '*', 'tab\there', 'new\nline', '"q"', 'back\\slash']) {
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
