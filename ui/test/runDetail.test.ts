import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { ArtifactRefSchema, PayloadSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import { artifactMetadata, downloadFilename, formatDuration, payloadJson, runCommand, shellQuote, timestampJson } from '../src/lib/runDetail';

test('copyable run commands quote every page-derived coordinate and name the namespace', () => {
	expect(shellQuote("a' b")).toBe("'a'\"'\"' b'");
	expect(runCommand('prod west', ['run', 'get', "ork-'42"])).toBe(
		"orkctl --namespace 'prod west' 'run' 'get' 'ork-'\"'\"'42'"
	);
	expect(runCommand('', ['run', 'cancel', 'ork-1'])).toContain("--namespace 'default'");
});

test('payloads and artifact metadata export values without treating storage URIs as links', () => {
	expect(payloadJson(create(PayloadSchema, { data: { answer: 42 } }))).toBe('{\n  "answer": 42\n}');
	expect(artifactMetadata(create(ArtifactRefSchema, {
		name: 'report.csv', uri: 's3://private/report.csv', contentType: 'text/csv', sizeBytes: 12n, sha256: 'abc', metadata: { source: 'task' }
	}))).toEqual({ name: 'report.csv', contentType: 'text/csv', sizeBytes: '12', sha256: 'abc', metadata: { source: 'task' } });
	expect(downloadFilename('../private\u0000report.csv')).toBe('.._private_report.csv');
});

test('durations only use two authoritative timestamps', () => {
	expect(formatDuration({ seconds: 10n, nanos: 0 }, { seconds: 11n, nanos: 500_000_000 })).toBe('1.5s');
	expect(formatDuration({ seconds: 11n, nanos: 0 }, { seconds: 10n, nanos: 0 })).toBe('—');
	const exported = { startedAt: timestampJson({ seconds: 10n, nanos: 0 }) };
	expect(JSON.stringify(exported)).toBe('{"startedAt":"1970-01-01T00:00:10.000Z"}');
});
