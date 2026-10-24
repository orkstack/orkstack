import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { TimestampSchema } from '@bufbuild/protobuf/wkt';
import { ArtifactKind, ReferenceState } from '../src/lib/gen/orkstack/v1/run_service_pb';
import {
	artifactKindFlag,
	provenanceCommand,
	referenceStateText,
	rfc3339,
	unreadableText
} from '../src/lib/provenance';

test('the three reference states use the CLI golden wordings, and ABSENT is never held or released', () => {
	const at = create(TimestampSchema, { seconds: BigInt(Date.UTC(2026, 0, 9, 3, 4, 5) / 1000), nanos: 0 });
	expect(referenceStateText(ReferenceState.HELD, undefined)).toBe("held — this run's archive holds its marker on the object");
	expect(referenceStateText(ReferenceState.RELEASED, at)).toBe(
		"released — cleanup released this run's object claims at 2026-01-09T03:04:05Z"
	);
	const absent = referenceStateText(ReferenceState.ABSENT, undefined);
	expect(absent).toBe(
		'absent — no marker and no cleanup record for this run (not archived yet, or archived before the object index existed)'
	);
	expect(referenceStateText(ReferenceState.UNSPECIFIED, undefined)).toBe(absent);
	expect(rfc3339(undefined)).toBe('—');
});

test('the unreadable footer counts markers and is empty for none', () => {
	expect(unreadableText(0n)).toBe('');
	expect(unreadableText(1n)).toBe('1 marker could not be resolved: its run body is not in the archive');
	expect(unreadableText(3n)).toBe('3 markers could not be resolved: their run bodies are not in the archive');
});

test('the CLI equivalent is runCommand with the kind as the flag the CLI parses', () => {
	expect(artifactKindFlag(ArtifactKind.OUTPUT)).toBe('output');
	expect(artifactKindFlag(ArtifactKind.VALUES)).toBe('values');
	expect(artifactKindFlag(ArtifactKind.STDOUT)).toBe('stdout');
	expect(artifactKindFlag(ArtifactKind.STDERR)).toBe('stderr');
	expect(provenanceCommand('', 'ork-1', 'completed', ArtifactKind.OUTPUT, 'report.txt')).toBe(
		"orkctl --namespace 'default' 'run' 'artifact' 'provenance' 'ork-1' '--task' 'completed' '--kind' 'output' '--name' 'report.txt'"
	);
});
