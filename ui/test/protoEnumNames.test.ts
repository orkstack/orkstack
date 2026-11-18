import { expect, test } from 'bun:test';
import { fromJson } from '@bufbuild/protobuf';
import { EventSchema, EventType, InterruptionPhase, PauseTransition } from '../src/lib/gen/orkstack/v1/run_pb';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { prefixOf, withProtoEnumNames } from '../src/lib/protoEnumNames';

// The engine's JSON dialect writes every enum short, nested ones too. Every
// one is restored from the schema, so fromJson decodes the frame.

const decode = (frame: unknown) => fromJson(EventSchema, withProtoEnumNames(frame as never, EventSchema));

test('a nested enum is restored: an interruption notice decodes', () => {
	const event = decode({
		runId: 'ork-1',
		sequence: '7',
		type: 'TASK_LOG',
		taskId: 'build',
		status: 'PENDING',
		interruption: { reason: 'ORKER_HEARTBEAT_LOST', phase: 'RUNNING', at: '2026-11-18T14:00:00Z' }
	});
	expect(event.type).toBe(EventType.TASK_LOG);
	expect(event.status).toBe(Status.PENDING);
	expect(event.interruption?.phase).toBe(InterruptionPhase.RUNNING);
	expect(event.interruption?.at?.seconds).toBe(BigInt(Date.parse('2026-11-18T14:00:00Z') / 1000));
});

test('declared names and numbers pass through; the frame is not modified', () => {
	const frame = { type: 'EVENT_TYPE_RUN_STARTED', status: 2, interruption: { phase: 'INTERRUPTION_PHASE_REPORTED' } };
	const copy = structuredClone(frame);
	const restored = withProtoEnumNames(frame, EventSchema) as typeof frame;
	expect(frame).toEqual(copy);
	expect(restored).toEqual(copy);
});

test('a well-known type is not walked: a Struct payload keeps its values', () => {
	const event = decode({ type: 'TASK_LOG', data: { data: { status: 'RUNNING', type: 'X' } } });
	expect(event.data?.data).toEqual({ status: 'RUNNING', type: 'X' });
});

test('an unknown value is left for fromJson to refuse', () => {
	let refused = false;
	try {
		decode({ type: 'NOT_A_TYPE' });
	} catch {
		refused = true;
	}
	expect(refused).toBe(true);
});

// prefixOf is internal/enumname.Prefix; its Go table's cases, and an acronym.
test('the prefix is enumname.Prefix, acronyms included', () => {
	const cases: [string, string][] = [
		['Status', 'STATUS_'],
		['EventType', 'EVENT_TYPE_'],
		['FilterOperator', 'FILTER_OPERATOR_'],
		['InterruptionPhase', 'INTERRUPTION_PHASE_'],
		['TLSMode', 'TLS_MODE_'],
		['HTTPMethod', 'HTTP_METHOD_'],
		['ModeV2Kind', 'MODE_V2_KIND_'],
		['OIDC', 'OIDC_']
	];
	for (const [name, want] of cases) expect(prefixOf(name)).toBe(want);
});

test('a field keyed by its proto name is restored too', () => {
	const event = decode({ type: 'TASK_LOG', interruption: { phase: 'AFTER_EXIT' }, pause_transition: 'PAUSED' });
	expect(event.interruption?.phase).toBe(InterruptionPhase.AFTER_EXIT);
	expect(event.pauseTransition).toBe(PauseTransition.PAUSED);
});
