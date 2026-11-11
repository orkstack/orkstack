import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { PayloadSchema, Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { AwaitResolvedVia, TaskAwaitSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	awaitCallbackUrl,
	awaitCurl,
	awaitDeadline,
	awaitRemaining,
	awaitStateLabel,
	printable,
	readableValues,
	resolvedValuesView,
	VALUES_SHOW_MAX,
	resolveCli
} from '../src/lib/awaits';

test('awaitCurl single-quotes a real token and double-quotes the placeholder', () => {
	const url = awaitCallbackUrl('http://localhost:8080/', 'ork-1', 'training');
	expect(url).toBe('http://localhost:8080/v1/runs/ork-1/awaits/training');
	const real = awaitCurl({ url, token: "tok'en" });
	expect(real).toContain(`-H 'Authorization: Bearer tok'"'"'en'`);
	expect(real).toContain(`curl -X POST http://localhost:8080/v1/runs/ork-1/awaits/training`);
	const placeholder = awaitCurl({ url });
	expect(placeholder).toContain('-H "Authorization: Bearer $ORKSTACK_CALLBACK_TOKEN"');
	const reject = awaitCurl({ url, reject: true });
	expect(reject).toContain('http://localhost:8080/v1/runs/ork-1/awaits/training/reject ');
	expect(reject).toContain(`-d '{"reason":"…"}'`);
});

test('resolveCli round-trips a value containing a quote', () => {
	const command = resolveCli({
		namespace: 'team-a',
		runId: 'ork-1',
		taskId: 'approval',
		valuesJson: `{"approver":"o'brien"}`
	});
	expect(command).toBe(
		`orkctl -n team-a run resolve ork-1 -t approval --values '{"approver":"o'"'"'brien"}'`
	);
	expect(resolveCli({ namespace: '', runId: 'ork-1', taskId: 'gate', reject: true, reason: 'no' })).toBe(
		`orkctl -n default run resolve ork-1 -t gate --reject --reason no`
	);
	expect(resolveCli({ namespace: '', runId: 'ork-1', taskId: 'gate', valuesJson: '{}' })).toBe(
		`orkctl -n default run resolve ork-1 -t gate`
	);
});

test('awaitRemaining counts down, says expired, and has no deadline as -', () => {
	const now = timestampFromMs(Date.UTC(2026, 0, 2, 3, 4, 5));
	const deadline = timestampFromMs(Date.UTC(2026, 0, 2, 3, 4, 5) + (5 * 3600 + 41 * 60) * 1000);
	expect(awaitRemaining(deadline, now)).toBe('in 5h41m');
	expect(awaitRemaining(now, deadline)).toBe('expired');
	expect(awaitRemaining(undefined, now)).toBe('-');
	expect(awaitRemaining(timestampFromMs(Date.UTC(2026, 0, 2, 3, 30, 5)), now)).toBe('in 26m');
});

test('the event-derived deadline equals the snapshot deadline for the same instant', () => {
	const awaitingAt = timestampFromMs(Date.UTC(2026, 8, 24, 14, 2, 0, 250));
	const snapshot = create(TaskAwaitSchema, {
		deadline: timestampFromMs(Date.UTC(2026, 8, 24, 14, 2, 0, 250) + 21600 * 1000)
	});
	const fromEvent = awaitDeadline({}, 21600, awaitingAt);
	const fromSnapshot = awaitDeadline({ await: snapshot }, 21600, undefined);
	expect(fromEvent).toEqual({ seconds: snapshot.deadline!.seconds, nanos: snapshot.deadline!.nanos });
	expect(fromSnapshot).toEqual(fromEvent);
	expect(awaitDeadline({}, 21600, undefined)).toBeUndefined();
});

test('awaitStateLabel names every state', () => {
	const via = (v: AwaitResolvedVia, by: string) => create(TaskAwaitSchema, { resolvedVia: v, resolvedBy: by });
	expect(awaitStateLabel({ status: Status.AWAITING })).toBe('Waiting for external completion');
	expect(
		awaitStateLabel({ status: Status.SUCCEEDED, await: via(AwaitResolvedVia.CALLBACK, 'ci-bot') })
	).toBe('Completed via callback by ci-bot');
	expect(
		awaitStateLabel({
			status: Status.FAILED,
			await: via(AwaitResolvedVia.API, 'alice'),
			error: { code: 'AWAIT_REJECTED' }
		})
	).toBe('Rejected via api by alice');
	expect(awaitStateLabel({ status: Status.FAILED, error: { code: 'AWAIT_TIMED_OUT' } })).toBe('Timed out');
	expect(awaitStateLabel({ status: Status.CANCELED, opened: true })).toBe('Canceled while waiting');
	expect(awaitStateLabel({ status: Status.PENDING, timeoutSeconds: 21600 })).toBe(
		'Will wait up to 6h once its needs are complete'
	);
});

test('a resolution draws in the flow when short, closed when long, and by reference when the run has no copy', () => {
	const payload = (data: unknown) => create(PayloadSchema, { data: data as never });
	expect(resolvedValuesView(undefined, undefined)).toEqual({ kind: 'none' });
	expect(resolvedValuesView(payload({ approved: true }), undefined)).toEqual({ kind: 'inline', text: '{"approved": true}' });
	const long = resolvedValuesView(payload({ note: 'x'.repeat(500) }), undefined);
	expect(long.kind).toBe('long');
	expect(resolvedValuesView(undefined, { sizeBytes: 9000n })).toEqual({ kind: 'reference', bytes: 9000 });
	// The engine sets an empty payload beside the reference of a value it does not carry inline.
	expect(resolvedValuesView(payload({}), { sizeBytes: 9000n })).toEqual({ kind: 'reference', bytes: 9000 });
	expect(resolvedValuesView(payload({}), { sizeBytes: 2n })).toEqual({ kind: 'inline', text: '{}' });
	// Values of 4096 bytes or fewer always travel inline, so an empty payload beside a
	// reference that small is what they were: `{}` written with a newline or spaces.
	expect(resolvedValuesView(payload({}), { sizeBytes: 3n })).toEqual({ kind: 'inline', text: '{}' });
	expect(resolvedValuesView(payload({}), { sizeBytes: 4096n })).toEqual({ kind: 'inline', text: '{}' });
	expect(resolvedValuesView(payload({}), { sizeBytes: 4097n })).toEqual({ kind: 'reference', bytes: 4097 });
	let deep: unknown = 'bottom';
	for (let i = 0; i < 500; i++) deep = { d: deep };
	expect(resolvedValuesView(payload(deep), undefined).kind).toBe('long');
});

test('control characters and reordering marks show as escapes, and lines and tabs stay', () => {
	expect(printable('a\u0007b\u001b[31m‮\u0085')).toBe('a\\u0007b\\u001b[31m\\u202e\\u0085');
	expect(printable('line\n\ttab')).toBe('line\n\ttab');
	expect(printable('<img src=x>')).toBe('<img src=x>');
});

test('stored values read indented when whole, and as they are when cut or not JSON', () => {
	expect(readableValues('{"a":1}', false)).toEqual({ text: '{\n  "a": 1\n}', cut: false });
	expect(readableValues('{"a":1', true)).toEqual({ text: '{"a":1', cut: true });
	expect(readableValues('not json\u0007', false)).toEqual({ text: 'not json\\u0007', cut: false });
});

test('deep JSON stays compact when indenting would outgrow what a box shows', () => {
	const deep = `{"a":${'['.repeat(2000)}${']'.repeat(2000)}}`;
	const view = resolvedValuesView(create(PayloadSchema, { data: JSON.parse(deep) as never }), undefined);
	expect(view.kind).toBe('long');
	if (view.kind === 'long') {
		expect(view.text.length <= VALUES_SHOW_MAX).toBe(true);
		expect(view.bytes).toBe(deep.length);
	}
	expect(readableValues(deep, false).text).toBe(deep);
});
