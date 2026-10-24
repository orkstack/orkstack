import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { Status } from '../src/lib/gen/orkstack/v1/common_pb';
import { AwaitResolvedVia, TaskAwaitSchema } from '../src/lib/gen/orkstack/v1/run_pb';
import {
	awaitCallbackUrl,
	awaitCurl,
	awaitDeadline,
	awaitRemaining,
	awaitStateLabel,
	resolveCli
} from '../src/lib/awaits';

test('awaitCurl single-quotes a real token and double-quotes the placeholder', () => {
	const url = awaitCallbackUrl('http://localhost:8080/', 'ork-1', 'training');
	expect(url).toBe('http://localhost:8080/v1/runs/ork-1/awaits/training');
	const real = awaitCurl({ url, token: "tok'en" });
	expect(real).toContain(`-H 'Authorization: Bearer tok'"'"'en'`);
	expect(real).toContain(`curl -X POST 'http://localhost:8080/v1/runs/ork-1/awaits/training'`);
	const placeholder = awaitCurl({ url });
	expect(placeholder).toContain('-H "Authorization: Bearer $ORKSTACK_CALLBACK_TOKEN"');
	const reject = awaitCurl({ url, reject: true });
	expect(reject).toContain(`'http://localhost:8080/v1/runs/ork-1/awaits/training/reject'`);
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
		`orkctl --namespace 'team-a' 'run' 'resolve' 'ork-1' '--task' 'approval' '--values' '{"approver":"o'"'"'brien"}'`
	);
	expect(resolveCli({ namespace: '', runId: 'ork-1', taskId: 'gate', reject: true, reason: 'no' })).toBe(
		`orkctl --namespace 'default' 'run' 'resolve' 'ork-1' '--task' 'gate' '--reject' '--reason' 'no'`
	);
	expect(resolveCli({ namespace: '', runId: 'ork-1', taskId: 'gate', valuesJson: '{}' })).toBe(
		`orkctl --namespace 'default' 'run' 'resolve' 'ork-1' '--task' 'gate'`
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
