import { expect, test } from 'bun:test';
import { create, type MessageInitShape } from '@bufbuild/protobuf';
import { TimestampSchema } from '@bufbuild/protobuf/wkt';
import {
	HeartbeatUnavailableReason,
	LiveAttemptSchema,
	LiveAttemptState,
	type LiveAttempt
} from '../src/lib/gen/orkstack/v1/service_pb';
import { formatBytes, heartbeatAge, liveLine } from '../src/lib/live';

// The live-attempt derivation's scenario table, L1–L11, as the server answers
// it, through the browser's live line (U1–U11). The sentences are the ones
// the spec lists and `orkctl run inspect` prints beside them. Times of day are
// rendered in the viewer's zone, so the table pins it.
(globalThis as unknown as { process: { env: Record<string, string | undefined> } }).process.env.TZ = 'UTC';

function at(iso: string) {
	const msValue = Date.parse(iso);
	return create(TimestampSchema, {
		seconds: BigInt(Math.floor(msValue / 1000)),
		nanos: (msValue % 1000) * 1_000_000
	});
}
const nowAt = (iso: string) => Date.parse(iso);

function attempt(fields: MessageInitShape<typeof LiveAttemptSchema>): LiveAttempt {
	return create(LiveAttemptSchema, {
		taskId: 'fit',
		attempt: 1,
		timeoutSeconds: 2700,
		cancellationGracePeriodSeconds: 30,
		...fields
	});
}

const running = {
	heartbeatAvailable: true,
	phase: 'run',
	containerId: '3f2a9c1d0b4e7f00',
	bytesLogged: 432013312n,
	orkerId: 'orker-7'
};

type Row = { name: string; attempt: LiveAttempt; now: number; kind: string; text: string; title?: string; stale: boolean };

const rows: Row[] = [
	{
		name: 'U1 (L1) started with a heartbeat',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			lastHeartbeatAt: at('2026-09-23T09:10:10Z'),
			heartbeatDeadlineAt: at('2026-09-23T09:10:40Z'),
			...running
		}),
		now: nowAt('2026-09-23T09:10:13Z'),
		kind: 'started',
		text: 'Phase run · 412.0 MiB logged · heartbeat 3s ago',
		title: 'Last heartbeat 3s ago; Temporal times this attempt out at 09:10:40 unless another beat arrives.',
		stale: false
	},
	{
		name: 'U2 (L2) started without a heartbeat',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			heartbeatUnavailableReason: HeartbeatUnavailableReason.NO_HEARTBEAT_YET
		}),
		now: nowAt('2026-09-23T09:10:01Z'),
		kind: 'unavailable',
		text: 'Phase unavailable · no heartbeat yet',
		title: 'An orker has this attempt; no heartbeat yet.',
		stale: false
	},
	{
		name: 'U3 (L3) a previous attempt\'s detail is not this attempt\'s',
		attempt: attempt({
			attempt: 2,
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:20:11Z'),
			heartbeatUnavailableReason: HeartbeatUnavailableReason.NO_HEARTBEAT_YET
		}),
		now: nowAt('2026-09-23T09:20:12Z'),
		kind: 'unavailable',
		text: 'Phase unavailable · no heartbeat yet',
		stale: false
	},
	{
		name: 'U4 (L4) waiting for an orker',
		attempt: attempt({
			state: LiveAttemptState.SCHEDULED,
			attemptScheduledAt: at('2026-09-23T09:14:02Z'),
			heartbeatUnavailableReason: HeartbeatUnavailableReason.NOT_STARTED
		}),
		now: nowAt('2026-09-23T09:16:12Z'),
		kind: 'waiting',
		text: 'Waiting for an orker · 2m 10s',
		stale: false
	},
	{
		name: 'U5 (L5) a retry waiting out its backoff',
		attempt: attempt({
			attempt: 3,
			state: LiveAttemptState.SCHEDULED,
			attemptScheduledAt: at('2026-09-23T09:20:11Z'),
			nextAttemptAt: at('2026-09-23T09:20:11Z'),
			heartbeatUnavailableReason: HeartbeatUnavailableReason.NOT_STARTED
		}),
		now: nowAt('2026-09-23T09:19:29Z'),
		kind: 'retry',
		text: 'Attempt 3 scheduled for 09:20:11 · in 42s',
		title: 'Attempt 2 ended; its error is in the attempt timeline.',
		stale: false
	},
	{
		name: 'U6 (L6) a stale heartbeat',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:56:06Z'),
			lastHeartbeatAt: at('2026-09-23T09:56:11Z'),
			heartbeatDeadlineAt: at('2026-09-23T09:56:41Z'),
			heartbeatStale: true,
			...running
		}),
		now: nowAt('2026-09-23T09:56:35Z'),
		kind: 'started',
		text: 'Phase run · 412.0 MiB logged · heartbeat 24s ago — STALE',
		title:
			'No heartbeat for 24s. The orker is gone or partitioned; Temporal times this attempt out at 09:56:41 unless a beat arrives.',
		stale: true
	},
	{
		name: 'U7 (L7) an undecodable detail',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			heartbeatUnavailableReason: HeartbeatUnavailableReason.UNDECODABLE
		}),
		now: nowAt('2026-09-23T09:10:06Z'),
		kind: 'unavailable',
		text: 'Phase unavailable · heartbeat detail undecodable',
		title: 'An orker has this attempt; heartbeat detail undecodable.',
		stale: false
	},
	{
		name: 'U8 (L8) cancel requested',
		attempt: attempt({
			state: LiveAttemptState.CANCEL_REQUESTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			lastHeartbeatAt: at('2026-09-23T09:10:40Z'),
			heartbeatDeadlineAt: at('2026-09-23T09:11:10Z'),
			...running,
			phase: 'cancel'
		}),
		now: nowAt('2026-09-23T09:10:41Z'),
		kind: 'cancelling',
		text: 'Cancelling · phase cancel',
		stale: false
	},
	{
		name: 'U9 (L9) an old orker\'s beat',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			lastHeartbeatAt: at('2026-09-23T09:10:05Z'),
			heartbeatDeadlineAt: at('2026-09-23T09:10:35Z'),
			heartbeatAvailable: true,
			phase: 'image'
		}),
		now: nowAt('2026-09-23T09:10:06Z'),
		kind: 'started',
		text: 'Phase image · 0 B logged · heartbeat 1s ago',
		stale: false
	},
	{
		name: 'U10 (L10) paused has no OrkStack name',
		attempt: attempt({
			state: LiveAttemptState.UNSPECIFIED,
			heartbeatUnavailableReason: HeartbeatUnavailableReason.NOT_STARTED
		}),
		now: nowAt('2026-09-23T09:10:06Z'),
		kind: 'unavailable',
		text: 'Phase unavailable · not started',
		stale: false
	},
	{
		name: 'U11 (L11) exactly at the threshold is the server\'s call: not stale',
		attempt: attempt({
			state: LiveAttemptState.STARTED,
			attemptStartedAt: at('2026-09-23T09:10:00Z'),
			lastHeartbeatAt: at('2026-09-23T09:10:05Z'),
			heartbeatDeadlineAt: at('2026-09-23T09:10:35Z'),
			heartbeatStale: false,
			...running
		}),
		now: nowAt('2026-09-23T09:10:25Z'),
		kind: 'started',
		text: 'Phase run · 412.0 MiB logged · heartbeat 20s ago',
		stale: false
	}
];

for (const row of rows) {
	test(row.name, () => {
		const line = liveLine(row.attempt, row.now);
		expect(line.kind).toBe(row.kind as typeof line.kind);
		expect(line.text).toBe(row.text);
		expect(line.stale).toBe(row.stale);
		if (row.title !== undefined) expect(line.title).toBe(row.title);
	});
}

test('U12 formatBytes matches the CLI table', () => {
	expect(formatBytes(0)).toBe('0 B');
	expect(formatBytes(1023)).toBe('1023 B');
	expect(formatBytes(1024)).toBe('1.0 KiB');
	expect(formatBytes(432013312n)).toBe('412.0 MiB');
	expect(formatBytes(5368709120n)).toBe('5.0 GiB');
});

test('U13 heartbeatAge reads just now for a non-positive difference', () => {
	const a = attempt({ lastHeartbeatAt: at('2026-09-23T09:10:05Z') });
	expect(heartbeatAge(a, nowAt('2026-09-23T09:10:05Z')).label).toBe('just now');
	expect(heartbeatAge(a, nowAt('2026-09-23T09:10:04Z')).label).toBe('just now');
	expect(heartbeatAge(a, nowAt('2026-09-23T09:10:08Z'))).toEqual({ seconds: 3, label: '3s ago' });
});

test('U14 liveLine never reads the browser clock', () => {
	const row = rows[0];
	const first = liveLine(row.attempt, row.now);
	const second = liveLine(row.attempt, row.now);
	expect(second).toEqual(first);
	// A nowMs far from the browser's clock still drives every age.
	expect(liveLine(row.attempt, row.now + 60_000).text).toBe('Phase run · 412.0 MiB logged · heartbeat 1m 3s ago');
});
