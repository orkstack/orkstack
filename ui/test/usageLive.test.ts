import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { LiveUsageSchema, LiveUsageUnavailableReason } from '../src/lib/gen/orkstack/v1/run_service_pb';
import { MeasuredUsageSchema, MeasurementMethod, UnmeasuredReason } from '../src/lib/gen/orkstack/v1/run_pb';
import { liveUnavailableWords, liveUsageFacts, measuredFacts, unmeasuredWords, UNSET } from '../src/lib/usageLive';

test('a live reading is cores and memory against its limit', () => {
	const facts = liveUsageFacts(create(LiveUsageSchema, { available: true, cpuCores: 1.5, memoryBytes: 64n << 20n, memoryLimitBytes: 1n << 30n }));
	expect(facts).toEqual({ cpu: '1.50 cores', memory: '64.0 MiB of 1.0 GiB', note: '', stale: false });
});

test('no rate yet is the em dash, never 0 cores', () => {
	const facts = liveUsageFacts(create(LiveUsageSchema, { available: true, memoryBytes: 512n }));
	expect(facts.cpu).toBe(UNSET);
	expect(facts.memory).toBe('512 B');
});

test('a measured idle container is 0.00 cores, which is a fact', () => {
	expect(liveUsageFacts(create(LiveUsageSchema, { available: true, cpuCores: 0, memoryBytes: 1n })).cpu).toBe('0.00 cores');
});

test('no limit is memory alone', () => {
	expect(liveUsageFacts(create(LiveUsageSchema, { available: true, memoryBytes: 2048n })).memory).toBe('2.0 KiB');
});

test('a stale reading says so', () => {
	expect(liveUsageFacts(create(LiveUsageSchema, { available: true, cpuCores: 1, memoryBytes: 1n, stale: true })).stale).toBe(true);
});

test('an unavailable reading is dashes with its reason', () => {
	const facts = liveUsageFacts(create(LiveUsageSchema, { unavailableReason: LiveUsageUnavailableReason.CGROUP_NOT_VISIBLE }));
	expect(facts).toEqual({ cpu: UNSET, memory: UNSET, note: "its Orker cannot see the container's cgroup", stale: false });
	expect(liveUsageFacts(undefined).cpu).toBe(UNSET);
});

test('every live reason and unmeasured reason has words', () => {
	for (const value of Object.values(LiveUsageUnavailableReason).filter((v): v is number => typeof v === 'number')) {
		expect(liveUnavailableWords(value)).not.toBe('for a reason this page does not know');
	}
	for (const value of Object.values(UnmeasuredReason).filter((v): v is number => typeof v === 'number')) {
		expect(unmeasuredWords(value)).not.toBe('for a reason this page does not know');
	}
	// A reason never shows as a number, known or not.
	expect(liveUnavailableWords(99 as LiveUsageUnavailableReason)).toBe('for a reason this page does not know');
	expect(liveUsageFacts(create(LiveUsageSchema, {})).note).toBe('no reading yet');
});

test('an ended attempt shows what its container used', () => {
	const facts = measuredFacts(
		create(MeasuredUsageSchema, { method: MeasurementMethod.CGROUP_V2, cpuSeconds: 12.34, memoryPeakBytes: 64n << 20n, memoryPeakExact: true, memoryLimitBytes: 1n << 30n })
	);
	expect(facts.used).toBe('12.3 cpu-s · 64.0 MiB peak');
	expect(facts.tip).toContain("the kernel's own peak of a 1.0 GiB limit");
	expect(facts.oom).toBe(false);
});

test('a sampled peak says it is not the kernel’s own', () => {
	const facts = measuredFacts(create(MeasuredUsageSchema, { method: MeasurementMethod.CGROUP_V1, cpuSeconds: 1, memoryPeakBytes: 10n }));
	expect(facts.tip).toContain('the most a reading saw');
});

test('stopped at its limit is flagged', () => {
	expect(measuredFacts(create(MeasuredUsageSchema, { method: MeasurementMethod.CGROUP_V2, oomKilled: true })).oom).toBe(true);
});

test('unmeasured, not run and older records never read as 0', () => {
	expect(measuredFacts(undefined).used).toBe(UNSET);
	expect(measuredFacts(create(MeasuredUsageSchema, {})).used).toBe(UNSET);
	const off = measuredFacts(create(MeasuredUsageSchema, { method: MeasurementMethod.UNMEASURED, unmeasuredReason: UnmeasuredReason.DISABLED }));
	expect(off).toEqual({ used: UNSET, tip: 'Not measured: measuring is off on its Orker.', oom: false });
	expect(measuredFacts(create(MeasuredUsageSchema, { method: MeasurementMethod.UNMEASURED, unmeasuredReason: UnmeasuredReason.NOT_RUN })).used).toBe('no container ran');
});
