import { expect, test } from 'bun:test';
import {
	confirmMatches,
	hoursToMs,
	isLowering,
	lowerConsequence,
	parseRetentionHours,
	retentionHours
} from '../src/lib/retentionForm';

// The Admin retention forms' (deployment maximum and per-namespace) local
// validation mirrors the engine's own rule (internal/api/system.go: a
// non-positive duration is refused with InvalidArgument before any call
// leaves the process) one layer up, so a bad value never reaches
// UpdateRetentionPolicy or CreateNamespace/UpdateNamespace at all.

test('a positive number of hours parses', () => {
	const result = parseRetentionHours('168');
	expect(result).toEqual({ hours: 168 });
});

test('whitespace around a positive number is trimmed', () => {
	expect(parseRetentionHours('  2.5  ')).toEqual({ hours: 2.5 });
});

test('blank input is refused by name, not silently treated as zero', () => {
	const result = parseRetentionHours('');
	expect('error' in result && result.error).toContain('positive');
});

test('zero and negative values are refused', () => {
	for (const raw of ['0', '-1', '-0.5']) {
		const result = parseRetentionHours(raw);
		expect('error' in result).toBe(true);
	}
});

test('non-numeric input is refused, not coerced to NaN silently accepted', () => {
	const result = parseRetentionHours('not-a-number');
	expect('error' in result).toBe(true);
});

test('hoursToMs converts hours to the milliseconds durationFromMs expects', () => {
	expect(hoursToMs(1)).toBe(3_600_000);
	expect(hoursToMs(0.5)).toBe(1_800_000);
	expect(hoursToMs(168)).toBe(604_800_000);
});

// F-ui-ux-1: the field starts at the value in force, and only LOWERING the
// maximum — which retires stored runs — asks for a typed confirmation.

const days7 = { seconds: 604800n, nanos: 0 };

test('the field is prefilled with the maximum in force, in hours', () => {
	expect(retentionHours(days7)).toBe('168');
	expect(retentionHours({ seconds: 5400n, nanos: 0 })).toBe('1.5');
	expect(retentionHours(undefined)).toBe('');
	expect(retentionHours({ seconds: 0n, nanos: 0 })).toBe('');
});

test('only a lower value is a lowering', () => {
	expect(isLowering(days7, 24)).toBe(true);
	expect(isLowering(days7, 167.5)).toBe(true);
	expect(isLowering(days7, 168)).toBe(false);
	expect(isLowering(days7, 720)).toBe(false);
	expect(isLowering(undefined, 1)).toBe(false);
});

test('the confirmation names both values and what is reclaimed', () => {
	const text = lowerConsequence('168h 0m', 24);
	expect(text).toContain('from 168h 0m to 24h');
	expect(text).toContain('logs, attempt records, snapshots and commits');
	expect(text).toContain('older than 24h');
	expect(text).toContain('sets no run retention of its own');
});

test('Accept needs the new value typed', () => {
	expect(confirmMatches('', 24)).toBe(false);
	expect(confirmMatches('2', 24)).toBe(false);
	expect(confirmMatches('24', 24)).toBe(true);
	expect(confirmMatches(' 24.0 ', 24)).toBe(true);
	expect(confirmMatches('168', 24)).toBe(false);
});
