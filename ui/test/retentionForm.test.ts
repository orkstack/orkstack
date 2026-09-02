import { expect, test } from 'bun:test';
import { hoursToMs, parseRetentionHours } from '../src/lib/retentionForm';

// The Admin Execution Retention form's local validation mirrors the engine's
// own rule (internal/api/system.go: a non-positive duration is refused with
// InvalidArgument before any call leaves the process) one layer up, so a bad
// value never reaches UpdateExecutionRetention at all.

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
