import { expect, test } from 'bun:test';
import { finishedRuns } from '../src/lib/stats';

// ONE WINDOW, ONE POPULATION — the arithmetic half.
//
// GetStats answers about the runs a window CREATED, with one declared
// exception: `running` is a fact about NOW
// (proto/orkstack/v1/stats.proto, internal/engine/stats.go). So the
// denominator the dashboard prints under its percentiles is the window's
// FINISHED runs, and nothing else. `orkctl stats` computes the same three keys
// (cmd/orkctl/stats.go, successRateLabel); the two surfaces must not disagree
// about the denominator of the same number.

test('the finished population excludes running, which is a fact about now', () => {
	// One long-lived run started before the window is still running inside it.
	// Counting it would make 3 samples "3 of 4" — a share of a set the samples
	// are not drawn from.
	const counts = { running: 1n, succeeded: 2n, failed: 1n, canceled: 0n };
	expect(finishedRuns(counts)).toBe(3);
});

test('a window that finished nothing is 0, not the number of runs in flight', () => {
	expect(finishedRuns({ running: 7n, succeeded: 0n, failed: 0n, canceled: 0n })).toBe(0);
});

test('canceled counts as finished, exactly as the success rate counts it', () => {
	// A client that cancels half its runs has not succeeded at them; hiding
	// them would make the rate rise when things go wrong (stats.proto).
	expect(finishedRuns({ running: 0n, succeeded: 1n, failed: 1n, canceled: 2n })).toBe(4);
});

test('a missing key is 0, and an absent answer is 0 rather than a crash', () => {
	expect(finishedRuns({ succeeded: 5n })).toBe(5);
	expect(finishedRuns(undefined)).toBe(0);
});
