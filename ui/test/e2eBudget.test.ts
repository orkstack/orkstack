import { expect, test } from 'bun:test';
import { cpus } from 'node:os';
import { REFERENCE_CPUS, boundScale, budgetNote, hostCPUs, scaledBudgetMs } from '../e2e/budget';

// The run page's render budget is held to its reference on the reference
// host, and to the reference scaled by 16 over the CPUs on a smaller one: the
// twin of internal/testsupport/proof's Timing.Check (docs/PROOFS.md §12).

test('the reference class and larger hosts are held to the reference bound', () => {
	expect(REFERENCE_CPUS).toBe(16);
	for (const n of [16, 32, 128]) expect(scaledBudgetMs(5_000, n)).toBe(5_000);
});

test('a smaller host is held to the reference scaled by 16 over its CPUs', () => {
	expect(scaledBudgetMs(5_000, 8)).toBe(10_000);
	expect(scaledBudgetMs(5_000, 4)).toBe(20_000);
	expect(scaledBudgetMs(5_000, 2)).toBe(40_000);
	expect(boundScale(1)).toBe(16);
});

test('an unknown CPU count is held to the reference bound, never widened', () => {
	for (const n of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) expect(boundScale(n)).toBe(1);
});

test('the host count never falls below its logical CPUs', () => {
	expect(hostCPUs()).toBeGreaterThanOrEqual(cpus().length);
});

test('the note names the bound, the reference and the CPUs', () => {
	expect(budgetNote(5_000, 4)).toBe('budget 20000 ms: the reference 5000 ms × 4 on 4 CPUs');
});
