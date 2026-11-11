import { availableParallelism, cpus } from 'node:os';

// A render budget calibrated on the reference host (docs/PROOFS.md §12) is
// held, on a host with fewer CPUs, to its reference bound scaled by the
// reference's CPUs over the host's, and on the reference class or larger to
// the reference bound itself: the twin of internal/testsupport/proof's
// Timing.Check. No table excuses a miss; the spec's own expectation fails
// beyond the bound this host is held to, and the spec's note names both.

/** The CPU count of the reference host the budgets were calibrated on. */
export const REFERENCE_CPUS = 16;

/**
 * How much a reference budget widens on a host of `count` CPUs: the
 * reference's CPUs over `count` on a smaller host, and 1 on the reference
 * class or larger, or when the count is unknown, so an unknown host is held to
 * the reference bound.
 */
export function boundScale(count: number): number {
	if (!Number.isFinite(count) || count < 1 || count >= REFERENCE_CPUS) return 1;
	return REFERENCE_CPUS / count;
}

/**
 * The CPU count a budget is scaled by: the larger of the host's logical CPUs
 * and those this process may run on, so restricting a proof to fewer cores
 * never widens a bound.
 */
export function hostCPUs(): number {
	return Math.max(cpus().length, availableParallelism());
}

/** A reference budget in milliseconds, scaled to a host of `count` CPUs. */
export function scaledBudgetMs(referenceMs: number, count: number = hostCPUs()): number {
	return Math.round(referenceMs * boundScale(count));
}

/** The note a spec prints beside a scaled budget. */
export function budgetNote(referenceMs: number, count: number = hostCPUs()): string {
	return `budget ${scaledBudgetMs(referenceMs, count)} ms: the reference ${referenceMs} ms × ${boundScale(count)} on ${count} CPUs`;
}
