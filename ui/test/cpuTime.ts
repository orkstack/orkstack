import { cpuUsage } from 'node:process';

/**
 * The CPU time this process has used so far, in milliseconds (user and system).
 *
 * A unit test that bounds what code costs reads it here and never from the
 * wall clock. Other processes on the host delay the code without adding to its
 * CPU time, so a loaded runner moves a wall-clock bound and leaves this one
 * (mostly) alone; code that really got slower moves both.
 */
export function cpuMs(): number {
	const { user, system } = cpuUsage();
	return (user + system) / 1000;
}

/**
 * Runs fn `runs` times and returns the least CPU time any run used, with the
 * last run's result.
 *
 * The least of a few, not one: the first call pays for the runtime compiling
 * the code, and the runtime's compiler and collector threads add their time to
 * the process's CPU time, more of it while the host is busy (bun gives a
 * process no per-thread CPU clock, as the Go tests have). Code that really got
 * slower is slower on every run, so the least still moves.
 */
export function leastCpuMs<T>(fn: () => T, runs = 3): { ms: number; result: T } {
	let ms = Infinity;
	let result!: T;
	for (let i = 0; i < runs; i++) {
		const started = cpuMs();
		result = fn();
		ms = Math.min(ms, cpuMs() - started);
	}
	return { ms, result };
}
