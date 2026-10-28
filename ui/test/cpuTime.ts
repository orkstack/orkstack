import { threadCpuUsage } from 'node:process';

/**
 * The CPU time the calling thread has used so far, in milliseconds (user and
 * system).
 *
 * A unit test that bounds what code costs reads it here and never from the
 * wall clock. Other processes on the host delay the code without adding to its
 * CPU time, so a loaded runner moves a wall-clock bound and leaves this one
 * alone; code that really got slower moves both. It is the thread's, not the
 * process's, as the Go tests read: the runtime's compiler and collector
 * threads run beside the test, and on a busy host they add hundreds of
 * milliseconds to the process's CPU time that the code under test never used.
 */
export function cpuMs(): number {
	const { user, system } = threadCpuUsage();
	return (user + system) / 1000;
}
