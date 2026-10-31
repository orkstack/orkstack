// The owner a proof's container is labelled with (orkstack.proof.owner,
// host:pid:start): the process it lives for. Every sweep (proofrun clean,
// scripts/proof.sh's) reads it to tell a live proof's container from a dead
// one's, so it must name a process that outlives the container's use. Pure,
// so ui/test can hold it without the Node types the e2e harness runs with.

/** OWNER_SHAPE is an orkstack.proof.owner label: host:pid:start. */
const OWNER_SHAPE = /^[^:\s]+:[1-9][0-9]*:[0-9]+$/;

/**
 * proofOwner is ORK_PROOF_OWNER when the proof names it (scripts/proof.sh
 * hands its own shell, for the browser server it starts for its whole gate
 * through a starter that exits at once), otherwise this process (host, pid,
 * start), which lives as long as the run when it is Playwright's own
 * (e2e/global-setup.ts). A malformed ORK_PROOF_OWNER is refused, never
 * replaced by this process.
 */
export function proofOwner(env: Record<string, string | undefined>, host: string, pid: number, start: string): string {
	const named = env.ORK_PROOF_OWNER;
	if (named !== undefined && named !== '') {
		if (!OWNER_SHAPE.test(named)) throw new Error(`ORK_PROOF_OWNER=${named} is not host:pid:start`);
		return named;
	}
	return `${host}:${pid}:${start}`;
}
