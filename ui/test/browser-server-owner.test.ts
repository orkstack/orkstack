import { expect, test } from 'bun:test';
import { env } from 'node:process';
import { proofOwner } from '../e2e/proof-owner';

// The browser-server container is labelled with the process it lives for
// (orkstack.proof.owner, host:pid:start), which every sweep (proofrun clean,
// scripts/proof.sh) reads to tell a live proof's container from a dead one's.
// scripts/proof.sh starts one server for its whole Playwright gate through a
// `bun e2e/browser-server.ts start` that exits at once: labelled with that
// process, a live proof's server read as abandoned and another proof's start
// stopped it mid-gate (2026-10-30, proof api-hardening-feature). The proof
// names itself as the owner instead.

test('the owner is the one the proof names', () => {
	expect(proofOwner({ ORK_PROOF_OWNER: 'sauroposeidon:3902900:18609085' }, 'sauroposeidon', 114342, '18612240')).toBe(
		'sauroposeidon:3902900:18609085'
	);
});

test('without one, the owner is this process, which lives as long as the Playwright run', () => {
	expect(proofOwner({}, 'builder', 4242, '777')).toBe('builder:4242:777');
	expect(proofOwner({ ORK_PROOF_OWNER: '' }, 'builder', 4242, '777')).toBe('builder:4242:777');
});

test('a malformed owner is refused, never replaced by this process', () => {
	for (const owner of ['sauroposeidon', 'sauroposeidon:abc:1', 'sauroposeidon:0:1', ':1:2', 'a b:1:2', 'h:1:2:3']) {
		let refused = '';
		try {
			proofOwner({ ORK_PROOF_OWNER: owner }, 'builder', 4242, '777');
		} catch (err) {
			refused = String(err);
		}
		expect(refused).toContain('ORK_PROOF_OWNER');
	}
});

test('the start command, a process that exits at once, refuses to name itself the owner', () => {
	const without: Record<string, string | undefined> = { ...env };
	delete without.ORK_PROOF_OWNER;
	const run = Bun.spawnSync(['bun', 'e2e/browser-server.ts', 'start', 'pr-test'], { cwd: `${import.meta.dir}/..`, env: without });
	expect(run.exitCode).toBe(2);
	expect(run.stdout.toString()).toBe('');
	expect(run.stderr.toString()).toContain('ORK_PROOF_OWNER');
});
