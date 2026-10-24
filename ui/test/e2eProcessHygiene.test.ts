import { expect, test } from 'bun:test';
import { readdirSync, readFileSync } from 'node:fs';

// The e2e suite shares its host with other stacks, proofs and agents, so a
// spec may only ever stop the processes it started itself, by handle
// (harness.ts killStarted / trackChild). A name or pattern kill — even an
// anchored one like `pkill -f '/bin/ork$'` — reaches every engine and Orker on
// the host, including another run's. This test keeps the suite free of them.

const e2eDir = `${import.meta.dir}/../e2e`;

// Commands that select processes by name or pattern rather than by handle.
const patternKill = /\b(pkill|killall|pgrep)\b|xargs\s+kill/;

test('no e2e file stops a process by name or pattern', () => {
	const offenders: string[] = [];
	for (const name of readdirSync(e2eDir).filter((f: string) => f.endsWith('.ts')).sort()) {
		const lines = readFileSync(`${e2eDir}/${name}`, 'utf8').split('\n');
		lines.forEach((line: string, i: number) => {
			const code = line.trimStart();
			if (code.startsWith('//') || code.startsWith('*') || code.startsWith('/*')) return;
			if (patternKill.test(code)) offenders.push(`${name}:${i + 1}: ${code}`);
		});
	}
	expect(offenders).toEqual([]);
});

test('the harness stops processes only through killStarted', () => {
	const harness = readFileSync(`${e2eDir}/harness.ts`, 'utf8');
	expect(harness).toContain('export async function killStarted');
	expect(harness).not.toContain('export async function killAll');
});
