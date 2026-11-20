import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

// The solo projects of ui/playwright.config.ts are the spec files that must
// run alone, after every other file, on one worker. A proof runs them only by
// naming each one with --project (scripts/proof.sh, scripts/proof-part.sh), so
// a solo project a script does not name is never run by a proof, and a spec a
// solo project does not hold runs beside the others.
//
// network-change.spec.ts is one of them: its positive control is a Chromium
// in the host's network namespace, which saw another spec's activity
// containers come and go and re-loaded for them (proof 44677556224).

const ui = `${import.meta.dir}/..`;
const config = readFileSync(`${ui}/playwright.config.ts`, 'utf8');
const scripts = ['scripts/proof.sh', 'scripts/proof-part.sh'].map((p) => ({ path: p, text: readFileSync(`${ui}/../${p}`, 'utf8') }));

const soloProjects = [...config.matchAll(/name: '(chromium-solo-[a-z-]+)'/g)].map((m) => m[1]);

test('network-change.spec.ts runs in a solo project of its own', () => {
	expect(soloProjects).toContain('chromium-solo-network-change');
	const block = config.slice(config.indexOf("name: 'chromium-solo-network-change'"));
	expect(block.slice(0, block.indexOf('\n\t\t}'))).toContain('testMatch: /network-change\\.spec\\.ts$/');
	const solo = config.slice(config.indexOf('export const SOLO_SPECS'), config.indexOf('export default'));
	expect(solo).toContain('/network-change\\.spec\\.ts$/');
});

test('every solo project is run by both proof scripts', () => {
	expect(soloProjects.length).toBeGreaterThanOrEqual(3);
	for (const { path, text } of scripts) {
		for (const project of soloProjects) {
			expect({ path, project, named: text.includes(`--project ${project}`) }).toEqual({ path, project, named: true });
		}
	}
});
