// The version OrkStack last released, read from CHANGELOG.md at build time, so
// the page never names a release that has not happened.
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';

// Astro builds from site/; bundled code cannot use its own location.
const repoRoot = path.resolve(process.cwd(), '..');
if (!existsSync(path.join(repoRoot, 'ui', 'src', 'tokens.css'))) {
	throw new Error(`build the site from site/ (cwd is ${process.cwd()})`);
}
const changelog = readFileSync(path.join(repoRoot, 'CHANGELOG.md'), 'utf8');
const m = /^## \[(\d+\.\d+\.\d+)\]/m.exec(changelog);
if (!m) throw new Error('CHANGELOG.md names no released version');

export const releasedVersion: string = m[1];
