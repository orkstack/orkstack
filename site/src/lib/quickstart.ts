// The quickstart's commands, read from the README's "Quickstart" section at
// build time, so the landing page can never show commands the README does
// not: the README's are the ones scripts/quickstart-smoke.sh runs as written,
// and internal/docscheck holds docs/GETTING_STARTED.md and the tutorial to
// them.
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';

// Astro builds from site/ (bun run build, the site gate, site.yml); bundled
// code cannot use its own location, so the repository is site/'s parent.
const repoRoot = path.resolve(process.cwd(), '..');
if (!existsSync(path.join(repoRoot, 'ui', 'src', 'tokens.css'))) {
	throw new Error(`build the site from site/ (cwd is ${process.cwd()})`);
}
const readme = readFileSync(path.join(repoRoot, 'README.md'), 'utf8');

const section = readme.split(/^## /m).find((part) => part.startsWith('Quickstart\n'));
if (!section) throw new Error('README.md has no "## Quickstart" section');
const fence = /^```bash\n([\s\S]*?)^```$/m.exec(section);
if (!fence) throw new Error('README.md\'s Quickstart has no ```bash block');

/** The README's Quickstart commands, one per line, in order. */
export const quickstartCommands: string[] = fence[1].split('\n').filter((line) => line.trim() !== '');
