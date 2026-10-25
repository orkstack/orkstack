// The docs pages' citations of the repository (scripts/citations.mjs): a page
// written for the site cites the repository docs it summarises in its
// See also, and every citation and anchor names a file and heading that
// exist. Each case is a planted page, so the check is shown to bite.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { checkCitations, checkAnchor, uncitedDocs, leftoverMarkers } from '../scripts/citations.mjs';

const REPO = 'https://github.com/orkstack/orkstack';
const BRANCH = 'main';
const url = (p) => `${REPO}/blob/${BRANCH}/${p}`;

function repo() {
	const root = mkdtempSync(path.join(tmpdir(), 'citations-'));
	mkdirSync(path.join(root, 'docs'));
	writeFileSync(path.join(root, 'docs', 'DSL.md'), '# DSL\n\n## Fan-out (`each:`)\n\n```md\n## Not a heading\n```\n');
	writeFileSync(path.join(root, 'README.md'), '# Read me\n');
	return root;
}
const check = (text, root = repo()) => checkCitations({ rel: 'site/content/x.md', text, repoRoot: root, repo: REPO, branch: BRANCH });

test('a page that cites a doc and heading in its See also passes, and names what it cites', () => {
	const { problems, cited } = check(`# X\n\nText.\n\n## See also\n\n- [DSL](${url('docs/DSL.md#fan-out-each')})\n`);
	assert.deepEqual(problems, []);
	assert.deepEqual([...cited], ['docs/DSL.md']);
});

test('a page with no See also, or a See also that cites nothing, is refused', () => {
	assert.match(check('# X\n\nText.\n').problems.join('\n'), /no "## See also"/);
	assert.match(check('# X\n\n## See also\n\n- [Fan out](/docs/how-to/fan-out/)\n').problems.join('\n'), /cites no repository doc in its See also/);
});

test('a citation inside a code block or a code span does not count', () => {
	const fenced = `# X\n\n## See also\n\n\`\`\`text\n${url('docs/DSL.md')}\n\`\`\`\n`;
	assert.match(check(fenced).problems.join('\n'), /cites no repository doc in its See also/);
	const span = `# X\n\n## See also\n\n- \`${url('docs/DSL.md')}\`\n`;
	assert.match(check(span).problems.join('\n'), /cites no repository doc in its See also/);
	const headingInFence = `# X\n\n\`\`\`md\n## See also\n\`\`\`\n\n- [DSL](${url('docs/DSL.md')})\n`;
	assert.match(check(headingInFence).problems.join('\n'), /no "## See also"/);
});

test('a citation of a file or heading that does not exist is refused', () => {
	const see = (u) => `# X\n\n## See also\n\n- [x](${u})\n`;
	assert.match(check(see(url('docs/NOPE.md'))).problems.join('\n'), /docs\/NOPE\.md, which does not exist/);
	assert.match(check(see(url('docs/DSL.md#no-such-heading'))).problems.join('\n'), /a heading docs\/DSL\.md does not have/);
	assert.match(check(see(url('docs/DSL.md#not-a-heading'))).problems.join('\n'), /does not have/, 'a heading inside a fence is no heading');
	assert.match(check(see(url('docs/../../etc/passwd'))).problems.join('\n'), /leaves the repository/);
	assert.match(check(see(url('docs/%zz.md'))).problems.join('\n'), /malformed/);
});

test('GitHub forms of a valid citation are accepted', () => {
	const see = (u) => `# X\n\n## See also\n\n- [x](${u}) and <${u}>.\n`;
	for (const u of [url('docs/DSL.md?plain=1#fan-out-each'), url('docs/DSL.md#L3'), url('docs/DSL.md#L3-L5'), url('README.md')]) {
		assert.deepEqual(check(see(u)).problems, [], u);
	}
});

test('an anchor into a markdown file is checked against its headings', () => {
	const root = repo();
	assert.equal(checkAnchor(root, 'docs/DSL.md', 'fan-out-each'), null);
	assert.match(checkAnchor(root, 'docs/DSL.md', 'gone'), /does not have/);
	assert.equal(checkAnchor(root, 'docs/DSL.md', 'L2'), null);
	assert.equal(checkAnchor(root, 'README.md', ''), null);
});

test('every doc under docs/ must be cited', () => {
	assert.deepEqual(uncitedDocs(new Set(['docs/A.md']), ['docs/A.md', 'docs/B.md', 'docs/schemas']), ['docs/B.md']);
});

test('a marker left after replacement is reported', () => {
	assert.deepEqual(leftoverMarkers('a <!-- site-diagram: Architecture --> b <!-- site-media: ui-x --> c'), [
		'site-diagram: Architecture',
		'site-media: ui-x',
	]);
	assert.deepEqual(leftoverMarkers('<!-- an ordinary comment -->'), []);
});
