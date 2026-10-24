// Where the site is served and what it links to. One place, read by
// astro.config.mjs, the docs sync and the landing page.
//
// GitHub Pages serves a project site at https://<owner>.github.io/<repo>/, so
// every route carries the repository's name as its base. A custom domain is a
// change here (SITE, and BASE '/').
export const SITE = 'https://orkstack.github.io';
export const BASE = '/orkstack';
export const REPO = 'https://github.com/orkstack/orkstack';
export const BRANCH = 'main';

/** A route under the base, always with a trailing slash. */
export function route(path = '') {
	const clean = path.replace(/^\/+|\/+$/g, '');
	return clean ? `${BASE}/${clean}/` : `${BASE}/`;
}

/** A repository path on GitHub: a file's blob, or a directory's tree. */
export function repoUrl(path, { dir = false } = {}) {
	return `${REPO}/${dir ? 'tree' : 'blob'}/${BRANCH}/${path.replace(/^\/+/, '')}`;
}
