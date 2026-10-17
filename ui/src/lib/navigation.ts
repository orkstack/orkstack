// The app's map: every page a person navigates to, which of the two modes it
// belongs to, and the words each place is called by. One list, read by the
// top bar, the Operate strip, the menu drawer, the command palette and the
// title block, so a page renamed here is renamed everywhere at once.
//
// Two modes, because the people who use this app arrive with two jobs:
//   Build   — author and apply Orkflows, see what is running and why it
//             ended: the dashboard, Runs, Orkflows and Apply;
//   Operate — run the deployment: every page under /admin.

export type Mode = 'build' | 'operate';

export interface NavPage {
	/** Stable id: the Operate strip's `admin-tab-{id}` and the palette's command id. */
	id: string;
	href: string;
	/** What the link says. */
	label: string;
	/** What the palette's "Go to …" command and the title block call it. */
	title: string;
	mode: Mode;
}

/** Build: the dashboard and the three daily surfaces. */
export const BUILD_PAGES: readonly NavPage[] = [
	{ id: 'overview', href: '/', label: 'Overview', title: 'Overview', mode: 'build' },
	{ id: 'runs', href: '/runs', label: 'Runs', title: 'Runs', mode: 'build' },
	{ id: 'orkflows', href: '/orkflows', label: 'Orkflows', title: 'Orkflows', mode: 'build' },
	{ id: 'apply', href: '/apply', label: 'Apply', title: 'Apply an Orkflow', mode: 'build' }
];

/** Operate: the deployment's own pages, in the order the strip shows them. */
export const OPERATE_PAGES: readonly NavPage[] = [
	{ id: 'overview', href: '/admin', label: 'Overview', title: 'Operate overview', mode: 'operate' },
	{ id: 'namespaces', href: '/admin/namespaces', label: 'Namespaces', title: 'Namespaces', mode: 'operate' },
	{ id: 'pools', href: '/admin/pools', label: 'Pools', title: 'Pools', mode: 'operate' },
	{ id: 'releases', href: '/admin/releases', label: 'Releases', title: 'Releases', mode: 'operate' },
	{ id: 'activities', href: '/admin/activities', label: 'Activities', title: 'Activities', mode: 'operate' },
	{ id: 'webhooks', href: '/admin/webhooks', label: 'Webhooks', title: 'Webhooks', mode: 'operate' },
	{ id: 'automation', href: '/admin/automation', label: 'Automation', title: 'Automation', mode: 'operate' },
	{ id: 'retention', href: '/admin/retention', label: 'Retention', title: 'Retention', mode: 'operate' },
	{ id: 'usage', href: '/admin/usage', label: 'Usage', title: 'Usage', mode: 'operate' },
	{ id: 'cache', href: '/admin/cache', label: 'Cache', title: 'Cache', mode: 'operate' },
	{ id: 'image-policy', href: '/admin/image-policy', label: 'Image policy', title: 'Image policy', mode: 'operate' },
	{ id: 'audit', href: '/admin/audit', label: 'Audit', title: 'Audit', mode: 'operate' }
];

/** The mode a path belongs to: everything under /admin is Operate. */
export function modeOf(pathname: string): Mode {
	return pathname === '/admin' || pathname.startsWith('/admin/') ? 'operate' : 'build';
}

// `/` and `/admin` are prefixes of every page in their mode, so they match
// only exactly; every other page also owns everything under it.
function owns(page: NavPage, pathname: string): boolean {
	if (page.href === '/' || page.href === '/admin') return pathname === page.href;
	return pathname === page.href || pathname.startsWith(page.href + '/');
}

/** The Build page a path is on, if any (`/runs/ork-…` is on Runs). */
export function buildPageOf(pathname: string): NavPage | undefined {
	return modeOf(pathname) === 'build' ? BUILD_PAGES.find((p) => owns(p, pathname)) : undefined;
}

/** The Operate page a path is on, if any. */
export function operatePageOf(pathname: string): NavPage | undefined {
	return modeOf(pathname) === 'operate' ? OPERATE_PAGES.find((p) => owns(p, pathname)) : undefined;
}

/** The page a path is on, in either mode. */
export function pageOf(pathname: string): NavPage | undefined {
	return buildPageOf(pathname) ?? operatePageOf(pathname);
}

/**
 * What an Operate page's figures cover: the deployment as a whole, the
 * namespace in the picker, or every namespace (Audit shows all by default and
 * filters to one only on request). The admin layout's own caption says the
 * same thing in prose.
 */
export function operateScope(pageId: string): string {
	switch (pageId) {
		case 'pools':
		case 'automation':
			return 'This namespace';
		case 'audit':
			return 'All namespaces';
		default:
			return 'Deployment-wide';
	}
}
