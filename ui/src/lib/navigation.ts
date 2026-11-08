// The app's map: every page a person navigates to, which of the two modes it
// belongs to, and the words each place is called by. One list, read by the
// top bar, the Operate strip, the menu drawer, the command palette and the
// title block, so a page renamed here is renamed everywhere at once.
//
// Two modes, because the people who use this app arrive with two jobs:
//   Build   — author and apply Orkflows, see what is running and why it
//             ended: the dashboard, Runs, Orkflows and Apply;
//   Operate — run the deployment: every page under /admin.

import type { IconName } from './icons';

export type Mode = 'build' | 'operate';

export interface NavPage {
	/** Stable id: the sidebar's `nav-{id}` / `admin-tab-{id}` and the palette's command id. */
	id: string;
	href: string;
	/** What the link says. */
	label: string;
	/** What the palette's "Go to …" command and the title block call it. */
	title: string;
	mode: Mode;
	/** The sidebar's glyph (#lib/icons.ts). */
	icon: IconName;
}

/** Build: the dashboard and the three daily surfaces. */
export const BUILD_PAGES: readonly NavPage[] = [
	{ id: 'overview', href: '/', label: 'Overview', title: 'Overview', mode: 'build', icon: 'home' },
	{ id: 'runs', href: '/runs', label: 'Runs', title: 'Runs', mode: 'build', icon: 'runs' },
	{ id: 'orkflows', href: '/orkflows', label: 'Orkflows', title: 'Orkflows', mode: 'build', icon: 'flow' },
	{ id: 'apply', href: '/apply', label: 'Apply', title: 'Apply an Orkflow', mode: 'build', icon: 'apply' }
];

/** Operate: the deployment's own pages, in the order the sidebar shows them. */
export const OPERATE_PAGES: readonly NavPage[] = [
	{ id: 'overview', href: '/admin', label: 'Overview', title: 'Operate overview', mode: 'operate', icon: 'home' },
	{ id: 'pools', href: '/admin/pools', label: 'Pools', title: 'Pools', mode: 'operate', icon: 'pool' },
	{ id: 'activities', href: '/admin/activities', label: 'Activities', title: 'Activities', mode: 'operate', icon: 'act' },
	{ id: 'namespaces', href: '/admin/namespaces', label: 'Namespaces', title: 'Namespaces', mode: 'operate', icon: 'ns' },
	{ id: 'releases', href: '/admin/releases', label: 'Releases', title: 'Releases', mode: 'operate', icon: 'rel' },
	{ id: 'webhooks', href: '/admin/webhooks', label: 'Webhooks', title: 'Webhooks', mode: 'operate', icon: 'hook' },
	{ id: 'automation', href: '/admin/automation', label: 'Automation', title: 'Automation', mode: 'operate', icon: 'doc' },
	{ id: 'retention', href: '/admin/retention', label: 'Retention', title: 'Retention', mode: 'operate', icon: 'clock' },
	{ id: 'usage', href: '/admin/usage', label: 'Usage', title: 'Usage', mode: 'operate', icon: 'usage' },
	{ id: 'cache', href: '/admin/cache', label: 'Cache', title: 'Cache', mode: 'operate', icon: 'cache' },
	{ id: 'image-policy', href: '/admin/image-policy', label: 'Image policy', title: 'Image policy', mode: 'operate', icon: 'shield' },
	{ id: 'audit', href: '/admin/audit', label: 'Audit', title: 'Audit', mode: 'operate', icon: 'audit' }
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
