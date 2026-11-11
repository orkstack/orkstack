// The app's icons: one family, drawn on a 16-unit grid with a 1.4 stroke,
// round caps and currentColor (the Harbour set; activity kinds are a-*).
// Each is a list of SVG elements. IconSprite.svelte draws every one once,
// as a <symbol>, and Icon.svelte points at it with <use>, so an icon costs
// one small element wherever it appears and no markup is ever parsed (the
// UI's strict content security policy: no {@html}). Every icon is decoration
// beside a word or an accessible name, never the only label.

export type IconTag = 'path' | 'circle' | 'rect' | 'ellipse';
export type IconElement = readonly [IconTag, Readonly<Record<string, string>>];

export const ICONS = {
	edit: [['path', {'d': 'M10.5 3 13 5.5 5.5 13 2.5 13.5 3 10.5ZM9 4.5 11.5 7', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	runs: [['path', {'d': 'M5.6 2.6v6.2l5.3-3.1z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}], ['path', {'d': 'M2 13h12', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}], ['circle', {'cx': '5.5', 'cy': '13', 'r': '1.7', 'fill': 'currentColor'}]],
	flow: [['circle', {'cx': '3.5', 'cy': '4', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '12.5', 'cy': '4', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '8', 'cy': '12.5', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M5.5 4h5M4.6 5.7 7 10.6M11.4 5.7 9 10.6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none'}]],
	apply: [['path', {'d': 'M3.5 8.5 6.5 11.5 12.5 4.5', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	home: [['path', {'d': 'M2.5 7 8 2.5 13.5 7v6.5h-11z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	pool: [['rect', {'x': '2', 'y': '2.5', 'width': '12', 'height': '4.5', 'rx': '1.2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['rect', {'x': '2', 'y': '9', 'width': '12', 'height': '4.5', 'rx': '1.2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M4.5 4.75h1.5M4.5 11.25h1.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	ns: [['path', {'d': 'M2 5.5h12v8H2zM2 5.5l1.5-3h4l1 3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	rel: [['path', {'d': 'M8 2v12M3 6l5-4 5 4', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	act: [['path', {'d': 'M8 1.8 13.5 5v6L8 14.2 2.5 11V5z M2.5 5 8 8.2 13.5 5M8 8.2v6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	hook: [['path', {'d': 'M6 3.5a2.5 2.5 0 1 1 2 4.4L6 12M10 12.5a2.5 2.5 0 1 1 0-0.1M3.5 12.5h5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}]],
	doc: [['path', {'d': 'M4 1.8h5.5L12.5 5v9.2H4z M9.5 1.8V5h3M6 8.5h4.5M6 11h4.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	clock: [['circle', {'cx': '8', 'cy': '8', 'r': '6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M8 4.5V8l2.5 1.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}]],
	usage: [['path', {'d': 'M2 13.5h12M4 11V7M8 11V3.5M12 11V8.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}]],
	cache: [['ellipse', {'cx': '8', 'cy': '4', 'rx': '5.5', 'ry': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M2.5 4v8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2V4M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none'}]],
	shield: [['path', {'d': 'M8 1.8 13 3.8v4c0 3-2.2 5.3-5 6.4-2.8-1.1-5-3.4-5-6.4v-4z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	audit: [['path', {'d': 'M3 2.5h10v11H3zM5.5 5.5h5M5.5 8h5M5.5 10.5h3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none'}]],
	search: [['circle', {'cx': '7', 'cy': '7', 'r': '4.5', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'm10.5 10.5 3 3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	copy: [['rect', {'x': '5', 'y': '5', 'width': '8.5', 'height': '8.5', 'rx': '1.5', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M3 10.5V3.5A1 1 0 0 1 4 2.5h6.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none'}]],
	chev: [['path', {'d': 'm4.5 6 3.5 3.5L11.5 6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	filter: [['path', {'d': 'M2 3.5h12L9.5 9v4l-3 1V9z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linejoin': 'round'}]],
	alert: [['path', {'d': 'M8 1.8 15 14H1z', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}], ['path', {'d': 'M8 6.5v3.5M8 12v.2', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	hourglass: [['path', {'d': 'M4 2h8M4 14h8M5 2c0 3 6 3.5 6 6s-6 3-6 6M11 2c0 3-6 3.5-6 6s6 3 6 6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}]],
	play: [['path', {'d': 'M5.2 3.4v9.2L12.4 8z', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}]],
	pause: [['path', {'d': 'M5.5 3.5v9M10.5 3.5v9', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	info: [['circle', {'cx': '8', 'cy': '8', 'r': '6.2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M8 7.2v4M8 4.8v.2', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	menu: [['path', {'d': 'M2.5 4h11M2.5 8h11M2.5 12h11', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	more: [['circle', {'cx': '3', 'cy': '8', 'r': '1.3', 'fill': 'currentColor'}], ['circle', {'cx': '8', 'cy': '8', 'r': '1.3', 'fill': 'currentColor'}], ['circle', {'cx': '13', 'cy': '8', 'r': '1.3', 'fill': 'currentColor'}]],
	term: [['rect', {'x': '1.75', 'y': '2.75', 'width': '12.5', 'height': '10.5', 'rx': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'm4.6 6.2 2 1.8-2 1.8M8.3 10.2h3.1', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	kbd: [['rect', {'x': '1.5', 'y': '3.5', 'width': '13', 'height': '9', 'rx': '1.8', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M4 6.5h1M7.5 6.5h1M11 6.5h1M4.5 9.5h7', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	'a-default': [['path', {'d': 'M8 1.8 13.5 5v6L8 14.2 2.5 11V5zM2.5 5 8 8.2 13.5 5M8 8.2v6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}]],
	'a-http': [['circle', {'cx': '8', 'cy': '8', 'r': '6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['ellipse', {'cx': '8', 'cy': '8', 'rx': '2.5', 'ry': '6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M2.2 8h11.6', 'stroke': 'currentColor', 'stroke-width': '1.4'}]],
	'a-shell': [['rect', {'x': '1.75', 'y': '1.75', 'width': '12.5', 'height': '12.5', 'rx': '2.5', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M10 5.6c-.4-.6-1.1-.9-2-.9-1.1 0-1.9.6-1.9 1.4 0 .9.8 1.2 1.9 1.5 1.1.3 1.9.7 1.9 1.6 0 .9-.8 1.5-1.9 1.5-.9 0-1.6-.3-2-.9M8 3.6v1.1M8 11.2v1.2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.3', 'stroke-linecap': 'round'}]],
	'a-file': [['path', {'d': 'M4 1.8h5L12.5 5.3v8.9H4z', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}], ['path', {'d': 'M9 1.8v3.5h3.5', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}]],
	'a-json': [['path', {'d': 'M6 2.5c-1.6 0-2 .8-2 2v1.6c0 .9-.5 1.4-1.5 1.9 1 .5 1.5 1 1.5 1.9v1.6c0 1.2.4 2 2 2M10 2.5c1.6 0 2 .8 2 2v1.6c0 .9.5 1.4 1.5 1.9-1 .5-1.5 1-1.5 1.9v1.6c0 1.2-.4 2-2 2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	'a-git': [['circle', {'cx': '4.5', 'cy': '3.5', 'r': '1.7', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '4.5', 'cy': '12.5', 'r': '1.7', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '11.5', 'cy': '5.5', 'r': '1.7', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M4.5 5.2v5.6M11.5 7.2c0 2.6-2.2 3.2-5.4 4.1', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	'a-template': [['rect', {'x': '2', 'y': '2.5', 'width': '12', 'height': '11', 'rx': '1.8', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M2 6h12M6.2 6v7.5', 'stroke': 'currentColor', 'stroke-width': '1.4'}]],
	'a-orkflow': [['circle', {'cx': '3.5', 'cy': '4', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '12.5', 'cy': '4', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['circle', {'cx': '8', 'cy': '12.5', 'r': '2', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M5.5 4h5M4.6 5.7 7 10.6M11.4 5.7 9 10.6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}]],
	'a-await': [['path', {'d': 'M2 9.5 3.6 3.5h8.8L14 9.5v3.5H2z', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}], ['path', {'d': 'M2 9.5h3.4l1 1.6h3.2l1-1.6H14', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linejoin': 'round'}]],
	'a-sleep': [['circle', {'cx': '8', 'cy': '8', 'r': '6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M8 4.5V8l2.5 1.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round'}]],
	retry: [['path', {'d': 'M13.3 8.6A5.4 5.4 0 1 1 11.6 4', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}], ['path', {'d': 'M12.2 1.4v3h-3', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	ban: [['circle', {'cx': '8', 'cy': '8', 'r': '6', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4'}], ['path', {'d': 'M3.8 12.2 12.2 3.8', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}]],
	trash: [['path', {'d': 'M2.8 4.2h10.4M6 4.2V3a.8.8 0 0 1 .8-.8h2.4a.8.8 0 0 1 .8.8v1.2M4.1 4.2l.6 8.7a.9.9 0 0 0 .9.9h4.8a.9.9 0 0 0 .9-.9l.6-8.7M6.8 7.2v3.6M9.2 7.2v3.6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	close: [['path', {'d': 'M4 4l8 8M12 4l-8 8', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	'chev-up': [['path', {'d': 'm4.5 10 3.5-3.5L11.5 10', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	'chev-left': [['path', {'d': 'M10 4.5 6.5 8l3.5 3.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	'chev-right': [['path', {'d': 'M6 4.5 9.5 8 6 11.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	external: [['path', {'d': 'M9.5 2.5h4v4M13.5 2.5 8 8M11.5 9.5v3a1 1 0 0 1-1 1h-7a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	sun: [['circle', {'cx': '8', 'cy': '8', 'r': '2.6', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}], ['path', {'d': 'M8 1.5v1.5M8 13v1.5M1.5 8H3M13 8h1.5M3.4 3.4l1 1M11.6 11.6l1 1M3.4 12.6l1-1M11.6 4.4l1-1', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	moon: [['path', {'d': 'M13 9.6A5.5 5.5 0 0 1 6.4 3 5.5 5.5 0 1 0 13 9.6z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	monitor: [['rect', {'x': '1.75', 'y': '2.5', 'width': '12.5', 'height': '8.5', 'rx': '1.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}], ['path', {'d': 'M5.5 14h5M8 11v3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	delta: [['path', {'d': 'M8 2.8 13.5 12.5h-11z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	updated: [['path', {'d': 'M8 2a6 6 0 1 1-5.2 3', 'fill': 'none', 'stroke': 'currentColor', 'stroke-width': '1.4', 'stroke-linecap': 'round'}], ['circle', {'cx': '8', 'cy': '8', 'r': '2', 'fill': 'currentColor'}]],
	download: [['path', {'d': 'M8 2.5v8M4.5 7.5 8 11l3.5-3.5M2.5 13.5h11', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	stop: [['rect', {'x': '4', 'y': '4', 'width': '8', 'height': '8', 'rx': '1.5', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	compare: [['path', {'d': 'M5.5 2.5v11M10.5 2.5v11M2.5 5.5h3M10.5 10.5h3', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	evidence: [['path', {'d': 'M8 1.8 13 3.8v4c0 3-2.2 5.3-5 6.4-2.8-1.1-5-3.4-5-6.4v-4z', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}], ['path', {'d': 'm5.8 8 1.6 1.6L10.4 6.4', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]],
	events: [['path', {'d': 'M2.5 4h2M2.5 8h2M2.5 12h2M7 4h6.5M7 8h6.5M7 12h4', 'stroke': 'currentColor', 'stroke-width': '1.4', 'fill': 'none', 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}]]
} as const;

// Every entry is a list of elements (checked here, not by `satisfies`, so the
// object stays the plain `as const` literal site/scripts/sync-docs.mjs reads).
const checked: Readonly<Record<string, readonly IconElement[]>> = ICONS;
void checked;

export type IconName = keyof typeof ICONS;

/** The activity kinds the UI draws an icon for without asking the image. */
export const ACTIVITY_KINDS = ['default', 'http', 'shell', 'file', 'json', 'git', 'template', 'orkflow', 'await', 'sleep'] as const;
export type ActivityKind = (typeof ACTIVITY_KINDS)[number];
