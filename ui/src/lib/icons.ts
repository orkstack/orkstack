// The app's icons: authored SVG, one 16-unit grid, one 1.5 stroke, round
// caps, drawn in currentColor. Every icon is decoration beside a word or an
// accessible name, never the only label (Icon.svelte renders aria-hidden).
// Each entry is the `d` of one or more paths.

export const ICONS = {
	close: ['M4 4l8 8M12 4l-8 8'],
	search: ['M7 12.5a5.5 5.5 0 1 0 0-11 5.5 5.5 0 0 0 0 11z', 'M11 11l3.5 3.5'],
	menu: ['M2.5 4.5h11M2.5 8h11M2.5 11.5h11'],
	chevronDown: ['M4.5 6.5 8 10l3.5-3.5'],
	chevronUp: ['M4.5 9.5 8 6l3.5 3.5'],
	chevronLeft: ['M9.5 4.5 6 8l3.5 3.5'],
	chevronRight: ['M6.5 4.5 10 8l-3.5 3.5'],
	copy: ['M5.5 5.5h7v7h-7z', 'M3.5 10.5v-7h7'],
	external: ['M9.5 2.5h4v4', 'M13.5 2.5 8 8', 'M11.5 9.5v4h-9v-9h4'],
	check: ['M3 8.5 6.5 12 13 4.5'],
	sun: ['M8 10.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5z', 'M8 1.5v1.5M8 13v1.5M1.5 8H3M13 8h1.5M3.4 3.4l1 1M11.6 11.6l1 1M3.4 12.6l1-1M11.6 4.4l1-1'],
	moon: ['M13 9.6A5.5 5.5 0 0 1 6.4 3 5.5 5.5 0 1 0 13 9.6z'],
	monitor: ['M1.5 2.5h13v8.5h-13z', 'M5.5 14h5M8 11v3'],
	delta: ['M8 3 13.5 12.5h-11z'],
	filter: ['M2 3.5h12L9.5 8.5v4l-3 1.5v-5.5z'],
	download: ['M8 2.5v8', 'M4.5 7.5 8 11l3.5-3.5', 'M2.5 13.5h11'],
	play: ['M5 3v10l8-5z'],
	stop: ['M4 4h8v8H4z'],
	compare: ['M5.5 2.5v11M10.5 2.5v11', 'M2.5 5.5h3M10.5 10.5h3'],
	file: ['M4 1.5h5.5L12.5 4.5v10H4z', 'M9.5 1.5v3h3'],
	clock: ['M8 14.5a6.5 6.5 0 1 0 0-13 6.5 6.5 0 0 0 0 13z', 'M8 4.5V8l2.5 1.5'],
	terminal: ['M2 3h12v10H2z', 'M4.5 6 6.5 8l-2 2M8 10.5h3.5']
} as const;

export type IconName = keyof typeof ICONS;
