// The generated screenshots and recordings (make site-media), read through
// their manifest so a page names an item, never a file. A missing item fails
// the build, unless SITE_ALLOW_MISSING_MEDIA=1, which only the first
// generation of the media sets: then the page draws an empty, labelled frame.
import { existsSync } from 'node:fs';
import path from 'node:path';

interface Item {
	alt: string;
	caption: string;
	/** A planned or finished recording's name and length, as the page prints them. */
	title?: string;
	duration?: string;
	/** A recording's WebVTT captions, in public/media. */
	captions?: string;
	/** A screenshot's size in CSS pixels: its one-x file's, half its two-x file's. */
	width?: number;
	height?: number;
	light?: string;
	light2x?: string;
	dark?: string;
	dark2x?: string;
	webm?: string;
	mp4?: string;
	poster?: string;
	webmDark?: string;
	mp4Dark?: string;
	posterDark?: string;
}

const manifests = import.meta.glob<{ items: Record<string, Item> }>('../assets/media/manifest.json', {
	eager: true,
	import: 'default',
});
const items: Record<string, Item> = Object.values(manifests)[0]?.items ?? {};
const allowMissing = import.meta.env.SITE_ALLOW_MISSING_MEDIA === '1' || process.env.SITE_ALLOW_MISSING_MEDIA === '1';

/** One theme of a screenshot: its one-x file, and the srcset that offers the two-x one. */
export interface ShotTheme {
	src: string;
	srcset?: string;
}

export interface Shot {
	alt: string;
	caption: string;
	/** The size to draw it at, in CSS pixels: one to one. */
	width: number;
	height: number;
	light?: ShotTheme;
	dark?: ShotTheme;
}

export function shot(name: string): Shot {
	const item = items[name];
	if (!item) {
		if (allowMissing) return { alt: `${name} (not generated yet)`, caption: name, width: 1600, height: 1000 };
		throw new Error(`media ${name} has no entry in src/assets/media/manifest.json (run make site-media)`);
	}
	const { width, height } = item;
	if (!width || !height) throw new Error(`media ${name} has no width and height in src/assets/media/manifest.json (run make site-media)`);
	const theme = (one: string | undefined, two: string | undefined): ShotTheme | undefined => {
		const src = served(one, name);
		const src2x = served(two, name);
		if (!src) return undefined;
		return { src, srcset: src2x ? `${src} ${width}w, ${src2x} ${width * 2}w` : undefined };
	};
	return {
		alt: item.alt,
		caption: item.caption,
		width,
		height,
		light: theme(item.light, item.light2x),
		dark: theme(item.dark, item.dark2x),
	};
}

export interface ClipTheme {
	webm?: string;
	mp4?: string;
	poster?: string;
}
export interface Clip {
	alt: string;
	caption: string;
	/** Its size in CSS pixels: half the recording's, which is drawn at two device pixels per CSS pixel. */
	width: number;
	height: number;
	/** A terminal recording (VHS), as against one of the UI. */
	terminal: boolean;
	light: ClipTheme;
	dark: ClipTheme;
}

// Screenshots and recordings are served as they are, from public/media: a
// screenshot is already the size it is drawn at, and Astro's image pipeline
// would only re-encode it, and a docs page embeds them by URL.
const publicMedia = path.resolve(process.cwd(), 'public', 'media');

function served(name: string | undefined, item: string): string | undefined {
	if (!name) return undefined;
	if (!existsSync(path.join(publicMedia, name))) {
		if (allowMissing) return undefined;
		throw new Error(`media ${item}: ${name} is missing from public/media (run make site-media)`);
	}
	return `${import.meta.env.BASE_URL.replace(/\/$/, '')}/media/${name}`;
}

export interface Video {
	title: string;
	alt: string;
	caption: string;
	/** The recording's length as the page prints it, such as 0:45. */
	duration: string;
	width: number;
	height: number;
	/** The recording exists: without it, the page shows the poster and says it is planned. */
	recorded: boolean;
	captions?: string;
	light: ClipTheme;
	dark: ClipTheme;
}

/** A place for a recording on the landing page: planned until its files exist. */
export function video(name: string): Video {
	const item = items[name];
	if (!item) throw new Error(`media ${name} has no entry in src/assets/media/manifest.json`);
	const { width, height, title, duration } = item;
	if (!width || !height || !title || !duration) throw new Error(`media ${name} needs a width, height, title and duration in src/assets/media/manifest.json`);
	const light = { webm: served(item.webm, name), mp4: served(item.mp4, name), poster: served(item.poster, name) };
	const dark = { webm: served(item.webmDark, name), mp4: served(item.mp4Dark, name), poster: served(item.posterDark, name) };
	const recorded = Boolean((light.webm || light.mp4) && (dark.webm || dark.mp4));
	// A recording never ships without its captions (AGENTS' site rules, the brief's §11).
	const captions = served(item.captions, name);
	if (recorded && !captions) throw new Error(`media ${name} is recorded but has no captions file`);
	return { title, alt: item.alt, caption: item.caption, duration, width, height, recorded, captions, light, dark };
}
