// The generated screenshots and recordings (make site-media), read through
// their manifest so a page names an item, never a file. A missing item fails
// the build, unless SITE_ALLOW_MISSING_MEDIA=1, which only the first
// generation of the media sets: then the page draws an empty, labelled frame.
import { existsSync } from 'node:fs';
import path from 'node:path';

interface Item {
	alt: string;
	caption: string;
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
	srcset: string;
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
		if (!src || !src2x) return undefined;
		return { src, srcset: `${src} ${width}w, ${src2x} ${width * 2}w` };
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

export function clip(name: string): Clip {
	const item = items[name];
	if (!item) {
		if (allowMissing) return { alt: `${name} (not generated yet)`, caption: name, light: {}, dark: {} };
		throw new Error(`media ${name} has no entry in src/assets/media/manifest.json (run make site-media)`);
	}
	return {
		alt: item.alt,
		caption: item.caption,
		light: { webm: served(item.webm, name), mp4: served(item.mp4, name), poster: served(item.poster, name) },
		dark: { webm: served(item.webmDark, name), mp4: served(item.mp4Dark, name), poster: served(item.posterDark, name) },
	};
}
