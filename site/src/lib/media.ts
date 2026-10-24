// The generated screenshots and recordings (make site-media), read through
// their manifest so a page names an item, never a file. A missing item fails
// the build, unless SITE_ALLOW_MISSING_MEDIA=1, which only the first
// generation of the media sets: then the page draws an empty, labelled frame.
import type { ImageMetadata } from 'astro';
import { existsSync } from 'node:fs';
import path from 'node:path';

interface Item {
	alt: string;
	caption: string;
	light?: string;
	dark?: string;
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
const images = import.meta.glob<ImageMetadata>('../assets/media/*.{png,webp,avif,jpg}', {
	eager: true,
	import: 'default',
});

const items: Record<string, Item> = Object.values(manifests)[0]?.items ?? {};
const allowMissing = import.meta.env.SITE_ALLOW_MISSING_MEDIA === '1' || process.env.SITE_ALLOW_MISSING_MEDIA === '1';

function file<T>(table: Record<string, T>, name: string | undefined, item: string): T | undefined {
	if (!name) return undefined;
	const hit = table[`../assets/media/${name}`];
	if (!hit && !allowMissing) throw new Error(`media ${item}: ${name} is missing from src/assets/media (run make site-media)`);
	return hit;
}

export interface Shot {
	alt: string;
	caption: string;
	light?: ImageMetadata;
	dark?: ImageMetadata;
}

export function shot(name: string): Shot {
	const item = items[name];
	if (!item) {
		if (allowMissing) return { alt: `${name} (not generated yet)`, caption: name };
		throw new Error(`media ${name} has no entry in src/assets/media/manifest.json (run make site-media)`);
	}
	return {
		alt: item.alt,
		caption: item.caption,
		light: file(images, item.light, name),
		dark: file(images, item.dark, name),
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

// Recordings are served as they are, from public/media: Astro's image
// pipeline has nothing to do for a video, and a docs page embeds them by URL.
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
