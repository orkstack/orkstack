import { defineCollection } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

// The docs are written by scripts/sync-docs.mjs from the repository's own
// markdown (docs.manifest.json); nothing under src/content/docs is authored.
export const collections = {
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
};
