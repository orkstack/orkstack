import { defineCollection } from 'astro:content';
import { docsLoader, i18nLoader } from '@astrojs/starlight/loaders';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';

// The docs are written by scripts/sync-docs.mjs from the repository's own
// markdown (docs.manifest.json); nothing under src/content/docs is authored.
// src/content/i18n holds the few of Starlight's words the site changes.
export const collections = {
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
	i18n: defineCollection({ loader: i18nLoader(), schema: i18nSchema() }),
};
