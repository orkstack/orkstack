import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';
import { inlineAsset } from './src/lib/preload';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	build: {
		// No font face is inlined into the stylesheet (src/lib/preload.ts).
		assetsInlineLimit: inlineAsset
	}
});
