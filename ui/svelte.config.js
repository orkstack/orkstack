import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

// The UI is a static single-page app: `bun run build` writes ui/build, which
// the engine embeds and serves at / on its API listener beside the Connect
// API and the WebSocket bridge (internal/webui). There is no server of the
// UI's own: every route renders in the browser from index.html, the SPA
// fallback, and the example gallery is written into the build as files
// (src/routes/apply/examples*). precompress writes a .br and a .gz twin of
// each HTML, JavaScript, CSS, JSON and SVG file, which the engine serves to a
// browser that accepts one.
//
// The build's version (_app/version.json, which tells an open tab a new build
// was deployed) is the commit's time when SOURCE_DATE_EPOCH is set, as the
// release sets it, so the same commit builds the same bytes; otherwise
// SvelteKit's default, the time of the build.
const epoch = process.env.SOURCE_DATE_EPOCH;

/** @type {import('@sveltejs/kit').Config} */
const config = {
	preprocess: vitePreprocess(),
	kit: {
		adapter: adapter({ fallback: 'index.html', precompress: true }),
		...(epoch ? { version: { name: epoch } } : {})
	}
};

export default config;
