import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';
import { inlineAsset } from './src/lib/preload.ts';

// Vite loads this file in Node (or Bun); the UI's tsconfig carries no Node
// types, so the one global it reads is declared here.
declare const process: { env: Record<string, string | undefined> };

// In development (`bun run dev`, which `make up` starts) the UI is served by
// Vite with hot reload, and the engine is elsewhere: Vite forwards the
// Connect calls, the plain-HTTP routes (/v1/: the trigger and callback
// commands the UI shows are built from the page's origin) and the WebSockets
// to it, so the browser reaches the API on the page's own origin, as it does
// when the engine serves the built UI. ORK_SERVER names the engine, as it does for orkctl. The Host header is
// passed through unchanged, so the engine sees a socket's Origin match its
// Host and admits it as same-origin. `vite preview` uses the same proxy.
const engine = process.env.ORK_SERVER || 'http://127.0.0.1:8080';
const proxy = {
	'/orkstack.v1.': { target: engine },
	'/v1/': { target: engine },
	'/ws/': { target: engine, ws: true }
};

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

export default defineConfig({
	plugins: [
		tailwindcss(),
		sveltekit({
			preprocess: vitePreprocess(),
			adapter: adapter({ fallback: 'index.html', precompress: true }),
			...(epoch ? { version: { name: epoch } } : {})
		})
	],
	server: { proxy },
	preview: { proxy },
	build: {
		// No font face is inlined into the stylesheet (src/lib/preload.ts).
		assetsInlineLimit: inlineAsset
	}
});
