import { sveltekit } from '@sveltejs/kit/vite';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vite';
import { inlineAsset } from './src/lib/preload';

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

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: { proxy },
	preview: { proxy },
	build: {
		// No font face is inlined into the stylesheet (src/lib/preload.ts).
		assetsInlineLimit: inlineAsset
	}
});
