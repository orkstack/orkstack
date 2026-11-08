import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
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

// The page's content security policy. SvelteKit writes it into index.html as
// a <meta http-equiv>, with the hash of every inline script it generates (its
// boot script) added to script-src; the engine sends the same policy as a
// header, with frame-ancestors, which only a header can carry
// (internal/webui, PagePolicy). No inline script runs that is not hashed
// here: app.html's theme script, which runs before SvelteKit's, is hashed
// from app.html itself, so editing it moves its hash with it. Scripts do not
// allow 'unsafe-eval': the YAML editor's validators are compiled ahead of time
// (test/gen-validators.ts), and the engine copies this policy
// (docs/SECURITY_MODEL.md says why). Styles allow 'unsafe-inline': Svelte writes style attributes and the editor inserts
// <style> elements at run time, which no build-time hash can name.
const appHtml = readFileSync(new URL('./src/app.html', import.meta.url), 'utf8');
const themeStart = appHtml.indexOf('<script>');
const themeEnd = appHtml.indexOf('</script>');
if (themeStart < 0 || themeEnd < themeStart) {
	throw new Error('vite.config.ts: src/app.html has no inline <script> to hash for the content security policy');
}
const themeScript = appHtml.slice(themeStart + '<script>'.length, themeEnd);
const themeHash: `sha256-${string}` = `sha256-${createHash('sha256').update(themeScript).digest('base64')}`;

export default defineConfig({
	plugins: [
		tailwindcss(),
		sveltekit({
			preprocess: vitePreprocess(),
			adapter: adapter({ fallback: 'index.html', precompress: true }),
			csp: {
				mode: 'hash',
				directives: {
					'default-src': ['self'],
					'script-src': ['self', themeHash],
					'style-src': ['self', 'unsafe-inline'],
					'img-src': ['self', 'data:'],
					'font-src': ['self'],
					'connect-src': ['self'],
					'object-src': ['none'],
					'base-uri': ['none'],
					'form-action': ['self']
				}
			},
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
