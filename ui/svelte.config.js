import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

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
// from app.html itself, so editing it moves its hash with it. Scripts allow
// 'unsafe-eval' for one reason: the YAML editor's schema checks compile
// their validators at run time (ajv, src/lib/yaml/lint.ts and complete.ts),
// from the static generated schemas the build carries. Once neither compiles
// a validator at run time, remove 'unsafe-eval' here and nowhere else (the
// engine copies this policy; docs/SECURITY_MODEL.md says why). Styles allow
// 'unsafe-inline': Svelte writes style attributes and the editor inserts
// <style> elements at run time, which no build-time hash can name.
const appHtml = readFileSync(new URL('./src/app.html', import.meta.url), 'utf8');
const themeStart = appHtml.indexOf('<script>');
const themeEnd = appHtml.indexOf('</script>');
if (themeStart < 0 || themeEnd < themeStart) {
	throw new Error('svelte.config.js: src/app.html has no inline <script> to hash for the content security policy');
}
const themeScript = appHtml.slice(themeStart + '<script>'.length, themeEnd);
const themeHash = `sha256-${createHash('sha256').update(themeScript).digest('base64')}`;

/** @type {import('@sveltejs/kit').Config} */
const config = {
	preprocess: vitePreprocess(),
	kit: {
		adapter: adapter({ fallback: 'index.html', precompress: true }),
		csp: {
			mode: 'hash',
			directives: {
				'default-src': ['self'],
				'script-src': ['self', 'unsafe-eval', themeHash],
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
	}
};

export default config;
