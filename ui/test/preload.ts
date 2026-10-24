import { plugin } from 'bun';

// `bun test` runs these modules outside Vite, so SvelteKit's virtual modules
// have to be provided. Only one is reachable from the code under test:
// $env/dynamic/public, which ui/src/lib/ws.ts reads to decide where a
// WebSocket goes, and $app/navigation, which ui/src/lib/dirtyGuard.ts
// registers with.
//
// Naming an engine here is what keeps the unit tests off `location`: with
// PUBLIC_ENGINE_WS_URL set, wsBaseUrl() never touches the DOM, and the URL a
// test asserts on is the URL a deployment with a configured engine produces.
plugin({
	name: 'sveltekit-env',
	setup(build) {
		// $app/navigation's beforeNavigate only records its callback, so a
		// test can play a navigation through it (dirtyGuard.test.ts).
		build.module('$app/navigation', () => ({
			exports: {
				beforeNavigate: (fn: unknown) => {
					(globalThis as unknown as { __beforeNavigate: unknown[] }).__beforeNavigate ??= [];
					(globalThis as unknown as { __beforeNavigate: unknown[] }).__beforeNavigate.push(fn);
				}
			},
			loader: 'object'
		}));
		build.module('$env/dynamic/public', () => ({
			exports: { env: { PUBLIC_ENGINE_WS_URL: 'ws://127.0.0.1:19999' } },
			loader: 'object'
		}));
	}
});
