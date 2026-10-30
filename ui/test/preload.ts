import { plugin } from 'bun';

// `bun test` runs these modules outside Vite, so SvelteKit's virtual modules
// have to be provided. Only one is reachable from the code under test:
// $app/navigation, which ui/src/lib/dirtyGuard.ts registers with.
//
// There is no `location` outside a browser either, and ui/src/lib/ws.ts
// reads it to open a socket on the page's own origin: the tests run as a
// page served from 127.0.0.1:19999.
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
	}
});

if (!('location' in globalThis)) {
	Object.defineProperty(globalThis, 'location', {
		value: { protocol: 'http:', host: '127.0.0.1:19999', hostname: '127.0.0.1', port: '19999' },
		configurable: true
	});
}
