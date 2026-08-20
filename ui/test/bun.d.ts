// The two Bun modules the unit tests under ui/test use.
//
// `bun-types` is deliberately NOT a dependency: it redeclares the DOM globals
// this application is written against (WebSocket, fetch, TextDecoder), and
// svelte-check type-checks the app and these tests together. Declaring the
// handful of members actually used keeps one runner's types out of the app's.
declare module 'bun' {
	export function plugin(spec: {
		name: string;
		setup(build: {
			module(
				specifier: string,
				callback: () => { exports: unknown; loader: 'object' }
			): void;
		}): void;
	}): void;
}

declare module 'bun:test' {
	export function test(name: string, fn: () => void | Promise<void>): void;
	export function expect<T>(value: T): {
		toBe(expected: T): void;
		toEqual(expected: unknown): void;
	};
}
