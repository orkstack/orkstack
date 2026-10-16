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
		toContain(expected: unknown): void;
		toHaveLength(expected: number): void;
		// Absence, asserted as itself: a helper that answers "there is nothing
		// to say" returns null, and a synthesized attempt record carries
		// undefined for every field no Orker filled in.
		toBeNull(): void;
		toBeUndefined(): void;
		not: {
			toBe(expected: T): void;
			toContain(expected: unknown): void;
		};
	};
}

// The filesystem reads the unit tests make: activityInterface.test.ts reads
// the first-party images' Dockerfiles, so the UI is tested against the label a
// real image ships rather than a copy that can drift from it, and
// e2eProcessHygiene.test.ts lists and reads the e2e sources, and
// vocabulary.test.ts walks the user-facing sources.
declare module 'node:fs' {
	export function readFileSync(path: string, encoding: 'utf8'): string;
	export function readdirSync(path: string, options?: { recursive?: boolean }): string[];
}

// Bun's built-in YAML 1.2 reader: authoredSchemas.test.ts reads the shipped
// documents the way an editor's YAML language server does.
declare const Bun: {
	readonly YAML: { parse(text: string): unknown };
};

interface ImportMeta {
	/** Bun: the directory of the current module. */
	readonly dir: string;
}
