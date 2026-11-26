// The two Bun modules, and the Bun globals, the unit tests under ui/test use.
//
// `bun-types` is deliberately NOT a dependency: it redeclares the DOM globals
// this application is written against (WebSocket, fetch, TextDecoder), and
// svelte-check type-checks the app and these tests together. Declaring the
// handful of members actually used keeps one runner's types out of the app's.
// Node's modules come from `@types/node`, which the specs under ui/e2e are
// checked against (e2e/tsconfig.json).
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
	// The third argument is the test's own timeout in milliseconds.
	export function test(name: string, fn: () => void | Promise<void>, timeoutMs?: number): void;
	// A second argument is the message a failure prints (which shape, which
	// edge), as Bun's own expect takes.
	export function expect<T>(value: T, message?: string): {
		toBe(expected: T): void;
		toBeLessThan(expected: number): void;
		toBeLessThanOrEqual(expected: number): void;
		toBeGreaterThanOrEqual(expected: number): void;
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
			// csp.test.ts: a refusal the spec did not declare is an error.
			toBeNull(): void;
		};
	};
}

// Bun's built-in YAML 1.2 reader: authoredSchemas.test.ts reads the shipped
// documents the way an editor's YAML language server does. spawnSync:
// browser-server-owner.test.ts runs e2e/browser-server.ts's start command.
declare const Bun: {
	readonly YAML: { parse(text: string): unknown };
	spawnSync(
		cmd: string[],
		options: { cwd: string; env: Record<string, string | undefined> }
	): { exitCode: number | null; stdout: { toString(): string }; stderr: { toString(): string } };
};

interface ImportMeta {
	/** Bun: the directory of the current module. */
	readonly dir: string;
	/** Bun (and Node 24+): whether this module is the entry point. */
	readonly main: boolean;
}
