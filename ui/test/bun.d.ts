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
	// A second argument is the message a failure prints (which shape, which
	// edge), as Bun's own expect takes.
	export function expect<T>(value: T, message?: string): {
		toBe(expected: T): void;
		toBeLessThan(expected: number): void;
		toBeLessThanOrEqual(expected: number): void;
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
// e2eProcessHygiene.test.ts lists and reads the e2e sources,
// vocabulary.test.ts walks the user-facing sources, and testidBaseline.test.ts
// rewrites its fixture on request.
declare module 'node:fs' {
	export function readFileSync(path: string, encoding: 'utf8'): string;
	/** Without an encoding: the file's bytes (e2e/build-hash.ts hashes them). */
	export function readFileSync(path: string): Uint8Array;
	export function readdirSync(path: string, options?: { recursive?: boolean }): string[];
	export function writeFileSync(path: string, data: string): void;
	// e2e/build-ui.ts and its test (build-ui.test.ts): the build's marker,
	// and the test's own temporary UI directory.
	export function existsSync(path: string): boolean;
	export function rmSync(path: string, options?: { force?: boolean; recursive?: boolean }): void;
	export function mkdirSync(path: string, options?: { recursive?: boolean }): void;
	export function mkdtempSync(prefix: string): string;
}

// e2e/build-ui.ts runs `bun run build`; build-ui.test.ts imports it.
declare module 'node:child_process' {
	export function execFileSync(
		file: string,
		args: string[],
		options: { cwd: string; stdio: 'inherit' | 'ignore' }
	): unknown;
}
declare module 'node:path' {
	const path: {
		resolve(...paths: string[]): string;
		dirname(path: string): string;
		join(...paths: string[]): string;
	};
	export default path;
}
declare module 'node:url' {
	export function fileURLToPath(url: string): string;
}
declare module 'node:os' {
	export function tmpdir(): string;
}

// The hash e2e/build-hash.ts takes over the UI build's inputs; its test under
// ui/test imports that module, so the one member it uses is declared here.
declare module 'node:crypto' {
	export function createHash(algorithm: 'sha256'): {
		update(data: string | Uint8Array): void;
		digest(encoding: 'hex'): string;
	};
}

// Bun's built-in YAML 1.2 reader: authoredSchemas.test.ts reads the shipped
// documents the way an editor's YAML language server does.
declare const Bun: {
	readonly YAML: { parse(text: string): unknown };
};

interface ImportMeta {
	/** Bun: the directory of the current module. */
	readonly dir: string;
	/** Bun (and Node 24+): whether this module is the entry point. */
	readonly main: boolean;
}

// The one environment read the unit tests make: UPDATE_DAG_PINS=1 rewrites
// dag.test.ts's pinned layouts. A module, not a global, so the app's code
// cannot reach a `process` it does not have.
declare module 'node:process' {
	export const env: Record<string, string | undefined>;
}
