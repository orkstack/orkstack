import { expect, test } from 'bun:test';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';

// Every orkctl command the UI prints uses the flag's shorthand where one
// exists (`-n`, not `--namespace`); the long form stays in `orkctl --help`
// and docs/CLI.md. docs/CLI.md is generated from the Cobra tree, so it is the
// source of the letters: a long flag is refused here when EVERY command that
// declares it gives it the same shorthand, which is true of each one the CLI
// shortens. cmd/orkctl's TestDocs_EveryOrkctlInvocationResolves holds the
// docs, the site and the example headers to the same rule.
//
// Comments are not printed, so they are not read. Generated files are skipped.

const uiDir = path.resolve(import.meta.dir, '..');
const cliReference = readFileSync(path.join(uiDir, '..', 'docs', 'CLI.md'), 'utf8');

/** The long flags that have one shorthand on every command that declares them. */
function alwaysShortened(reference: string): Map<string, string> {
	const shorts = new Map<string, Set<string>>();
	const unshortened = new Set<string>();
	for (const line of reference.split('\n')) {
		const flag = /^- `(?:-(\w), )?--([\w-]+)[ `]/.exec(line);
		if (!flag) continue;
		const [, short, long] = flag;
		if (!short) {
			unshortened.add(long);
			continue;
		}
		if (!shorts.has(long)) shorts.set(long, new Set());
		shorts.get(long)!.add(short);
	}
	const out = new Map<string, string>();
	for (const [long, letters] of shorts) {
		if (unshortened.has(long) || letters.size !== 1) continue;
		out.set(long, [...letters][0]);
	}
	return out;
}

/** The UI's own sources, relative to ui/: generated clients and validators excluded. */
function sources(): string[] {
	return readdirSync(`${uiDir}/src`, { recursive: true })
		.filter((f) => /\.(?:ts|svelte)$/.test(f) && !f.startsWith('lib/gen/') && !f.endsWith('.gen.ts'))
		.filter((f) => statSync(`${uiDir}/src/${f}`).isFile())
		.map((f) => `src/${f}`);
}

function isComment(line: string): boolean {
	const trimmed = line.trim();
	return trimmed.startsWith('//') || trimmed.startsWith('*') || trimmed.startsWith('/*') || trimmed.startsWith('<!--');
}

function offending(match: (line: string) => string[]): string[] {
	const bad: string[] = [];
	for (const file of sources()) {
		readFileSync(`${uiDir}/${file}`, 'utf8')
			.split('\n')
			.forEach((line, i) => {
				if (isComment(line)) return;
				for (const why of match(line)) bad.push(`${file}:${i + 1}: ${why}`);
			});
	}
	return bad;
}

const letters = alwaysShortened(cliReference);

test('the letter table is read from docs/CLI.md', () => {
	expect(letters.get('namespace')).toBe('n');
	expect(letters.get('json')).toBe('j');
	expect(letters.get('task')).toBe('t');
	expect(letters.get('filter')).toBe('F');
	expect(letters.get('since')).toBe('s');
	// --input-json has no shorthand, so it is never mistaken for --input.
	expect(letters.has('input-json')).toBe(false);
});

test('no UI source prints a long flag that has a shorthand', () => {
	const long = new RegExp(`--(${[...letters.keys()].join('|')})(?![\\w-])`, 'g');
	const bad = offending((line) => [...line.matchAll(long)].map((m) => `--${m[1]}: print -${letters.get(m[1])}`));
	expect(bad).toEqual([]);
});

test('no UI source prints a removed orkctl verb', () => {
	// 0.3.0: an Orkflow is created (orkctl orkflow create), an automation
	// document applied and exported under its own noun, a webhook created.
	const removed = [
		/\borkctl (?:-n \S+ )?apply\b/,
		/'orkctl',\s*(?:'-n',\s*[^,]+,\s*)?'apply'/,
		/\borkctl (?:-n \S+ )?export\b/,
		/\bwebhook register\b/,
		/'webhook',\s*'register'/
	];
	const bad = offending((line) => (removed.some((re) => re.test(line)) ? [line.trim()] : []));
	expect(bad).toEqual([]);
});
