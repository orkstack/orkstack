// The verdict of the shared fixture that fails a spec on a content security
// policy violation (./test.ts). It is kept apart from Playwright so the UI's
// unit suite can test it (test/csp.test.ts).

/**
 * CspWatch sorts the refusals one test's pages report. A refusal that
 * matches a pattern the test declared with `expect` is the test's own doing
 * and is kept in `expected`; every other one is in `unexpected`, and the
 * test fails when any is left there (violationsError).
 *
 * A page under two policies reports a refusal once per policy that refuses
 * it: the engine sends the build's policy as a header and the page carries
 * it as a meta tag too, so one refused script is reported twice. Declaring
 * the expectation before provoking it, rather than counting reports, is
 * what keeps a positive control from racing the second report.
 */
export class CspWatch {
	readonly expected: string[] = [];
	readonly unexpected: string[] = [];
	private readonly patterns: RegExp[] = [];

	/** expect declares that refusals matching pattern are the test's own doing. */
	expect(pattern: RegExp): void {
		// A global or sticky pattern keeps state between tests of it.
		this.patterns.push(new RegExp(pattern.source, pattern.flags.replace(/[gy]/g, '')));
	}

	/** record files one refusal a page reported. */
	record(what: string): void {
		(this.patterns.some((p) => p.test(what)) ? this.expected : this.unexpected).push(what);
	}
}

/**
 * violationsError is the error that fails a test whose pages left the
 * refusals `unexpected`, each named once, or null when they left none.
 */
export function violationsError(unexpected: readonly string[]): Error | null {
	const distinct = [...new Set(unexpected)];
	if (distinct.length === 0) return null;
	return new Error(
		`the content security policy refused ${distinct.length} thing(s) a page tried, which the spec did not expect:\n  ` +
			distinct.join('\n  ')
	);
}
