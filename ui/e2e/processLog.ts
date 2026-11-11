import { appendFileSync, createWriteStream, existsSync, writeFileSync, type WriteStream } from 'node:fs';
import path from 'node:path';

/**
 * startTranscript writes a spec file's transcript header in its beforeAll.
 * When the transcript is already there, the spec file has started again —
 * Playwright runs its remaining tests in a new worker after a failure — and
 * the lines the earlier start noted, the failing window among them, are kept:
 * the header is appended under a line marking the new start, as a process
 * log's start is (openProcessLog).
 */
export function startTranscript(file: string, header: string): void {
	if (!existsSync(file)) {
		writeFileSync(file, header);
		return;
	}
	appendFileSync(file, `==== ${new Date().toISOString()} started again; the lines above are an earlier start's ====\n${header}`);
}

/**
 * openProcessLog opens the log a spec's process (an engine, an Orker, a
 * helper) writes to, for appending, and marks this start with a line of its
 * own. A log is never opened afresh: after a failure Playwright runs the
 * spec file's remaining tests in a new worker, whose beforeAll starts the
 * processes again under the same names, and a log truncated there lost the
 * failing window it held. Every start a spec file makes is kept, in order,
 * each under the line that names it.
 */
export function openProcessLog(file: string, command: string, pid: number | undefined): WriteStream {
	const out = createWriteStream(file, { flags: 'a' });
	out.write(`==== ${new Date().toISOString()} ${path.basename(command)} started, pid ${pid ?? 'unknown'} ====\n`);
	return out;
}
