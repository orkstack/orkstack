import { expect, test } from 'bun:test';
import { appendFileSync, mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { openProcessLog, startTranscript } from '../e2e/processLog';

// A spec's engine and Orker logs are the record of a failing window. After a
// failure Playwright runs the file's remaining tests in a new worker, whose
// beforeAll starts the processes again under the same log names; a log opened
// afresh there erased the window the failure happened in (CI run
// 42866551413, composed-orkflows.spec.ts). Every start is kept, in order.

function closed(stream: ReturnType<typeof openProcessLog>): Promise<void> {
	return new Promise((resolve, reject) => {
		stream.on('error', reject);
		stream.end(resolve);
	});
}

test('a second start into the same log keeps the first start, each under its own line', async () => {
	const dir = mkdtempSync(`${tmpdir()}/orkstack-process-log-`);
	try {
		const file = `${dir}/engine.log`;
		const first = openProcessLog(file, '/tmp/bin/ork', 101);
		first.write('first start: the failing window\n');
		await closed(first);
		const second = openProcessLog(file, '/tmp/bin/ork', 202);
		second.write('second start: the restarted worker\n');
		await closed(second);

		const lines = readFileSync(file, 'utf8').trimEnd().split('\n');
		expect(lines).toHaveLength(4);
		expect(/^==== \d{4}-\d\d-\d\dT\S+ ork started, pid 101 ====$/.test(lines[0]), lines[0]).toBe(true);
		expect(lines[1]).toBe('first start: the failing window');
		expect(/^==== \d{4}-\d\d-\d\dT\S+ ork started, pid 202 ====$/.test(lines[2]), lines[2]).toBe(true);
		expect(lines[3]).toBe('second start: the restarted worker');
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

test('a spec file started again keeps its transcript: the earlier start, a line marking the new one, its header', () => {
	const dir = mkdtempSync(`${tmpdir()}/orkstack-transcript-`);
	try {
		const file = `${dir}/transcript.txt`;
		startTranscript(file, '# the spec header\n');
		appendFileSync(file, 'noted before the failure\n');
		// Playwright's new worker after the failure: beforeAll runs again.
		startTranscript(file, '# the spec header\n');
		appendFileSync(file, 'noted after the restart\n');

		const lines = readFileSync(file, 'utf8').trimEnd().split('\n');
		expect(lines).toHaveLength(5);
		expect(lines[0]).toBe('# the spec header');
		expect(lines[1]).toBe('noted before the failure');
		expect(/^==== \d{4}-\d\d-\d\dT\S+ started again; the lines above are an earlier start's ====$/.test(lines[2]), lines[2]).toBe(true);
		expect(lines[3]).toBe('# the spec header');
		expect(lines[4]).toBe('noted after the restart');
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
});

// A path that names a log or a transcript: `outPath(TRANSCRIPT)`,
// `out('engine.log')`, `ENGINE_LOG`.
const LOG_OR_TRANSCRIPT = /transcript|\.log\b|_log\b/i;

test('no e2e file opens a log or a transcript afresh, which would erase what an earlier start wrote', () => {
	const e2eDir = `${import.meta.dir}/../e2e`;
	const offenders: string[] = [];
	for (const name of readdirSync(e2eDir, { recursive: true })
		.filter((f: string) => f.endsWith('.ts'))
		.sort()) {
		// The one place a spec's log or transcript is opened, and it appends.
		if (name === 'processLog.ts') continue;
		const source = readFileSync(`${e2eDir}/${name}`, 'utf8');
		const at = (index: number | undefined) => `${name}:${source.slice(0, index ?? 0).split('\n').length}`;
		for (const call of source.matchAll(/createWriteStream\(/g)) offenders.push(`${at(call.index)}: createWriteStream`);
		// A whole-file write, or an open for writing, whose path (its first
		// argument, however it is wrapped) names a log or a transcript.
		for (const call of source.matchAll(/\b(writeFileSync|openSync)\(\s*([^,]+),\s*([^,)]*)/g)) {
			const [, fn, target, second] = call;
			if (!LOG_OR_TRANSCRIPT.test(target)) continue;
			if (fn === 'openSync' && !/['"][^'"]*w/.test(second)) continue;
			offenders.push(`${at(call.index)}: ${fn}(${target.trim()}, …)`);
		}
	}
	expect(offenders).toEqual([]);
});
