import { expect, test } from 'bun:test';
import { defaultsForInputs, parseInputObject, submitCli } from '../src/lib/authoring';
import { shellQuote } from '../src/lib/runDetail';

declare const Bun: {
	spawnSync(options: { cmd: string[] }): { exitCode: number; stdout: Uint8Array };
};

test('declared defaults retain JSON types', () => {
	expect(
		defaultsForInputs({ enabled: { type: 'boolean', default: false }, count: { type: 'number', default: 3 } })
	).toEqual({ enabled: false, count: 3 });
});

test('JSON input must be an object', () => {
	expect(parseInputObject('["not", "an object"]')).toEqual({ error: 'Inputs must be a JSON object, e.g. {"url": "https://…"}.' });
	expect(parseInputObject('{"quoted":"it\'s safe"}')).toEqual({ value: { quoted: "it's safe" } });
});

test('CLI preview writes exact JSON and quotes every user supplied coordinate', () => {
	const command = submitCli({ name: 'deploy', namespace: 'payments', version: '1.2.3', input: { note: "it's safe" } });
	expect(command).toContain("printf '%s\\n' '{\"note\":\"it'\"'\"'s safe\"}' > input.json");
	expect(command).toContain("orkctl run submit deploy --namespace payments --version 1.2.3");
	expect(shellQuote("a'b")).toBe("'a'\"'\"'b'");
});

test('the CLI preview carries the origin reference, and a re-run is `run again` of its source', () => {
	const submit = submitCli({ name: 'deploy', namespace: '', version: '', input: {}, originReference: 'https://ci.example/1' });
	expect(submit).toContain("orkctl run submit deploy");
	expect(submit).toContain('--origin-ref https://ci.example/1');
	const again = submitCli({ name: 'deploy', namespace: 'payments', version: '1.2.3', input: {}, sourceRunId: 'ork-9', originReference: 'INC-1' });
	expect(again).toContain("orkctl run again ork-9 --namespace payments --version 1.2.3");
	expect(again).toContain('--origin-ref INC-1');
	expect(again).not.toContain('run submit');
	expect(submitCli({ name: 'deploy', namespace: '', version: '', input: {} })).not.toContain('--origin-ref');
});

test('POSIX shell quoting round-trips apostrophes, substitutions, whitespace, and newlines', () => {
	for (const original of ["a'b", '`not executed` $(also-not)', 'two words', 'first\nsecond']) {
		const result = Bun.spawnSync({ cmd: ['/bin/sh', '-c', `set -- ${shellQuote(original)}; printf '%s' "$1"`] });
		expect(result.exitCode).toBe(0);
		expect(new TextDecoder().decode(result.stdout)).toBe(original);
	}
});
