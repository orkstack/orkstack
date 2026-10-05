import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { ErrorSchema } from '../src/lib/gen/orkstack/v1/common_pb';
import {
	PlanOutcome,
	PlanRunResponseSchema,
	PlanTaskSchema,
	type PlanRunResponse
} from '../src/lib/gen/orkstack/v1/service_pb';
import { planCli } from '../src/lib/authoring';
import {
	parseAssumedOutputs,
	planCaveats,
	planDetail,
	planFailure,
	planRows,
	planSummary
} from '../src/lib/plan';

const task = (taskId: string, outcome: PlanOutcome, wave: number, extra: Record<string, unknown> = {}) =>
	create(PlanTaskSchema, { taskId, outcome, wave, ...extra });

// U1
test('planRows orders by wave then declaration and puts wave-0 rows last', () => {
	const plan = create(PlanRunResponseSchema, {
		tasks: [
			task('late', PlanOutcome.RUNS, 2),
			task('broken', PlanOutcome.FAILS, 0),
			task('early', PlanOutcome.RUNS, 1),
			task('also-late', PlanOutcome.SKIPPED, 2),
			task('gone', PlanOutcome.UNREACHED, 0)
		]
	});
	expect(planRows(plan).map((row) => row.taskId)).toEqual(['early', 'late', 'also-late', 'broken', 'gone']);
});

// U2
test('planSummary handles plurals', () => {
	const one = create(PlanRunResponseSchema, { runs: 1, skipped: 0, waveCount: 1 });
	expect(planSummary(one)).toBe('1 run, 0 skipped, 1 wave. Nothing was started.');
	const many = create(PlanRunResponseSchema, { runs: 22, skipped: 18, waveCount: 6 });
	expect(planSummary(many)).toBe('22 run, 18 skipped, 6 waves. Nothing was started.');
	expect(planSummary(create(PlanRunResponseSchema, {}))).toBe('0 run, 0 skipped, 0 waves. Nothing was started.');
});

// U3
test('planFailure is empty without a FAILS task and names it with one', () => {
	expect(planFailure(create(PlanRunResponseSchema, { tasks: [task('a', PlanOutcome.RUNS, 1)] }))).toBe('');
	const failing = create(PlanRunResponseSchema, {
		tasks: [
			task('first', PlanOutcome.RUNS, 1),
			task('broken', PlanOutcome.FAILS, 0, {
				error: create(ErrorSchema, { code: 'RUN_FAILED', message: 'evaluate condition "x": boom' })
			}),
			task('after', PlanOutcome.UNREACHED, 0)
		]
	});
	expect(planFailure(failing)).toBe(
		'The run would FAIL deciding broken: evaluate condition "x": boom. 1 task unreached; ' +
			'tasks planned to run in the same or a later wave may be cancelled in flight.'
	);
	expect(planDetail(failing.tasks[2], failing)).toBe('after broken fails');
	expect(planDetail(failing.tasks[1], failing)).toBe('evaluate condition "x": boom');
});

// U4
test('planCaveats match the CLI sentences', () => {
	const plan: PlanRunResponse = create(PlanRunResponseSchema, {
		tasks: [
			task('deploy-eu', PlanOutcome.RUNS, 4, { assumedAbsent: ['build'], pin: 'release' }),
			task('smoke-test', PlanOutcome.SKIPPED, 5, {
				assumedAbsent: ['deploy-eu'],
				skipReason: 'condition "x" evaluated false'
			})
		],
		unusedAssumedOutputs: ['deploy-us', 'notify']
	});
	expect(planCaveats(plan)).toEqual([
		'2 conditions read upstream outputs that were not supplied (planned as {}): deploy-eu, smoke-test.',
		'Assumed outputs for tasks that do not run were ignored: deploy-us, notify.'
	]);
	const single = create(PlanRunResponseSchema, {
		tasks: [task('only', PlanOutcome.SKIPPED, 2, { assumedAbsent: ['x'] })]
	});
	expect(planCaveats(single)).toEqual([
		'1 condition reads upstream outputs that were not supplied (planned as {}): only.'
	]);
	expect(planCaveats(create(PlanRunResponseSchema, {}))).toEqual([]);
	expect(planDetail(plan.tasks[0], plan)).toBe('pin release');
	expect(planDetail(plan.tasks[1], plan)).toBe('condition "x" evaluated false');
});

// U5
test('planCli omits --version when empty, emits --outputs-file only with assumptions, and quotes', () => {
	const current = planCli({ name: 'deploy', namespace: 'payments', version: '', input: { region: 'eu' } });
	expect(current).toBe(
		`printf '%s\\n' '{"region":"eu"}' > input.json\n` +
			`orkctl run plan 'deploy' --namespace 'payments' --input-json "$(cat input.json)"`
	);
	expect(current).not.toContain('--version');
	expect(current).not.toContain('--outputs-file');

	const pinned = planCli({
		name: "it's",
		namespace: '',
		version: '1.2.3',
		input: {},
		assumedOutputs: { build: { ok: true } }
	});
	expect(pinned).toContain(`printf '%s\\n' '{"build":{"ok":true}}' > outputs.json`);
	expect(pinned).toContain(
		`orkctl run plan 'it'"'"'s' --namespace 'default' --version '1.2.3' --input-json "$(cat input.json)" --outputs-file outputs.json`
	);
	expect(planCli({ name: 'x', namespace: 'n', version: '', input: {}, assumedOutputs: {} })).not.toContain(
		'outputs.json'
	);
});

// U6
test('parseAssumedOutputs follows its five rules in order', () => {
	expect(parseAssumedOutputs('')).toEqual({ value: {} });
	expect(parseAssumedOutputs('   ')).toEqual({ value: {} });
	expect(parseAssumedOutputs('{}')).toEqual({ value: {} });
	const bad = parseAssumedOutputs('{');
	expect('error' in bad && bad.error.startsWith('Assumed outputs are not valid JSON: ')).toBe(true);
	const keyed = 'Assumed outputs must be a JSON object keyed by task id, e.g. {"build": {"ok": true}}.';
	for (const text of ['[]', 'null', '3']) expect(parseAssumedOutputs(text)).toEqual({ error: keyed });
	const perKey = 'Assumed output for task "build" must be a JSON object, as a values.json is.';
	for (const text of ['{"build": 1}', '{"build": []}', '{"build": null}']) {
		expect(parseAssumedOutputs(text)).toEqual({ error: perKey });
	}
	expect(parseAssumedOutputs('{"a": {}, "b": 2, "c": 3}')).toEqual({
		error: 'Assumed output for task "b" must be a JSON object, as a values.json is.'
	});
	expect(parseAssumedOutputs('{"build": {"ok": true}, "test": {}}')).toEqual({
		value: { build: { ok: true }, test: {} }
	});
});

test('an await plans as AWAITS with the CLI detail, and the summary counts it only when present', async () => {
	const { AwaitSpecSchema } = await import('../src/lib/gen/orkstack/v1/orkflow_pb');
	const { goDuration, planOutcomeWord } = await import('../src/lib/plan');
	const plan = create(PlanRunResponseSchema, {
		runs: 2,
		awaits: 2,
		waveCount: 3,
		tasks: [
			task('training', PlanOutcome.AWAITS, 2, {
				await: create(AwaitSpecSchema, { timeoutSeconds: 21600, callback: true })
			}),
			task('approval', PlanOutcome.AWAITS, 2, {
				await: create(AwaitSpecSchema, { timeoutSeconds: 86400 })
			})
		]
	});
	expect(planOutcomeWord(PlanOutcome.AWAITS)).toBe('awaits');
	expect(planDetail(plan.tasks[0], plan)).toBe('await ≤ 6h0m0s, callback');
	expect(planDetail(plan.tasks[1], plan)).toBe('await ≤ 24h0m0s');
	expect(planSummary(plan)).toBe('2 run, 2 awaits, 0 skipped, 3 waves. Nothing was started.');
	expect(planSummary(create(PlanRunResponseSchema, { runs: 1, awaits: 1, waveCount: 2 }))).toBe(
		'1 run, 1 await, 0 skipped, 2 waves. Nothing was started.'
	);
	expect(goDuration(150)).toBe('2m30s');
	expect(goDuration(45)).toBe('45s');
});

test('a composed task plans as one RUNS row with the CLI detail', async () => {
	const { OrkflowRefSchema } = await import('../src/lib/gen/orkstack/v1/orkflow_pb');
	const plan = create(PlanRunResponseSchema, {
		tasks: [
			task('release', PlanOutcome.RUNS, 2, {
				orkflow: create(OrkflowRefSchema, { name: 'release-block', version: '2.1.0' })
			})
		]
	});
	expect(planDetail(plan.tasks[0], plan)).toBe(
		'orkflow release-block@2.1.0 as a child run (not planned: orkctl run plan release-block --version 2.1.0)'
	);
});
