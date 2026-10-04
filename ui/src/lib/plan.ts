// Pure helpers for the submit dialog's Plan action.
//
// The plan itself is computed SERVER-side (OrkflowService.PlanRun) with the
// interpreter's own condition evaluator. What lives here is only how a client
// orders and words it, each mirroring cmd/orkctl/run_plan.go so the dialog and
// `orkctl run plan` print the same rows and the same sentences.
import type { JsonObject } from '@bufbuild/protobuf';
import { PlanOutcome, type PlanRunResponse, type PlanTask } from './gen/orkstack/v1/service_pb';

/** The lower-case OUTCOME word the CLI's table prints. */
export function planOutcomeWord(outcome: PlanOutcome): string {
	switch (outcome) {
		case PlanOutcome.RUNS:
			return 'runs';
		case PlanOutcome.SKIPPED:
			return 'skipped';
		case PlanOutcome.FAILS:
			return 'fails';
		case PlanOutcome.UNREACHED:
			return 'unreached';
		case PlanOutcome.AWAITS:
			return 'awaits';
		default:
			return 'unspecified';
	}
}

function failingTask(plan: PlanRunResponse): PlanTask | undefined {
	return plan.tasks.find((task) => task.outcome === PlanOutcome.FAILS);
}

/** Rows by wave, then declaration order; wave-0 (FAILS, UNREACHED) rows last. */
export function planRows(plan: PlanRunResponse): PlanTask[] {
	return plan.tasks
		.map((task, index) => ({ task, index }))
		.sort((a, b) => {
			const aw = a.task.wave;
			const bw = b.task.wave;
			if ((aw === 0) !== (bw === 0)) return aw === 0 ? 1 : -1;
			if (aw !== bw) return aw - bw;
			return a.index - b.index;
		})
		.map(({ task }) => task);
}

/** The Wave cell: the number, or – for a task that has none. */
export function planWaveLabel(task: PlanTask): string {
	return task.wave > 0 ? String(task.wave) : '–';
}

/** The Detail cell, verbatim as the CLI prints it. */
export function planDetail(task: PlanTask, plan: PlanRunResponse): string {
	switch (task.outcome) {
		case PlanOutcome.SKIPPED:
			return task.skipReason;
		case PlanOutcome.RUNS:
			return task.pin ? `pin ${task.pin}` : '';
		case PlanOutcome.AWAITS: {
			const detail = `await ≤ ${goDuration(task.await?.timeoutSeconds ?? 0)}`;
			return task.await?.callback ? `${detail}, callback` : detail;
		}
		case PlanOutcome.FAILS:
			return task.error?.message ?? '';
		case PlanOutcome.UNREACHED: {
			const failing = failingTask(plan);
			return failing ? `after ${failing.taskId} fails` : '';
		}
		default:
			return '';
	}
}

function plural(n: number, noun: string): string {
	return n === 1 ? `${n} ${noun}` : `${n} ${noun}s`;
}

/**
 * The summary sentence, with the CLI's plurals. The await count is inserted
 * after the run count only when the plan has an await, on the CLI's rule.
 */
export function planSummary(plan: PlanRunResponse): string {
	const awaits = plan.awaits > 0 ? `${plural(plan.awaits, 'await')}, ` : '';
	return `${plan.runs} run, ${awaits}${plan.skipped} skipped, ${plural(plan.waveCount, 'wave')}. Nothing was started.`;
}

/**
 * goDuration spells whole seconds the way Go's time.Duration.String does, so
 * the dialog's DETAIL reads exactly as `orkctl run plan` prints it: 6h0m0s,
 * 2m30s, 45s.
 */
export function goDuration(totalSeconds: number): string {
	if (totalSeconds <= 0) return '0s';
	const h = Math.floor(totalSeconds / 3600);
	const m = Math.floor((totalSeconds % 3600) / 60);
	const s = totalSeconds % 60;
	if (h > 0) return `${h}h${m}m${s}s`;
	if (m > 0) return `${m}m${s}s`;
	return `${s}s`;
}

/** The FAILS sentence, '' when no task fails. */
export function planFailure(plan: PlanRunResponse): string | '' {
	const failing = failingTask(plan);
	if (!failing) return '';
	const unreached = plan.tasks.filter((task) => task.outcome === PlanOutcome.UNREACHED).length;
	return (
		`The run would FAIL deciding ${failing.taskId}: ${failing.error?.message ?? ''}. ` +
		`${plural(unreached, 'task')} unreached; tasks planned to run in the same or a later wave ` +
		'may be cancelled in flight.'
	);
}

/** The caveat lines: unsupplied upstream outputs, then ignored stubs. */
export function planCaveats(plan: PlanRunResponse): string[] {
	const lines: string[] = [];
	const absent = plan.tasks.filter((task) => task.assumedAbsent.length > 0).map((task) => task.taskId);
	if (absent.length > 0) {
		const verb = absent.length === 1 ? 'condition reads' : 'conditions read';
		lines.push(
			`${absent.length} ${verb} upstream outputs that were not supplied (planned as {}): ${absent.join(', ')}.`
		);
	}
	if (plan.unusedAssumedOutputs.length > 0) {
		lines.push(
			`Assumed outputs for tasks that do not run were ignored: ${plan.unusedAssumedOutputs.join(', ')}.`
		);
	}
	return lines;
}

function isJsonObject(value: unknown): value is JsonObject {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Parses the Assumed task outputs textarea: a JSON object keyed by task id
 * whose every value is itself an object, as a values.json is. Whether a key
 * names a task of the Orkflow is the server's to refuse.
 */
export function parseAssumedOutputs(
	text: string
): { value: Record<string, JsonObject> } | { error: string } {
	if (text.trim() === '') return { value: {} };
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch (err) {
		return {
			error: `Assumed outputs are not valid JSON: ${err instanceof Error ? err.message : String(err)}`
		};
	}
	if (!isJsonObject(parsed)) {
		return {
			error: 'Assumed outputs must be a JSON object keyed by task id, e.g. {"build": {"ok": true}}.'
		};
	}
	for (const key of Object.keys(parsed)) {
		if (!isJsonObject(parsed[key])) {
			return { error: `Assumed output for task "${key}" must be a JSON object, as a values.json is.` };
		}
	}
	return { value: parsed as Record<string, JsonObject> };
}
