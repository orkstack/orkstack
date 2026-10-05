import type { JsonObject, JsonValue } from '@bufbuild/protobuf';
import { shellQuote } from './runDetail';

// The DSL keeps input declarations as a deliberately small JSON-schema
// fragment. These helpers make the submit page useful without pretending to
// validate the compiler's language in the browser.
export type InputSpec = {
	name: string;
	type: 'string' | 'number' | 'boolean' | 'object' | 'array' | 'json';
	required: boolean;
	defaultValue?: JsonValue;
};

function isObject(value: JsonValue | undefined): value is JsonObject {
	return value !== undefined && value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function inputSpecs(inputs: JsonObject | undefined): InputSpec[] {
	return Object.entries(inputs ?? {}).map(([name, raw]) => {
		const spec = isObject(raw) ? raw : {};
		const declared = spec.type;
		const type =
			declared === 'string' ||
			declared === 'number' ||
			declared === 'boolean' ||
			declared === 'object' ||
			declared === 'array'
				? declared
				: 'json';
		return {
			name,
			type,
			required: spec.required === true,
			...(spec.default === undefined ? {} : { defaultValue: spec.default })
		};
	});
}

export function defaultsForInputs(inputs: JsonObject | undefined): JsonObject {
	const defaults: JsonObject = {};
	for (const spec of inputSpecs(inputs)) {
		if (spec.defaultValue !== undefined) defaults[spec.name] = spec.defaultValue;
	}
	return defaults;
}

export function parseInputObject(text: string): { value: JsonObject } | { error: string } {
	try {
		const value: unknown = JSON.parse(text || '{}');
		if (value === null || typeof value !== 'object' || Array.isArray(value)) {
			return { error: 'Inputs must be a JSON object, e.g. {"url": "https://…"}.' };
		}
		return { value: value as JsonObject };
	} catch (err) {
		return { error: `Inputs are not valid JSON: ${err instanceof Error ? err.message : String(err)}` };
	}
}

export function setInput(value: JsonObject, name: string, next: JsonValue | undefined): JsonObject {
	const copy = { ...value };
	if (next === undefined) delete copy[name];
	else copy[name] = next;
	return copy;
}

export function submitCli(args: {
	name: string;
	namespace: string;
	version: string;
	input: JsonObject;
	idempotencyKey?: string;
	allowEmptyPool?: boolean;
	/** Ignore every memo for this run (--no-cache, docs/DSL.md "Caching"). */
	noCache?: boolean;
	/** Recorded on the run as its origin reference (--origin-ref). */
	originReference?: string;
	/**
	 * Set when the draft re-runs an existing run: the command is then
	 * `orkctl run again <id>`, which records the same run-again origin the
	 * dialog does, instead of `orkctl run submit <name>`.
	 */
	sourceRunId?: string;
}): string {
	const namespace = args.namespace || 'default';
	const flags = [
		`--namespace ${shellQuote(namespace)}`,
		`--version ${shellQuote(args.version)}`,
		'--input-json "$(cat input.json)"'
	];
	if (args.idempotencyKey) flags.push(`--idempotency-key ${shellQuote(args.idempotencyKey)}`);
	if (args.allowEmptyPool) flags.push('--allow-empty-pool');
	if (args.noCache) flags.push('--no-cache');
	if (args.originReference) flags.push(`--origin-ref ${shellQuote(args.originReference)}`);
	const command = args.sourceRunId
		? `orkctl run again ${shellQuote(args.sourceRunId)}`
		: `orkctl run submit ${shellQuote(args.name)}`;
	return `printf '%s\\n' ${shellQuote(JSON.stringify(args.input))} > input.json\n${command} ${flags.join(' ')}`;
}

// planCli is submitCli's sibling for the Plan action: the same input.json
// idiom, plus outputs.json when assumed outputs are set. An empty version is
// omitted, so the copied command plans `current` exactly as the dialog did.
export function planCli(args: {
	name: string;
	namespace: string;
	version: string;
	input: JsonObject;
	assumedOutputs?: JsonObject;
}): string {
	const namespace = args.namespace || 'default';
	const flags = [`--namespace ${shellQuote(namespace)}`];
	if (args.version) flags.push(`--version ${shellQuote(args.version)}`);
	flags.push('--input-json "$(cat input.json)"');
	const lines = [`printf '%s\\n' ${shellQuote(JSON.stringify(args.input))} > input.json`];
	if (args.assumedOutputs && Object.keys(args.assumedOutputs).length > 0) {
		lines.push(`printf '%s\\n' ${shellQuote(JSON.stringify(args.assumedOutputs))} > outputs.json`);
		flags.push('--outputs-file outputs.json');
	}
	lines.push(`orkctl run plan ${shellQuote(args.name)} ${flags.join(' ')}`);
	return lines.join('\n');
}
