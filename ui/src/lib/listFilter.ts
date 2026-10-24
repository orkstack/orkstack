// The browser's half of the one structured list filter (docs/UI_CLI_PARITY.md
// "Filtering lists"). Every list request that takes `repeated FieldFilter
// filters` declares its allowlist once, in proto, with the
// (orkstack.v1.filterable) option; this module reads that declaration from the
// generated descriptors, so the browser, `orkctl --filter help` and the engine
// all answer "what can I filter on" from one place.
//
// The grammar here is internal/listfilter/grammar.go's, held to the same
// table (internal/listfilter/testdata/vectors.json, read by
// test/listFilter.test.ts). The browser checks SYNTAX only: what a field may
// take is the server's to refuse, and its refusal is shown verbatim, so the
// browser invents no validation the CLI would bypass.
import { create, createRegistry, getOption, type DescEnum, type DescField, type DescFile, type DescMessage } from '@bufbuild/protobuf';
import { ScalarType } from '@bufbuild/protobuf';
import {
	FieldFilterSchema,
	FilterOperator,
	FilterValueRule,
	filterable,
	type FieldFilter
} from './gen/orkstack/v1/filter_pb';
import { shellQuote } from './runDetail';

export type { FieldFilter };
export { FilterOperator };

/** What a filterable field holds, which decides its operators and its value control. */
export type FilterKind = 'text' | 'enum' | 'bool' | 'time' | 'labels';

export interface FilterField {
	/** The proto path a filter carries: "name", "hold.reason", "labels". */
	path: string;
	/** The display name: "Hold reason". */
	label: string;
	kind: FilterKind;
	operators: FilterOperator[];
	/** For an enum: the short names a filter may name, in declaration order. */
	values: string[];
	keptWhenUnavailable: boolean;
	requires: string;
	valueRule: FilterValueRule;
}

export interface FilterSpec {
	/** The listed message's name: "Namespace". */
	resource: string;
	fields: FilterField[];
}

/** The fixed display names both clients use (internal/listfilter OperatorLabel). */
export const OPERATOR_LABELS: Record<FilterOperator, string> = {
	[FilterOperator.UNSPECIFIED]: 'no operator',
	[FilterOperator.EQUALS]: 'equals (=)',
	[FilterOperator.NOT_EQUALS]: 'not equals (!=)',
	[FilterOperator.IN]: 'in (=a,b)',
	[FilterOperator.CONTAINS]: 'contains (~)',
	[FilterOperator.PREFIX]: 'prefix (^)',
	[FilterOperator.AT_OR_AFTER]: 'at or after (>=)',
	[FilterOperator.BEFORE]: 'before (<)'
};

const OPERATOR_TEXT: Record<FilterOperator, string> = {
	[FilterOperator.UNSPECIFIED]: '',
	[FilterOperator.EQUALS]: '=',
	[FilterOperator.NOT_EQUALS]: '!=',
	[FilterOperator.IN]: '=',
	[FilterOperator.CONTAINS]: '~',
	[FilterOperator.PREFIX]: '^',
	[FilterOperator.AT_OR_AFTER]: '>=',
	[FilterOperator.BEFORE]: '<'
};

// filesOf is a request's own file and everything it imports, transitively.
// A listed message is looked up there by the full name the declaration gives:
// the request's file declares the response that carries the listed message,
// so it defines or imports it. Resolving through the request — rather than a
// registry of every generated file — is what keeps this module from pulling
// every service's schemas into each route that renders a filter bar.
function filesOf(file: DescFile, seen = new Set<DescFile>()): Set<DescFile> {
	if (seen.has(file)) return seen;
	seen.add(file);
	for (const dependency of file.dependencies) filesOf(dependency, seen);
	return seen;
}

/**
 * enumPrefix is the SCREAMING_SNAKE form of the enum's name plus "_" — the
 * prefix every value carries — by internal/enumname.Prefix's rule.
 */
export function enumPrefix(name: string): string {
	let out = '';
	for (let i = 0; i < name.length; i += 1) {
		const c = name[i];
		const upper = c >= 'A' && c <= 'Z';
		if (i > 0 && upper) {
			const prevUpper = name[i - 1] >= 'A' && name[i - 1] <= 'Z';
			const nextLower = i + 1 < name.length && name[i + 1] >= 'a' && name[i + 1] <= 'z';
			if (!prevUpper || nextLower) out += '_';
		}
		out += c.toUpperCase();
	}
	return out + '_';
}

/** An enum value's short name: STATUS_FAILED reads FAILED (internal/enumname.Short). */
export function shortEnumName(desc: DescEnum, name: string): string {
	const trimmed = name.startsWith(enumPrefix(desc.name)) ? name.slice(enumPrefix(desc.name).length) : name;
	if (trimmed === name || trimmed === '' || desc.values.some((v) => v.name === trimmed)) return name;
	return trimmed;
}

function resolveField(resource: DescMessage, path: string): { kind: FilterKind; leaf: DescField } {
	let message: DescMessage | undefined = resource;
	const parts = path.split('.');
	for (let i = 0; i < parts.length; i += 1) {
		const field: DescField | undefined = message?.fields.find((f) => f.name === parts[i]);
		if (!field) throw new Error(`${resource.typeName} has no field ${path}`);
		const last = i === parts.length - 1;
		if (field.fieldKind === 'map') return { kind: 'labels', leaf: field };
		if (field.fieldKind === 'message') {
			if (field.message.typeName === 'google.protobuf.Timestamp') return { kind: 'time', leaf: field };
			if (last) throw new Error(`${path} is a message`);
			message = field.message;
			continue;
		}
		if (!last) throw new Error(`${path} continues past a scalar`);
		if (field.fieldKind === 'enum') return { kind: 'enum', leaf: field };
		if (field.fieldKind === 'scalar' && field.scalar === ScalarType.BOOL) return { kind: 'bool', leaf: field };
		if (field.fieldKind === 'scalar' && field.scalar === ScalarType.STRING) return { kind: 'text', leaf: field };
		throw new Error(`${path} is not filterable`);
	}
	throw new Error(`empty path`);
}

const specs = new Map<string, FilterSpec>();

/** The allowlist a list request declares, resolved against the listed message. */
export function specFor(request: DescMessage): FilterSpec {
	const cached = specs.get(request.typeName);
	if (cached) return cached;
	const declared = getOption(request, filterable);
	const resource = createRegistry(...filesOf(request.file)).getMessage(declared.resource);
	if (!resource) throw new Error(`${request.typeName}: resource ${declared.resource} is not a known message`);
	const spec: FilterSpec = {
		resource: resource.name,
		fields: declared.fields.map((f) => {
			const { kind, leaf } = resolveField(resource, f.path);
			let values: string[] = [];
			if (kind === 'enum' && leaf.fieldKind === 'enum') {
				values = f.allowedValues.length > 0
					? [...f.allowedValues]
					: leaf.enum.values.filter((v) => v.number !== 0).map((v) => shortEnumName(leaf.enum, v.name));
			}
			return {
				path: f.path,
				label: f.label,
				kind,
				operators: [...f.operators],
				values,
				keptWhenUnavailable: f.keptWhenUnavailable,
				requires: f.requires,
				valueRule: f.valueRule
			};
		})
	};
	specs.set(request.typeName, spec);
	return spec;
}

/** The declared field a filter's path addresses, and its label key. */
export function fieldOf(spec: FilterSpec, path: string): { field: FilterField; key: string } | null {
	const exact = spec.fields.find((f) => f.path === path);
	if (exact) return { field: exact, key: '' };
	const labels = spec.fields.find((f) => f.kind === 'labels' && path.startsWith(`${f.path}.`));
	return labels ? { field: labels, key: path.slice(labels.path.length + 1) } : null;
}

export function filterOf(field: string, op: FilterOperator, values: string[]): FieldFilter {
	return create(FieldFilterSchema, { field, op, values });
}

const FIELD_END = new Set(['=', '!', '~', '^', '<', '>']);

/** One filter in the CLI grammar (internal/listfilter.Parse), or why it is not one. */
export function parseFilter(text: string): FieldFilter | { error: string } {
	if (text === '') return { error: 'empty filter' };
	let field = '';
	let i = 0;
	for (; i < text.length; i += 1) {
		const c = text[i];
		if (c === '\\') {
			if (i + 1 === text.length) return { error: `a trailing \\ escapes nothing in filter ${JSON.stringify(text)}` };
			i += 1;
			field += text[i];
			continue;
		}
		if (FIELD_END.has(c)) break;
		field += c;
	}
	if (i === text.length) return { error: `no operator in filter ${JSON.stringify(text)}; use = != ~ ^ >= or <` };
	if (i === 0) return { error: `empty field in filter ${JSON.stringify(text)}` };
	let op: FilterOperator;
	switch (text[i]) {
		case '=':
			op = FilterOperator.EQUALS;
			i += 1;
			break;
		case '~':
			op = FilterOperator.CONTAINS;
			i += 1;
			break;
		case '^':
			op = FilterOperator.PREFIX;
			i += 1;
			break;
		case '<':
			op = FilterOperator.BEFORE;
			i += 1;
			break;
		case '!':
			if (text[i + 1] !== '=') return { error: 'no operator "!"; use != (not equals)' };
			op = FilterOperator.NOT_EQUALS;
			i += 2;
			break;
		default: // '>'
			if (text[i + 1] !== '=') return { error: 'no operator ">"; use >= (at or after) or < (before)' };
			op = FilterOperator.AT_OR_AFTER;
			i += 2;
	}
	const rest = text.slice(i);
	if (rest.startsWith('=')) return { error: 'no operator "<=", "==" or ">=="; escape a value\'s leading = as \\=' };
	const values: string[] = [];
	let item = '';
	for (let j = 0; j < rest.length; j += 1) {
		const c = rest[j];
		if (c === '\\') {
			if (j + 1 === rest.length) return { error: `a trailing \\ escapes nothing in filter ${JSON.stringify(text)}` };
			j += 1;
			item += rest[j];
		} else if (c === ',') {
			values.push(item);
			item = '';
		} else {
			item += c;
		}
	}
	values.push(item);
	if (values.length > 1) {
		if (op !== FilterOperator.EQUALS) return { error: 'a list of values needs =; escape a literal comma as \\,' };
		if (values.some((v) => v === '')) {
			return { error: `an empty value in the list of filter ${JSON.stringify(text)}; escape a literal comma as \\,` };
		}
		op = FilterOperator.IN;
	}
	return filterOf(field, op, values);
}

/** Escapes one value of a filter: \ and , always, and a leading =. */
export function escapeValue(value: string): string {
	let out = '';
	for (let i = 0; i < value.length; i += 1) {
		const c = value[i];
		if (c === '\\' || c === ',' || (i === 0 && c === '=')) out += '\\';
		out += c;
	}
	return out;
}

/** A filter in the CLI grammar; parseFilter(formatFilter(f)) is f. */
export function formatFilter(f: Pick<FieldFilter, 'field' | 'op' | 'values'>): string {
	let out = '';
	for (const c of f.field) {
		if (c === '\\' || FIELD_END.has(c)) out += '\\';
		out += c;
	}
	return out + OPERATOR_TEXT[f.op] + f.values.map(escapeValue).join(',');
}

const utf8 = new TextEncoder();

/** Bytewise (UTF-8) order, the order the engine's canonical form uses. */
export function compareBytes(a: string, b: string): number {
	const x = utf8.encode(a);
	const y = utf8.encode(b);
	for (let i = 0; i < Math.min(x.length, y.length); i += 1) if (x[i] !== y[i]) return x[i] - y[i];
	return x.length - y.length;
}

/** The filters as the URL and the page stacks carry them: formatted, deduplicated, sorted bytewise. */
export function canonicalTexts(filters: Pick<FieldFilter, 'field' | 'op' | 'values'>[]): string[] {
	return [...new Set(filters.map(formatFilter))].sort(compareBytes);
}

/**
 * The filters a list's URL holds under `param`, or the first parameter that
 * does not parse — which the page shows without sending a request.
 */
export function filtersFromUrl(url: URL, param = 'filter'): { filters: FieldFilter[]; error: string | null } {
	const filters: FieldFilter[] = [];
	for (const text of url.searchParams.getAll(param)) {
		const parsed = parseFilter(text);
		if ('error' in parsed) return { filters: [], error: `${param} ${JSON.stringify(text)}: ${parsed.error}` };
		filters.push(parsed);
	}
	return { filters, error: null };
}

/** `--filter <quoted>` for each filter, for a CLI equivalent. */
export function filterFlags(filters: Pick<FieldFilter, 'field' | 'op' | 'values'>[]): string[] {
	return canonicalTexts(filters).map((text) => `--filter ${shellQuote(text)}`);
}

/** The value of the one non-empty equals filter on path, or null — Equality's rule. */
export function equalityOf(filters: Pick<FieldFilter, 'field' | 'op' | 'values'>[], path: string): string | null {
	const on = filters.filter((f) => f.field === path);
	if (on.length !== 1) return null;
	const [only] = on;
	const single = only.op === FilterOperator.EQUALS || (only.op === FilterOperator.IN && new Set(only.values).size === 1);
	return single && only.values[0] ? only.values[0] : null;
}

/** The instant a filter value spells, in UTC, for showing beside a datetime-local input. */
export function utcInstant(value: string): string {
	if (!value) return '';
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? '' : date.toISOString().replace('.000Z', 'Z');
}
