import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { hasOption, type DescMessage } from '@bufbuild/protobuf';
import { filterable, FilterOperator, FilterValueRule } from '../src/lib/gen/orkstack/v1/filter_pb';
import { file_orkstack_v1_audit } from '../src/lib/gen/orkstack/v1/audit_pb';
import { file_orkstack_v1_schedule } from '../src/lib/gen/orkstack/v1/schedule_pb';
import {
	ListNamespacesRequestSchema,
	ListOrkersRequestSchema,
	ListReleasesRequestSchema,
	file_orkstack_v1_service
} from '../src/lib/gen/orkstack/v1/service_pb';
import {
	OPERATOR_LABELS,
	canonicalTexts,
	enumPrefix,
	equalityOf,
	filterFlags,
	filterOf,
	filtersFromUrl,
	formatFilter,
	parseFilter,
	specFor
} from '../src/lib/listFilter';

// The ONE table both grammars are held to: internal/listfilter reads the same
// file with os.ReadFile.
interface Vectors {
	grammar: { text: string; filter?: { field: string; op: string; values: string[] }; formatted?: string; error?: string }[];
}
const vectors: Vectors = JSON.parse(readFileSync(new URL('../../internal/listfilter/testdata/vectors.json', import.meta.url).pathname, 'utf8'));
const op = (name: string) => FilterOperator[name as keyof typeof FilterOperator] as FilterOperator;

test('the shared vectors hold at least forty grammar rows', () => {
	expect(vectors.grammar.length >= 40).toBe(true);
});

test('parseFilter reads every shared vector as the engine does', () => {
	for (const row of vectors.grammar) {
		const parsed = parseFilter(row.text);
		if (row.error !== undefined) {
			expect('error' in parsed ? parsed.error : `parsed ${row.text}`).toContain(row.error);
			continue;
		}
		if ('error' in parsed) throw new Error(`${row.text}: ${parsed.error}`);
		expect({ field: parsed.field, op: parsed.op, values: parsed.values }).toEqual({
			field: row.filter!.field,
			op: op(row.filter!.op),
			values: row.filter!.values
		});
	}
});

test('formatFilter writes every shared vector as the engine does, and round-trips', () => {
	for (const row of vectors.grammar) {
		if (!row.filter) continue;
		const f = filterOf(row.filter.field, op(row.filter.op), row.filter.values);
		expect(formatFilter(f)).toBe(row.formatted ?? row.text);
		const back = parseFilter(formatFilter(f));
		if ('error' in back) throw new Error(back.error);
		expect({ field: back.field, op: back.op, values: back.values }).toEqual({ field: f.field, op: f.op, values: f.values });
	}
});

// Every list request in the generated files that declares filters resolves in
// the browser: its fields name real fields of the listed message.
test('specFor resolves every filterable request', () => {
	const requests: DescMessage[] = [];
	for (const file of [file_orkstack_v1_service, file_orkstack_v1_schedule, file_orkstack_v1_audit]) {
		for (const service of file.services) for (const method of service.methods) {
			if (method.name.startsWith('List') && hasOption(method.input, filterable)) requests.push(method.input);
		}
	}
	expect(requests.length >= 9).toBe(true);
	for (const request of requests) {
		const spec = specFor(request);
		expect(spec.fields.length > 0).toBe(true);
		for (const field of spec.fields) {
			expect(field.operators.length > 0).toBe(true);
			expect(['text', 'enum', 'bool', 'time', 'labels']).toContain(field.kind);
		}
	}
});

test('a spec carries kinds, enum values, rules and the unreachable-row flag', () => {
	const namespaces = specFor(ListNamespacesRequestSchema);
	expect(namespaces.resource).toBe('Namespace');
	expect(namespaces.fields.map((f) => `${f.path}:${f.kind}`)).toEqual([
		'name:text', 'description:text', 'labels:labels', 'default_pool:text', 'hold.reason:text',
		'require_signed_by:text', 'telemetry.sink:text', 'created_at:time', 'updated_at:time'
	]);
	expect(namespaces.fields[0].valueRule).toBe(FilterValueRule.NAME);
	const releases = specFor(ListReleasesRequestSchema);
	expect(releases.fields.find((f) => f.path === 'role')?.values).toEqual(['CURRENT', 'RAMPING', 'PREVIOUS', 'CANDIDATE']);
	expect(releases.fields.find((f) => f.path === 'drained')?.kind).toBe('bool');
	const orkers = specFor(ListOrkersRequestSchema);
	expect(orkers.fields.filter((f) => f.keptWhenUnavailable).map((f) => f.path)).toEqual(['pool', 'build_id', 'cordoned']);
	expect(orkers.fields[0].valueRule).toBe(FilterValueRule.ORKER_ID);
});

test('enum prefixes follow the engine rule', () => {
	expect(enumPrefix('Status')).toBe('STATUS_');
	expect(enumPrefix('EventType')).toBe('EVENT_TYPE_');
	expect(enumPrefix('RunOriginKind')).toBe('RUN_ORIGIN_KIND_');
	expect(enumPrefix('ReleaseRole')).toBe('RELEASE_ROLE_');
});

test('operator labels are the fixed display names', () => {
	expect(OPERATOR_LABELS[FilterOperator.IN]).toBe('in (=a,b)');
	expect(OPERATOR_LABELS[FilterOperator.AT_OR_AFTER]).toBe('at or after (>=)');
});

test('a URL filter that does not parse is an error the page shows without a request', () => {
	expect(filtersFromUrl(new URL('http://x/admin/namespaces?filter=name%7Epay&filter=labels.team%3Dpayments'))).toEqual({
		filters: [filterOf('name', FilterOperator.CONTAINS, ['pay']), filterOf('labels.team', FilterOperator.EQUALS, ['payments'])],
		error: null
	});
	const bad = filtersFromUrl(new URL('http://x/orkflows?filter=name%7Epay&filter=created_at%3E2026'));
	expect(bad.filters).toEqual([]);
	expect(bad.error).toBe('filter "created_at>2026": no operator ">"; use >= (at or after) or < (before)');
	// A filter the server will refuse (an unknown field) still parses: the
	// browser invents no validation the CLI would bypass.
	expect(filtersFromUrl(new URL('http://x/orkflows?filter=nope%3Dx')).error).toBeNull();
	// Another list's parameter is read by name.
	expect(filtersFromUrl(new URL('http://x/admin/webhooks?filter=a%3Db&deliveryFilter=delivered%3Dfalse'), 'deliveryFilter').filters)
		.toEqual([filterOf('delivered', FilterOperator.EQUALS, ['false'])]);
});

test('filter flags are canonical and always single-quoted', () => {
	const filters = [
		filterOf('name', FilterOperator.PREFIX, ['tenant-']),
		filterOf('labels.team', FilterOperator.EQUALS, ['payments']),
		filterOf('description', FilterOperator.EQUALS, ["it's x,y"])
	];
	expect(filterFlags(filters)).toEqual([
		`--filter 'description=it'"'"'s x\\,y'`,
		`--filter 'labels.team=payments'`,
		`--filter 'name^tenant-'`
	]);
	expect(['orkctl', 'namespace', 'list', ...filterFlags([filters[1]])].join(' ')).toBe(
		"orkctl namespace list --filter 'labels.team=payments'"
	);
	expect(canonicalTexts([filters[0], filters[0]])).toEqual(['name^tenant-']);
});

test('equalityOf is the one non-empty equals on a path', () => {
	expect(equalityOf([filterOf('orkflow_name', FilterOperator.EQUALS, ['deploy'])], 'orkflow_name')).toBe('deploy');
	expect(equalityOf([filterOf('orkflow_name', FilterOperator.IN, ['deploy', 'deploy'])], 'orkflow_name')).toBe('deploy');
	expect(equalityOf([filterOf('orkflow_name', FilterOperator.EQUALS, [''])], 'orkflow_name')).toBeNull();
	expect(equalityOf([filterOf('orkflow_name', FilterOperator.PREFIX, ['d'])], 'orkflow_name')).toBeNull();
	expect(
		equalityOf([filterOf('orkflow_name', FilterOperator.EQUALS, ['a']), filterOf('orkflow_name', FilterOperator.EQUALS, ['b'])], 'orkflow_name')
	).toBeNull();
	expect(equalityOf([], 'orkflow_name')).toBeNull();
});
