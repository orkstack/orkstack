import { expect, test } from 'bun:test';
import {
	AUDIT_CAPTION,
	DEFAULT_AUDIT_LIMIT,
	auditCliCommand,
	auditHref,
	auditLimit,
	auditPrincipalLabel,
	auditResultLabel,
	auditScopeSentence,
	auditStackKey,
	type AuditCoordinates
} from '../src/lib/audit';
import { ListAuditRecordsRequestSchema } from '../src/lib/gen/orkstack/v1/audit_pb';
import { parseFilter, specFor, type FieldFilter } from '../src/lib/listFilter';

const base: AuditCoordinates = {
	ns: 'payments',
	filters: [],
	limit: DEFAULT_AUDIT_LIMIT,
	cursor: '',
	page: 1
};

function parsed(...texts: string[]): FieldFilter[] {
	return texts.map((text) => {
		const f = parseFilter(text);
		if ('error' in f) throw new Error(f.error);
		return f;
	});
}

test('the page offers the ListAuditRecords allowlist, namespace among the properties', () => {
	const spec = specFor(ListAuditRecordsRequestSchema);
	expect(spec.resource).toBe('AuditRecord');
	expect(spec.fields.map((f) => f.path)).toEqual(['at', 'namespace', 'action', 'principal', 'target', 'succeeded', 'error_code']);
	expect(spec.fields.find((f) => f.path === 'at')?.kind).toBe('time');
	expect(spec.fields.find((f) => f.path === 'succeeded')?.kind).toBe('bool');
});

test('an absent principal is unattributed and a refusal carries its code', () => {
	expect(auditPrincipalLabel('')).toBe('unattributed');
	expect(auditPrincipalLabel('dsantos')).toBe('dsantos');
	expect(auditResultLabel({ succeeded: true, errorCode: '' })).toBe('ok');
	expect(auditResultLabel({ succeeded: false, errorCode: 'failed_precondition' })).toBe('refused failed_precondition');
	expect(AUDIT_CAPTION).toContain('does not authenticate it');
});

test('limits outside 1..200 fall back to the default rather than being sent', () => {
	expect(auditLimit('2')).toBe(2);
	expect(auditLimit('200')).toBe(200);
	expect(auditLimit('201')).toBe(DEFAULT_AUDIT_LIMIT);
	expect(auditLimit('0')).toBe(DEFAULT_AUDIT_LIMIT);
	expect(auditLimit(null)).toBe(DEFAULT_AUDIT_LIMIT);
});

test('the address carries the filters as repeated filter keys, drops the cursor on a change, and omits defaults', () => {
	expect(auditHref(base)).toBe('/admin/audit?ns=payments');
	const filtered = { ...base, filters: parsed('principal=a b', 'action^namespace.'), limit: 2, cursor: 'v2:x', page: 3 };
	expect(auditHref(filtered)).toBe(
		'/admin/audit?ns=payments&filter=action%5Enamespace.&filter=principal%3Da+b&limit=2&cursor=v2%3Ax&page=3'
	);
	expect(auditHref(filtered, { cursor: '', page: 1 })).toBe(
		'/admin/audit?ns=payments&filter=action%5Enamespace.&filter=principal%3Da+b&limit=2'
	);
	// `namespace=` is a filter like any other: the empty value is kept.
	expect(new URL(auditHref(base, { filters: parsed('namespace=') }), 'http://x').searchParams.getAll('filter')).toEqual(['namespace=']);
	// The stack key is the filter set alone.
	expect(auditStackKey(filtered)).toBe(auditStackKey({ ...filtered, cursor: 'v2:y', page: 7 }));
	expect(auditStackKey(filtered)).not.toBe(auditStackKey({ ...filtered, filters: parsed('principal=other') }));
});

test('the CLI equivalent is admin audit with every filter shell-quoted and --limit, never -n', () => {
	expect(auditCliCommand(base)).toBe('orkctl admin audit');
	expect(auditCliCommand({ filters: parsed('namespace=payments', "principal=o'brien", 'action^namespace.'), limit: 2 })).toBe(
		`orkctl admin audit --filter 'action^namespace.' --filter namespace=payments --filter 'principal=o'"'"'brien' --limit 2`
	);
	expect(auditCliCommand({ filters: parsed('namespace='), limit: DEFAULT_AUDIT_LIMIT })).toBe(`orkctl admin audit --filter namespace=`);
});

test('the scope sentence says what the namespace filter selects', () => {
	expect(auditScopeSentence([])).toContain('Showing every namespace, deployment-wide writes included');
	expect(auditScopeSentence(parsed('namespace='))).toContain('Showing only deployment-wide writes');
	expect(auditScopeSentence(parsed('namespace=payments'))).toContain('Showing namespace payments');
	expect(auditScopeSentence(parsed('namespace!='))).toBe('Showing the namespaces the namespace filter selects.');
});
