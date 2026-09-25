import { expect, test } from 'bun:test';
import {
	AUDIT_CAPTION,
	DEFAULT_AUDIT_LIMIT,
	auditCliCommand,
	auditFilterError,
	auditHref,
	auditLimit,
	auditPrincipalLabel,
	auditRequestNamespace,
	auditResultLabel,
	auditScope,
	auditStackKey,
	type AuditCoordinates
} from '../src/lib/audit';

const base: AuditCoordinates = {
	scope: 'all',
	namespace: 'payments',
	action: '',
	principal: '',
	since: '',
	until: '',
	limit: DEFAULT_AUDIT_LIMIT,
	cursor: '',
	page: 1
};

test('the scope is every namespace unless the page asks for one, and default is spelled out', () => {
	expect(auditScope(null)).toBe('all');
	expect(auditScope('bogus')).toBe('all');
	expect(auditRequestNamespace({ scope: 'all', namespace: 'payments' })).toBe('');
	expect(auditRequestNamespace({ scope: 'namespace', namespace: 'payments' })).toBe('payments');
	// Empty on this one request means EVERY namespace, so default is never sent empty.
	expect(auditRequestNamespace({ scope: 'namespace', namespace: '' })).toBe('default');
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

test('a window the CLI would refuse is refused here', () => {
	expect(auditFilterError('', '')).toBeNull();
	expect(auditFilterError('2026-01-01', '2026-04-01T00:00:00Z')).toBeNull();
	expect(auditFilterError('yesterday', '')).toContain('Since');
	expect(auditFilterError('', '2026-01-01T00:00')).toContain('Until');
	expect(auditFilterError('2026-04-01', '2026-01-01')).toBe('Until must be after since.');
});

test('the address keeps every filter, drops the cursor on a filter change, and omits defaults', () => {
	expect(auditHref(base)).toBe('/admin/audit?ns=payments');
	const filtered = { ...base, scope: 'namespace' as const, action: 'namespace.', principal: 'a b', limit: 2, cursor: 'v1:x', page: 3 };
	expect(auditHref(filtered)).toBe(
		'/admin/audit?ns=payments&scope=namespace&action=namespace.&principal=a+b&limit=2&cursor=v1%3Ax&page=3'
	);
	expect(auditHref(filtered, { cursor: '', page: 1 })).toBe(
		'/admin/audit?ns=payments&scope=namespace&action=namespace.&principal=a+b&limit=2'
	);
	// The stack key is the filter set alone.
	expect(auditStackKey(filtered)).toBe(auditStackKey({ ...filtered, cursor: 'v1:y', page: 7 }));
	expect(auditStackKey(filtered)).not.toBe(auditStackKey({ ...filtered, principal: 'other' }));
});

test('the CLI equivalent names the same listing, shell-quoted', () => {
	expect(auditCliCommand(base)).toBe('orkctl admin audit');
	expect(
		auditCliCommand({ ...base, scope: 'namespace', action: 'namespace.', principal: "o'brien", since: '2026-01-01', limit: 2 })
	).toBe(`orkctl -n 'payments' admin audit --since '2026-01-01' --action 'namespace.' --principal 'o'"'"'brien' --limit 2`);
});
