import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { BindingPin, BindingSchema } from '../src/lib/gen/orkstack/v1/access_pb';
import { keep, parseSubjects, pinWords, scopeKind } from '../src/lib/accessBindings';

const b = (o: Partial<{ name: string; role: string; scope: string; subjects: string[]; pin: BindingPin }>) =>
	create(BindingSchema, { name: 'ops', role: 'operator', scope: 'namespace:payments', subjects: ['group:corp:sre'], ...o });

test('keep filters by role, kind of scope and text in name, scope or subject', () => {
	const none = { text: '', role: '', scopeKind: '' as const };
	expect(keep(b({}), none)).toBe(true);
	expect(keep(b({}), { ...none, role: 'viewer' })).toBe(false);
	expect(keep(b({}), { ...none, scopeKind: 'namespace' })).toBe(true);
	expect(keep(b({}), { ...none, scopeKind: 'pool' })).toBe(false);
	expect(keep(b({}), { ...none, text: 'SRE' })).toBe(true);
	expect(keep(b({}), { ...none, text: 'payments' })).toBe(true);
	expect(keep(b({}), { ...none, text: 'nobody' })).toBe(false);
});

test('scopeKind and pinWords', () => {
	expect(scopeKind('deployment')).toBe('deployment');
	expect(scopeKind('orkflow:a/x')).toBe('orkflow');
	expect(scopeKind('cluster')).toBe('');
	expect(pinWords(b({ pin: BindingPin.STALE }))?.stale).toBe(true);
	expect(pinWords(b({ pin: BindingPin.PINNED }))?.word).toBe('Pinned');
	expect(pinWords(b({ pin: BindingPin.NONE }))).toBeNull();
});

test('parseSubjects keeps one subject a line', () => {
	expect(parseSubjects(' group:corp:sre \n\nuser:corp:ana\n')).toEqual(['group:corp:sre', 'user:corp:ana']);
});
