import { expect, test } from 'bun:test';
import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { BindingMatch, BindingPin, BindingSchema } from '../src/lib/gen/orkstack/v1/access_pb';
import { bindingEdit, keep, matchWord, parseSubjects, pinWords, savedBinding, scopeKind, sharedCredentialMark } from '../src/lib/accessBindings';

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

test('a binding for a credential Orkers share says what it does not let them do, and no other does', () => {
	const mark = sharedCredentialMark(create(BindingSchema, { name: 'edge', role: 'orker', scope: 'pool:edge', orkerIdsAny: true }));
	expect(mark).toEqual({
		word: 'hosts no event source',
		why: 'Its Orkers share one credential, so this binding lets none of them host an event source.'
	});
	expect(sharedCredentialMark(create(BindingSchema, { name: 'edge', role: 'orker', scope: 'pool:edge' }))).toBeNull();
});

test('an edit keeps what the form does not show: a shared credential stays one', () => {
	const stored = create(BindingSchema, { name: 'edge', role: 'orker', scope: 'pool:edge', subjects: ['cert:a'], orkerIdsAny: true, scopeId: 'id', pin: BindingPin.PINNED, fromConfiguration: false });
	const edit = { name: ' edge ', role: 'orker', scope: ' pool:edge ', subjects: 'cert:a\n\ncert:b\n' };
	const saved = savedBinding(edit, stored);
	expect(saved.orkerIdsAny).toBe(true);
	expect([saved.name, saved.role, saved.scope, saved.subjects]).toEqual(['edge', 'orker', 'pool:edge', ['cert:a', 'cert:b']]);
	// What only the engine sets is never sent back: a write that sets it is refused.
	expect([saved.scopeId, saved.pin, saved.fromConfiguration]).toEqual(['', BindingPin.UNSPECIFIED, false]);
	// The flag is the orker role's alone; another role with it is refused.
	expect(savedBinding({ ...edit, role: 'viewer' }, stored).orkerIdsAny).toBe(false);
	expect(savedBinding(edit, null).orkerIdsAny).toBe(false);
});

test('an edit that changes nothing sends the binding as it was stored, every field of it', () => {
	// What only the engine sets: a write that sets one is refused, and it keeps its own.
	const engineSet = ['scope_id', 'pin', 'pinned_at', 'from_configuration'];
	for (const stored of [
		create(BindingSchema, { name: 'edge', role: 'orker', scope: 'pool:edge', subjects: ['cert:edge-*', 'serviceaccount:edge'], orkerIdsAny: true }),
		create(BindingSchema, { name: 'gpu', role: 'orker', scope: 'pool:gpu', subjects: ['cert:gpu-1'], orkerIdsAny: false }),
		create(BindingSchema, { name: 'ops', role: 'operator', scope: 'namespace:payments', subjects: ['group:corp:sre'], scopeId: 'ns_1', pin: BindingPin.PINNED, pinnedAt: timestampFromMs(1) })
	]) {
		const saved = savedBinding(bindingEdit(stored), stored);
		for (const field of BindingSchema.fields) {
			const read = (b: typeof stored) => JSON.stringify(b[field.localName as keyof typeof stored] ?? null);
			const want = engineSet.includes(field.name) ? read(create(BindingSchema, {})) : read(stored);
			expect(`${stored.name} ${field.name} ${read(saved)}`).toBe(`${stored.name} ${field.name} ${want}`);
		}
	}
	// The sample sets every field a person writes, so a field added to the
	// contract fails here until the form or savedBinding carries it.
	const sample = create(BindingSchema, { name: 'n', role: 'orker', scope: 'pool:p', subjects: ['s'], orkerIdsAny: true });
	const unset = BindingSchema.fields.filter((f) => !engineSet.includes(f.name) && JSON.stringify(sample[f.localName as keyof typeof sample]) === JSON.stringify(create(BindingSchema, {})[f.localName as keyof typeof sample])).map((f) => f.name);
	expect(unset).toEqual([]);
});

test('every match has its own word, and one this build does not know is named, never blank', () => {
	const known = Object.values(BindingMatch).filter((m): m is BindingMatch => typeof m === 'number' && m !== BindingMatch.UNSPECIFIED);
	const words = known.map(matchWord);
	expect(new Set(words).size).toBe(known.length);
	expect(words.filter((w) => w === '' || w.startsWith('Unknown'))).toEqual([]);
	expect(matchWord(BindingMatch.SHARED_ORKER_CREDENTIAL)).toBe('Its Orkers share one credential');
	expect(matchWord(99 as BindingMatch)).toBe('Unknown (99)');
	expect(matchWord(BindingMatch.UNSPECIFIED)).toBe('Unknown (0)');
});
