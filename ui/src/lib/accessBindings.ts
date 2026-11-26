// The Bindings screen's reading of the access document: which bindings a
// filter keeps, what each binding's pin says in words, what a save sends, and
// the words of an explanation's outcomes.
import { create } from '@bufbuild/protobuf';
import { BindingMatch, BindingPin, BindingSchema, type Binding } from '#lib/gen/orkstack/v1/access_pb.ts';

export interface BindingFilter {
	text: string;
	role: string;
	scopeKind: '' | 'deployment' | 'namespace' | 'orkflow' | 'pool';
}

/** scopeKind is the kind a scope string names. */
export function scopeKind(scope: string): BindingFilter['scopeKind'] {
	if (scope === 'deployment') return 'deployment';
	const kind = scope.split(':', 1)[0];
	return kind === 'namespace' || kind === 'orkflow' || kind === 'pool' ? kind : '';
}

/** keep reports whether a binding passes the filter: a role and a kind of
 * scope chosen, and text found in its name, scope or any subject. */
export function keep(b: Binding, f: BindingFilter): boolean {
	if (f.role && b.role !== f.role) return false;
	if (f.scopeKind && scopeKind(b.scope) !== f.scopeKind) return false;
	const t = f.text.trim().toLowerCase();
	if (!t) return true;
	return [b.name, b.scope, ...b.subjects].some((s) => s.toLowerCase().includes(t));
}

/** pinWords says what a binding at a namespace or an Orkflow matches now. */
export function pinWords(b: Binding): { word: string; tip: string; stale: boolean } | null {
	switch (b.pin) {
		case BindingPin.PINNED:
			return { word: 'Pinned', tip: 'Matches the namespace or Orkflow that existed when it was saved, and no other made later under its name.', stale: false };
		case BindingPin.BY_NAME:
			return { word: 'By name', tip: 'Saved before its namespace or Orkflow existed: it matches by name until it is saved again.', stale: false };
		case BindingPin.STALE:
			return { word: 'Stale', tip: 'Its namespace or Orkflow was deleted and made again: it matches nothing until it is pinned again.', stale: true };
	}
	return null;
}

/** parseSubjects reads the subjects field: one per line, blanks dropped. */
export function parseSubjects(text: string): string[] {
	return text
		.split('\n')
		.map((s) => s.trim())
		.filter((s) => s !== '');
}

/** sharedCredentialMark is what a binding to a credential that Orkers share
 * (orker_ids_any) does not let them do; null for any other binding. Said of
 * the binding, not of its Orkers: one may hold another by its own id. */
export function sharedCredentialMark(b: Binding): { word: string; why: string } | null {
	if (!b.orkerIdsAny) return null;
	return {
		word: 'hosts no event source',
		why: 'Its Orkers share one credential, so this binding lets none of them host an event source.'
	};
}

/** What the binding form holds. */
export interface BindingEdit {
	name: string;
	role: string;
	scope: string;
	subjects: string;
}

/** bindingEdit is a stored binding as the form opens on it. */
export function bindingEdit(b: Binding): BindingEdit {
	return { name: b.name, role: b.role, scope: b.scope, subjects: b.subjects.join('\n') };
}

/**
 * savedBinding is the binding a save sends. A write replaces the whole
 * binding, so what the form does not show is kept from the stored one: that
 * it is for a credential Orkers share, which only the orker role may be. What
 * the engine alone sets (the pin, configuration) is never sent.
 */
export function savedBinding(edit: BindingEdit, stored: Binding | null): Binding {
	return create(BindingSchema, {
		name: edit.name.trim(),
		role: edit.role,
		scope: edit.scope.trim(),
		subjects: parseSubjects(edit.subjects),
		orkerIdsAny: edit.role === 'orker' && (stored?.orkerIdsAny ?? false)
	});
}

// Every match of the contract has its word: one added to it fails the type check here.
const MATCH_WORDS: Record<Exclude<BindingMatch, BindingMatch.UNSPECIFIED>, string> = {
	[BindingMatch.GRANTED]: 'Granted it',
	[BindingMatch.SUBJECT_DIFFERS]: 'Names someone else',
	[BindingMatch.SCOPE_DIFFERS]: 'At another scope',
	[BindingMatch.ROLE_LACKS_PERMISSION]: 'Its role lacks the permission',
	[BindingMatch.PINNED_ELSEWHERE]: 'Pinned to another incarnation',
	[BindingMatch.SHARED_ORKER_CREDENTIAL]: 'Its Orkers share one credential'
};

/** matchWord is why a binding did or did not grant a permission; one this build does not know is named by its number, never left blank. */
export function matchWord(m: BindingMatch): string {
	return (MATCH_WORDS as Partial<Record<BindingMatch, string>>)[m] ?? `Unknown (${String(m)})`;
}
