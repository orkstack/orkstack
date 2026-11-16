// The Bindings screen's reading of the access document: which bindings a
// filter keeps, and what each binding's pin says in words.
import { BindingPin, type Binding } from '#lib/gen/orkstack/v1/access_pb.ts';

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
